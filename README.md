# Pipeline SELIC / IPCA — Bronze, Silver e Gold

Pipeline de dados em três camadas no Databricks, alimentado por um script
de extração local que consome as APIs do Banco Central (SGS) para SELIC
(série 11) e IPCA (série 433).

## Arquitetura

```
extract/extrai_selic_ipca.py    (roda localmente, fora do Databricks)
        │
        ▼  (upload manual via CLI, ou automático com --upload)
/Volumes/analise_taxas/selic_ipca/dados_brutos/{selic,ipca}/{selic,ipca}.json
        │
        ▼  notebooks/01_bronze.py   (Auto Loader, incremental)
bronze_selic, bronze_ipca            (Delta, schema como string)
        │
        ▼  notebooks/02_silver.py  (tipagem + MERGE idempotente)
silver_selic, silver_ipca             (Delta, tipado, sem duplicatas)
        │
        ▼  notebooks/03_gold.py    (agregação mensal)
gold_indicadores_mensais              (Delta, 1 linha por mês)
```

Toda a lógica de transformação fica no pacote `src/` (módulos Python
puros, sem dependência de notebook). Os notebooks em `notebooks/` são
wrappers finos que apenas chamam as funções do `src/` — cada um vira uma
task do Databricks Workflow.

## Estrutura do projeto

```
project_selic_ipca/
├── databricks.yml                 # Databricks Asset Bundle (deploy do Job)
├── requirements.txt                # dependências do script de extração
├── extract/
│   ├── extrai_selic_ipca.py       # script de extração local (com backfill e --upload)
│   └── upload_volume.py           # upload automático para o Volume via SDK
├── src/
│   ├── config.py                  # nomes de catálogo/schema/volume/tabelas
│   ├── bronze.py                  # ingestão incremental via Auto Loader
│   ├── silver.py                  # tipagem, nulos, MERGE idempotente
│   ├── gold.py                    # agregação mensal e indicadores
│   └── quality_checks.py          # checagens de qualidade (falha explícita)
├── pipelines/
│   └── dlt_selic_ipca.py          # implementação alternativa via Lakeflow/DLT
├── notebooks/
│   ├── 01_bronze.py               # task 1 do Workflow
│   ├── 02_silver.py               # task 2 do Workflow
│   ├── 03_gold.py                 # task 3 do Workflow
│   └── 04_verificacao_idempotencia.py  # evidência de idempotência
├── evidencias/                     # prints de execução real (ver seção própria)
└── README.md
```

## Camada Bronze

- **Auto Loader** (`cloudFiles`), com `trigger(availableNow=True)` — cada
  execução processa o que estiver disponível no Volume e encerra
  (comportamento de batch incremental, não streaming contínuo).
- Schema fixo com todos os campos como `StringType` — o dado bruto da API
  é preservado sem nenhuma conversão de tipo.
- Colunas de auditoria: `dt_ingestao` (timestamp da carga) e
  `nm_arquivo_origem` (nome do arquivo de origem, via `_metadata.file_name`).
- Cada série fica em sua própria subpasta dentro do Volume
  (`dados_brutos/selic/`, `dados_brutos/ipca/`), cada uma com seu próprio
  checkpoint (`checkpointLocation`) num Volume dedicado (`checkpoints`),
  separado do Volume de dados — necessário porque o DBFS root público
  vem desabilitado por padrão em workspaces mais novos (incluindo o Free
  Edition), e porque um checkpoint dentro do mesmo Volume dos dados causa
  erro `LOCATION_OVERLAP` no Unity Catalog.
- Nome de query único a cada execução (`queryName` com timestamp) e
  `spark.streams.resetTerminated()` ao final de cada série, para evitar
  resíduo de estado de streaming entre execuções sequenciais na mesma
  sessão — mitigação para uma instabilidade observada durante o
  desenvolvimento em compute serverless (ver nota abaixo).

> **Nota de desenvolvimento**: durante os testes, streaming queries
> sequenciais (`selic` seguida de `ipca` na mesma sessão de notebook)
> apresentaram travamentos intermitentes em compute serverless. Após
> aplicar as mitigações acima (nome de query único, `resetTerminated()`,
> polling em vez de `awaitTermination()` bloqueante, pausa entre séries),
> o pipeline foi validado com **duas execuções completas e bem-sucedidas
> via Databricks Workflow** (ver `evidencias/`), confirmando que essas
> mitigações resolveram a instabilidade nesse ambiente.

## Camada Silver

**Chave de negócio declarada: `dt_referencia`** (data do registro,
convertida do campo bruto `data`, formato `dd/MM/yyyy` → `date`). Cada
série (SELIC, IPCA) tem sua própria tabela Silver, então a chave é única
dentro de cada tabela.

- Tipagem: `data` (string) → `dt_referencia` (date); `valor` (string) →
  `vl_taxa` (double).
- Tratamento de nulos: registros onde a conversão de data ou valor falha
  são descartados (indicativo de payload malformado na origem).
- Deduplicação: se a mesma data aparecer mais de uma vez (ex:
  reprocessamento com valor revisado pelo BCB), mantém apenas o registro
  mais recente por `dt_ingestao`.
- Carga idempotente via `MERGE` (`whenMatchedUpdateAll` /
  `whenNotMatchedInsertAll`), usando `dt_referencia` como chave — rodar o
  pipeline duas vezes não duplica nenhum registro; no máximo atualiza um
  valor já existente.

## Camada Gold

**Grão da tabela: um registro por mês** (`ano_mes`, formato `yyyy-MM`).

Colunas e metodologia:

| Coluna | Cálculo |
|---|---|
| `selic_media_mes` | Média simples das taxas SELIC diárias (% a.a.) do mês |
| `ipca_mes` | Variação mensal do IPCA (%), conforme reportado pelo BCB |
| `juro_real_mes` | SELIC anualizada convertida para taxa mensal equivalente (juros compostos: `(1+selic/100)^(1/12) - 1`), aplicada à fórmula de Fisher: `((1+selic_mensal)/(1+ipca_mes/100) - 1) * 100` |
| `taxa_acumulada_12m` | IPCA acumulado em uma janela móvel de 12 meses: `(∏(1+ipca_mes/100) - 1) * 100`. **Nulo nos primeiros 11 meses da série**, onde a janela ainda não está completa — isso é esperado, não é erro de qualidade |

> **Observação sobre premissas**: a definição de "juro real" e "taxa
> acumulada em 12 meses" não é única — aqui usamos a interpretação
> financeira padrão (Fisher para juro real; produtório de IPCA para
> acumulado 12m). Essas fórmulas estão implementadas em
> `src/gold.py::_calcula_indicadores`.

A tabela Gold é recriada por completo (`overwrite`) a cada execução —
como é uma agregação determinística a partir da Silver, isso é idempotente
por construção: mesma entrada sempre produz a mesma saída, sem risco de
duplicar linhas.

## Checagens de qualidade de dados

Implementadas em `src/quality_checks.py`, cada uma levanta
`DataQualityError` quando violada — a exceção propaga sem ser capturada,
então a task do Databricks Workflow falha explicitamente (fica vermelha)
quando qualquer checagem é violada.

| # | Checagem | Onde é usada |
|---|---|---|
| 1 | `check_not_empty` | Bronze não pode ficar vazia após ingestão; Silver e Gold não podem ficar vazias após transformação |
| 2 | `check_column_not_null` | `vl_taxa` não pode ter nulos na Silver após tipagem |
| 3 | `check_value_range` | `vl_taxa` (SELIC/IPCA) dentro de [-20, 100]; `juro_real_mes` dentro de [-50, 50] — fora disso indica erro de parsing |
| 4 | `check_unique_key` | Chave de negócio (`dt_referencia` na Silver, `ano_mes` na Gold) não pode ter duplicatas |
| 5 | `check_no_gaps_in_months` | Não pode haver meses faltando na série mensal usada pela Gold |

São 5 checagens no total, cobrindo a transição Bronze→Silver e
Silver→Gold — acima do mínimo de 3 pedido no desafio.

## Como rodar

### 1. Extração local
```bash
pip install -r requirements.txt
python extract/extrai_selic_ipca.py
```
Gera `extract/dados_brutos/selic.json` e `extract/dados_brutos/ipca.json`
localmente, com retries e tratamento de erro. Termina com código de
saída `1` se qualquer série falhar — código de saída `0` significa
sucesso.

Para reprocessar um período específico (backfill) ou subir os arquivos
automaticamente, ver seção **Diferenciais** abaixo.

### 2. Setup do Unity Catalog (uma vez)

O `CREATE SCHEMA` roda numa célula de notebook do Databricks (célula
SQL, ou `spark.sql(...)` numa célula Python) — não é um comando de
terminal:
```sql
CREATE SCHEMA IF NOT EXISTS analise_taxas.selic_ipca;
```

A criação dos volumes é via terminal local, usando o Databricks CLI —
são necessários **dois** volumes: um para os dados brutos, outro
dedicado a checkpoints do Auto Loader:
```bash
databricks volumes create analise_taxas selic_ipca dados_brutos MANAGED \
  --comment "Arquivos brutos de SELIC e IPCA"
databricks volumes create analise_taxas selic_ipca checkpoints MANAGED \
  --comment "Checkpoints e schema location do Auto Loader"
```

### 3. Upload dos arquivos

Cada série vai para sua própria subpasta dentro do Volume:
```bash
databricks fs cp extract/dados_brutos/selic.json dbfs:/Volumes/analise_taxas/selic_ipca/dados_brutos/selic/selic.json --overwrite
databricks fs cp extract/dados_brutos/ipca.json dbfs:/Volumes/analise_taxas/selic_ipca/dados_brutos/ipca/ipca.json --overwrite
```
(ou use `python extract/extrai_selic_ipca.py --upload`, ver Diferenciais)

### 4. Importar o projeto no Databricks
Importe a pasta `project_selic_ipca/` inteira como um **Databricks Repo**
(Git folder) ou via `databricks sync`, preservando a estrutura `src/` +
`notebooks/`. Isso é o que permite o
`sys.path.append(os.path.abspath(".."))` nos notebooks localizar o
pacote `src` para importação. Evite `databricks workspace import-dir`
para essa estrutura, pois esse comando converte todo `.py` em notebook,
o que quebraria o import do pacote `src`.

### 5. Criar o Databricks Workflow
Crie um Job com três tasks encadeadas (Free Edition aceita compute
serverless) — ou use o Databricks Asset Bundle (ver Diferenciais) para
criar isso automaticamente a partir de código:

1. **camada_bronze** → `notebooks/01_bronze.py`
2. **camada_silver** → `notebooks/02_silver.py`, depende de `camada_bronze`
3. **camada_gold** → `notebooks/03_gold.py`, depende de `camada_silver`

Cada task falha automaticamente se a exceção de qualquer checagem de
qualidade for levantada, sem precisar de configuração extra — basta não
capturar a exceção (o próprio Databricks marca a task/run como falha).

### 6. Evidência de idempotência
Rode o Workflow duas vezes seguidas (sem alterar os arquivos no Volume) e
use `notebooks/04_verificacao_idempotencia.py` para comparar as
contagens de linhas antes/depois — devem ser idênticas.

## Evidências de execução

A pasta `evidencias/` contém prints comprovando o pipeline rodando de
fato no workspace, incluindo duas execuções completas e bem-sucedidas do
Workflow (Bronze → Silver → Gold), com contagens de linhas idênticas
entre a primeira e a segunda execução — a prova concreta de idempotência
pedida pelo desafio.

| Arquivo | O que mostra |
|---|---|
| `01_job_duas_execucoes.png` | Tela de Runs do Job, com duas execuções concluídas com sucesso |
| `02_saida_idempotencia_execucao1.png` | Saída de `04_verificacao_idempotencia.py` após a 1ª execução do Workflow |
| `03_saida_idempotencia_execucao2.png` | Mesma saída após a 2ª execução — contagens idênticas às da execução 1 |
| `04_checagem_qualidade_falha.png` | (opcional) Uma checagem de qualidade forçada a falhar de propósito, mostrando a task ficando vermelha com a mensagem de erro explícita |
| `05_tabelas_unity_catalog.png` | Catalog Explorer mostrando as tabelas Bronze/Silver/Gold registradas em `analise_taxas.selic_ipca` |

## Padrão de código

- Lógica de negócio 100% em módulos `.py` dentro de `src/`, não em
  células de notebook.
- Nomes de função/variável em português (consistente com o domínio do
  desafio), docstrings em todas as funções públicas, type hints nos
  parâmetros e retornos.
- Notebooks servem apenas como ponto de entrada (thin wrappers) para
  orquestração via Databricks Workflow.

## Diferenciais implementados

### 1. Databricks Asset Bundle (deploy)

`databricks.yml` na raiz do projeto define o Job (`job_carga_geral`) como
código versionado, permitindo deploy reprodutível:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run job_carga_geral -t dev
```

Isso substitui a criação manual do Job pela UI — o Workflow inteiro
(3 tasks encadeadas, compute serverless) é recriado a partir do YAML em
qualquer workspace com um comando.

### 2. Automação do upload para o Volume (SDK)

`extract/upload_volume.py` usa o **Databricks SDK** (Files API) para
subir os arquivos direto do script de extração, sem precisar do comando
manual `databricks fs cp`:

```bash
pip install databricks-sdk
python extract/extrai_selic_ipca.py --upload
```

A flag `--upload` é opcional — sem ela, o script continua funcionando
exatamente como antes (só salva localmente). Requer autenticação já
configurada (mesmo perfil usado por `databricks configure`).

### 3. Estratégia de backfill

O script de extração aceita datas customizadas via linha de comando, sem
precisar editar código:

```bash
python extract/extrai_selic_ipca.py --data-inicial 01/01/2025 --data-final 31/07/2026 --upload
```

**Como o backfill se propaga pelas camadas:**
- **Bronze**: o Auto Loader detecta o arquivo alterado no Volume (novo
  conteúdo, mesmo caminho) e processa o lote incremental normalmente na
  próxima execução — não afeta o histórico já ingerido.
- **Silver**: o `MERGE` por `dt_referencia` lida automaticamente com o
  backfill — se o período novo tiver datas que já existiam (ex:
  reprocessamento de um mês por causa de revisão do BCB), os valores são
  atualizados; datas novas são inseridas. Nenhuma duplicata é criada.
- **Gold**: é recalculada por completo (`overwrite`) a cada execução, a
  partir da Silver mais atual — então um backfill na Silver se reflete
  automaticamente na Gold na próxima execução do pipeline, sem passo
  manual adicional.

**Para um backfill completo do zero** (recarregar todo o histórico do
início): apagar as tabelas Bronze/Silver/Gold, o checkpoint do Auto
Loader e a schema location, depois rodar a extração com o intervalo de
datas completo e o Workflow na sequência normal.

### 4. Lakeflow Declarative Pipelines (DLT) — implementação alternativa

`pipelines/dlt_selic_ipca.py` reimplementa as três camadas de forma
declarativa, usando `@dlt.table` e expectativas de qualidade
(`@dlt.expect_or_drop`, `@dlt.expect_or_fail`) em vez das checagens
explícitas em Python usadas no pipeline principal.

As tabelas geradas usam sufixo `_dlt` (`bronze_selic_dlt`,
`silver_selic_dlt`, etc.) para não colidir com o pipeline principal.

**Como rodar**: crie um Lakeflow Declarative Pipeline pela UI
(**Jobs & Pipelines** → **Create** → **ETL Pipeline**), aponte o
código-fonte para `pipelines/dlt_selic_ipca.py`, e defina destino
catalog=`analise_taxas`, schema=`selic_ipca`. O pipeline detecta as
dependências entre as tabelas automaticamente pelo grafo de `dlt.read`/
`dlt.read_stream`.
