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
puros, sem dependência de notebook). Os notebooks só chamam as funções que 
já estão prontas no src/ — a lógica de verdade fica toda lá, o notebook é só 
o ponto de entrada.— cada um vira uma task do Databricks Workflow.

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
├── evidencias/                     # prints de execução real
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
  sessão.


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

> **Observação**: As fórmulas estão implementadas em
> `src/gold.py::_calcula_indicadores`.

A tabela Gold é recriada por completo (`overwrite`) a cada execução —
como é uma agregação determinística a partir da Silver, isso é idempotente
por construção: mesma entrada sempre produz a mesma saída, para não ocorrer a duplicação de registros

## Checagens de qualidade de dados

Implementadas em `src/quality_checks.py`, cada uma levanta
`DataQualityError` quando violada — a exceção propaga sem ser capturada,
então a task do Databricks Workflow falha explicitamente
quando qualquer checagem é violada.

| # | Checagem | Onde é usada |
|---|---|---|
| 1 | `check_not_empty` | Bronze não pode ficar vazia após ingestão; Silver e Gold não podem ficar vazias após transformação |
| 2 | `check_column_not_null` | `vl_taxa` não pode ter nulos na Silver após tipagem |
| 3 | `check_value_range` | `vl_taxa` (SELIC/IPCA) dentro de [-20, 100]; `juro_real_mes` dentro de [-50, 50] — fora disso indica erro de parsing |
| 4 | `check_unique_key` | Chave de negócio (`dt_referencia` na Silver, `ano_mes` na Gold) não pode ter duplicatas |
| 5 | `check_no_gaps_in_months` | Não pode haver meses faltando na série mensal usada pela Gold |


## Como rodar

### 1. Setup do Unity Catalog (uma vez)

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

### 2. Extração local
```bash
pip install -r requirements.txt
python extract/extrai_selic_ipca.py
```
Gera `extract/dados_brutos/selic.json` e `extract/dados_brutos/ipca.json`
localmente, com retries e tratamento de erro. Termina com código de
saída `1` se qualquer série falhar — código de saída `0` significa
sucesso.

Para fazer o Upload dos arquivos: 

Cada série vai para sua própria subpasta dentro do Volume. Comandos devem ser executados via cli:
```bash
databricks fs cp extract/dados_brutos/selic.json dbfs:/Volumes/analise_taxas/selic_ipca/dados_brutos/selic/selic.json --overwrite
databricks fs cp extract/dados_brutos/ipca.json dbfs:/Volumes/analise_taxas/selic_ipca/dados_brutos/ipca/ipca.json --overwrite
```

Para fazer o upload diretamente para os volumes sem precisar passar por uma pasta local e depois fazer o upload, basta executar: 

``` bash
pip install -r requirements.txt
python extract/extrai_selic_ipca.py --upload
```

Agora, se quiser filtrar por data e já fazer o upload dos arquivos no volume:

```bash
pip install -r requirements.txt
python extract/extrai_selic_ipca.py --data-inicial 01/01/2025 --data-final 31/07/2026 --upload
```

Essa última chamada, é utilizando a estratégia de backfill.

**Observação:** Nesses casos em que já é feito o upload dos arquivos no mesmo script de criação, 
é OBRIGATÓRIO que o volume já esteja criado (passo 1. Setup do Unity Catalog). Além disso, requer autenticação já
configurada (mesmo perfil usado por `databricks configure`).

### 3. Importar o projeto no Databricks
Clone este repositório como um **Databricks Repo** (Git folder)
ou via `databricks sync`, preservando a estrutura `src/` +
`notebooks/`. Isso é o que permite o
`sys.path.append(os.path.abspath(".."))` nos notebooks localizar o
pacote `src` para importação. Evite `databricks workspace import-dir`
para essa estrutura, pois esse comando converte todo `.py` em notebook,
o que quebraria o import do pacote `src`.

### 4. Criar o Databricks Workflow

O Job inteiro (3 tasks encadeadas, compute serverless) já está definido
como código em `databricks.yml`. Basta rodar via cli:
```bash
databricks bundle validate
databricks bundle deploy -t dev
```
Isso cria o Job `Job - Carga Geral` no workspace automaticamente, com as
tasks já configuradas e encadeadas.

Para disparar uma execução:
```bash
databricks bundle run job_carga_geral -t dev
```

### 5. Evidência de idempotência
Para comprovar a idempotência, foi executado o Workflow duas vezes seguidas (sem alterar os arquivos no Volume). 
Após cada execução do workflow, foi executado o notebook `notebooks/04_verificacao_idempotencia.py` para comparar as
contagens de linhas antes/depois. A quantidade de linhas após as duas execuções permaneceram idênticas.

A pasta `evidencias/` contém prints ilustrando as duas execuções completas e bem-sucedidas do
Workflow (Bronze → Silver → Gold), com contagens de linhas idênticas
entre a primeira e a segunda execução.

| Arquivo | O que mostra |
|---|---|
| `01_job_duas_execucoes.png` | Tela de Runs do Job, com duas execuções concluídas com sucesso |
| `02_saida_idempotencia_execucao1.png` | Saída de `04_verificacao_idempotencia.py` após a 1ª execução do Workflow |
| `03_saida_idempotencia_execucao2.png` | Mesma saída após a 2ª execução — contagens idênticas às da execução 1 |
| `03_execução_do_DLT_pipeline.png` | Execução do pipeline DLT |
| `05_tabelas_unity_catalog.png` | Catalog Explorer mostrando as tabelas Bronze/Silver/Gold registradas em `analise_taxas.selic_ipca` |


### Pipelines (DLT) — implementação alternativa

`pipelines/dlt_selic_ipca.py` reimplementa as três camadas de forma
declarativa, usando `@dlt.table` e expectativas de qualidade
(`@dlt.expect_or_drop`, `@dlt.expect_or_fail`) em vez das checagens
explícitas em Python usadas no pipeline principal.

As tabelas geradas usam sufixo `_dlt` (`bronze_selic_dlt`,
`silver_selic_dlt`, etc.) para não colidir com o pipeline já montado.

**Como rodar**: crie um Lakeflow Declarative Pipeline pela UI
(**Jobs & Pipelines** → **Create** → **ETL Pipeline**), aponte o
código-fonte para `pipelines/dlt_selic_ipca.py`, e defina destino
catalog=`analise_taxas`, schema=`selic_ipca`. O pipeline detecta as
dependências entre as tabelas automaticamente pelo grafo de `dlt.read`/
`dlt.read_stream`.
