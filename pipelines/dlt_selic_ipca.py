"""Pipeline Lakeflow Declarative Pipelines (DLT) para SELIC/IPCA.

IMPLEMENTAÇÃO ALTERNATIVA / ADICIONAL — ver README para contexto completo.

Este arquivo é uma implementação declarativa das camadas Bronze, Silver e
Gold usando DLT, com expectativas de qualidade (@dlt.expect_*) em vez das
checagens explícitas em `src/quality_checks.py`. As tabelas geradas usam
sufixo `_dlt` para não colidir com as tabelas do pipeline principal
(orquestrado via `src/` + Databricks Workflow), que é a entrega primária
e validada com duas execuções bem-sucedidas.

Como rodar: este arquivo NÃO é executado como notebook comum. Ele precisa
ser configurado como código-fonte de um Lakeflow Declarative Pipeline
(Workflows > Pipelines > Create Pipeline), apontando pra este arquivo (ou
para a pasta `pipelines/`), com destino: catalog=analise_taxas,
schema=selic_ipca.

Diferença de tratamento de qualidade em relação a `src/quality_checks.py`:
- `expect_or_drop`: registro que viola a regra é descartado silenciosamente
  (mas contabilizado nas métricas do pipeline), sem derrubar a execução.
- `expect_or_fail`: qualquer violação derruba o pipeline inteiro
  (equivalente à falha explícita usada no pipeline principal).
- `expect` (sem sufixo): registro é mantido mesmo violando a regra, só
  fica registrado nas métricas — útil para regras "de observação".
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

CATALOG = "analise_taxas"
SCHEMA = "selic_ipca"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/dados_brutos"

SCHEMA_BRUTO = StructType(
    [
        StructField("data", StringType(), True),
        StructField("valor", StringType(), True),
    ]
)


def _cria_tabela_bronze(nome_serie: str):
    """Cria uma tabela DLT Bronze para a série informada, via Auto Loader."""

    @dlt.table(
        name=f"bronze_{nome_serie}_dlt",
        comment=f"Ingestão bruta incremental da série {nome_serie} (Auto Loader gerenciado pelo DLT)",
    )
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .schema(SCHEMA_BRUTO)
            .load(f"{VOLUME_PATH}/{nome_serie}")
            .withColumn("nm_arquivo_origem", F.col("_metadata.file_name"))
            .withColumn("dt_ingestao", F.current_timestamp())
        )

    return _bronze


bronze_selic_dlt = _cria_tabela_bronze("selic")
bronze_ipca_dlt = _cria_tabela_bronze("ipca")


def _cria_tabela_silver(nome_serie: str):
    """Cria uma tabela DLT Silver, tipada, com expectativas de qualidade."""

    @dlt.table(
        name=f"silver_{nome_serie}_dlt",
        comment=f"Camada Silver tipada da série {nome_serie}",
    )
    @dlt.expect_or_drop("data_valida", "dt_referencia IS NOT NULL")
    @dlt.expect_or_drop("valor_valido", "vl_taxa IS NOT NULL")
    @dlt.expect_or_fail("valor_em_faixa_plausivel", "vl_taxa BETWEEN -20 AND 100")
    def _silver():
        return (
            dlt.read_stream(f"bronze_{nome_serie}_dlt")
            .withColumn("dt_referencia", F.to_date("data", "dd/MM/yyyy"))
            .withColumn("vl_taxa", F.col("valor").cast(DoubleType()))
            .select("dt_referencia", "vl_taxa", "dt_ingestao", "nm_arquivo_origem")
        )

    return _silver


silver_selic_dlt = _cria_tabela_silver("selic")
silver_ipca_dlt = _cria_tabela_silver("ipca")


@dlt.table(
    name="gold_indicadores_mensais_dlt",
    comment="Indicadores mensais consolidados: SELIC média, IPCA, juro real e acumulado 12m",
)
@dlt.expect_or_fail("ano_mes_presente", "ano_mes IS NOT NULL")
@dlt.expect_or_fail("juro_real_em_faixa_plausivel", "juro_real_mes BETWEEN -50 AND 50")
def gold_indicadores_mensais_dlt():
    df_selic_mensal = (
        dlt.read("silver_selic_dlt")
        .withColumn("ano_mes", F.date_format("dt_referencia", "yyyy-MM"))
        .groupBy("ano_mes")
        .agg(F.avg("vl_taxa").alias("selic_media_mes"))
    )
    df_ipca_mensal = (
        dlt.read("silver_ipca_dlt")
        .withColumn("ano_mes", F.date_format("dt_referencia", "yyyy-MM"))
        .groupBy("ano_mes")
        .agg(F.avg("vl_taxa").alias("ipca_mes"))
    )

    df = df_selic_mensal.join(df_ipca_mensal, on="ano_mes", how="inner")

    selic_mensal_equivalente = F.pow(1 + F.col("selic_media_mes") / 100, F.lit(1 / 12)) - 1
    df = df.withColumn(
        "juro_real_mes",
        ((1 + selic_mensal_equivalente) / (1 + F.col("ipca_mes") / 100) - 1) * 100,
    )

    janela_12m = Window.orderBy("ano_mes").rowsBetween(-11, 0)
    df = df.withColumn("_log_fator_ipca", F.log(1 + F.col("ipca_mes") / 100))
    df = df.withColumn("_soma_log_12m", F.sum("_log_fator_ipca").over(janela_12m))
    df = df.withColumn("_qtd_meses_janela", F.count("_log_fator_ipca").over(janela_12m))
    df = df.withColumn(
        "taxa_acumulada_12m",
        F.when(F.col("_qtd_meses_janela") == 12, (F.exp("_soma_log_12m") - 1) * 100),
    )

    return df.drop("_log_fator_ipca", "_soma_log_12m", "_qtd_meses_janela")
