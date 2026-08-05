"""Camada Silver: tipagem, padronização de datas, tratamento de nulos e
carga idempotente via MERGE.

Chave de negócio: dt_referencia (data do registro, convertida a partir do
campo bruto "data"). Cada série (SELIC, IPCA) tem sua própria tabela
Silver, então a chave é única dentro de cada tabela — rodar o pipeline
mais de uma vez faz o MERGE atualizar ou ignorar registros já existentes,
nunca duplicá-los.
"""

from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from src.config import PipelineConfig
from src.quality_checks import (
    check_column_not_null,
    check_not_empty,
    check_unique_key,
    check_value_range,
)

CHAVE_NEGOCIO = "dt_referencia"
SERIES = ("selic", "ipca")


def _transforma_bronze_para_silver(df_bronze: DataFrame) -> DataFrame:
    """Tipagem, padronização de data e remoção de registros inválidos."""
    df = df_bronze.withColumn(
        CHAVE_NEGOCIO, F.to_date("data", "dd/MM/yyyy")
    ).withColumn("vl_taxa", F.col("valor").cast(DoubleType()))

    # Tratamento de nulos: descarta registros onde a data ou o valor não
    # puderam ser convertidos (indicativo de payload malformado na origem).
    df = df.filter(F.col(CHAVE_NEGOCIO).isNotNull() & F.col("vl_taxa").isNotNull())

    # Em caso de reingestão do mesmo dia (ex: reprocessamento do arquivo
    # bruto), mantém apenas o registro mais recente por data.
    janela_mais_recente = Window.partitionBy(CHAVE_NEGOCIO).orderBy(
        F.col("dt_ingestao").desc()
    )
    df = (
        df.withColumn("_rn", F.row_number().over(janela_mais_recente))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "data", "valor")
    )

    return df.select(CHAVE_NEGOCIO, "vl_taxa", "dt_ingestao", "nm_arquivo_origem")


def _merge_em_silver(spark: SparkSession, df_novo: DataFrame, tabela_destino: str) -> None:
    """Executa MERGE idempotente na tabela Silver, usando dt_referencia como chave."""
    if not spark.catalog.tableExists(tabela_destino):
        df_novo.write.format("delta").saveAsTable(tabela_destino)
        return

    tabela_delta = DeltaTable.forName(spark, tabela_destino)
    (
        tabela_delta.alias("destino")
        .merge(
            df_novo.alias("origem"),
            f"destino.{CHAVE_NEGOCIO} = origem.{CHAVE_NEGOCIO}",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_silver(spark: SparkSession, config: PipelineConfig, nome_serie: str) -> DataFrame:
    """Processa a camada Silver para uma série (selic ou ipca)."""
    tabela_bronze = config.full_table(f"bronze_{nome_serie}")
    tabela_silver = config.full_table(f"silver_{nome_serie}")

    df_bronze = spark.table(tabela_bronze)
    check_not_empty(df_bronze, tabela_bronze)

    df_silver = _transforma_bronze_para_silver(df_bronze)
    check_not_empty(df_silver, tabela_silver)
    check_column_not_null(df_silver, "vl_taxa", tabela_silver)

    # SELIC (% a.a.) e IPCA (% mensal) não deveriam ultrapassar faixas
    # plausíveis; valores fora disso indicam erro de parsing na origem.
    check_value_range(df_silver, "vl_taxa", -20.0, 100.0, tabela_silver)

    _merge_em_silver(spark, df_silver, tabela_silver)

    df_resultado = spark.table(tabela_silver)
    check_unique_key(df_resultado, CHAVE_NEGOCIO, tabela_silver)

    return df_resultado


def run_all_silver(spark: SparkSession, config: PipelineConfig) -> dict[str, DataFrame]:
    """Executa a camada Silver para todas as séries configuradas."""
    return {serie: run_silver(spark, config, serie) for serie in SERIES}
