"""Camada Bronze: ingestão incremental dos arquivos brutos do Volume."""

from __future__ import annotations

import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StringType, StructField, StructType

from src.config import PipelineConfig

SCHEMA_BRUTO = StructType(
    [
        StructField("data", StringType(), True),
        StructField("valor", StringType(), True),
    ]
)

SERIES = ("selic", "ipca")


def _aguarda_conclusao(query: StreamingQuery, intervalo_segundos: float = 2.0) -> None:
    while query.isActive:
        time.sleep(intervalo_segundos)
    erro = query.exception()
    if erro is not None:
        raise erro


def ingest_bronze(spark: SparkSession, config: PipelineConfig, nome_serie: str) -> DataFrame:
    spark.conf.set("spark.sql.shuffle.partitions", "1")

    tabela_destino = config.full_table(f"bronze_{nome_serie}")
    checkpoint_path = f"{config.checkpoint_root}/bronze_{nome_serie}/checkpoint"
    schema_location = f"{config.checkpoint_root}/bronze_{nome_serie}/schema"
    caminho_pasta = f"{config.volume_path}/{nome_serie}"

    df_bronze = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("multiLine", "true")
        .schema(SCHEMA_BRUTO)
        .load(caminho_pasta)
        .withColumn("nm_arquivo_origem", col("_metadata.file_name"))
        .withColumn("dt_ingestao", current_timestamp())
    )

    query = (
        df_bronze.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(availableNow=True)
        .queryName(f"bronze_{nome_serie}_{int(time.time())}")  # nome único a cada chamada
        .toTable(tabela_destino)
    )

    _aguarda_conclusao(query)
    query.stop()

    # Limpeza: garante que nenhuma referência à query fica pendurada na
    # sessão antes de retornar, o que poderia atrapalhar a próxima chamada.
    spark.streams.resetTerminated()

    return spark.table(tabela_destino)


def run_bronze(spark: SparkSession, config: PipelineConfig) -> dict[str, DataFrame]:
    """Executa a ingestão Bronze para todas as séries configuradas."""
    resultados = {}

    for i, serie in enumerate(SERIES):
        if i > 0:
            time.sleep(5)
        print(f"Iniciando ingestão Bronze da série: {serie}...")
        resultados[serie] = ingest_bronze(spark, config, serie)
        print(f"Finalizada ingestão da série: {serie}")

    return resultados