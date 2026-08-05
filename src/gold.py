"""Camada Gold: indicadores mensais consolidados.

Grão da tabela: um registro por mês (coluna ano_mes, formato "yyyy-MM").

Metodologia (ver também README.md):
- selic_media_mes: média simples das taxas SELIC diárias (% a.a.) do mês.
- ipca_mes: variação mensal do IPCA (%) reportada pela série do BCB.
- juro_real_mes: juro real do mês. A SELIC anualizada é convertida para uma
  taxa mensal equivalente por juros compostos, e o juro real é obtido pela
  fórmula de Fisher: ((1 + selic_mensal) / (1 + ipca_mes) - 1).
- taxa_acumulada_12m: IPCA acumulado nos últimos 12 meses (janela móvel),
  via produtório de (1 + ipca_mes/100) - 1. Fica nulo nos primeiros 11
  meses da série, onde a janela de 12 meses ainda não está completa —
  isso é esperado e não indica erro de qualidade.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import PipelineConfig
from src.quality_checks import (
    check_no_gaps_in_months,
    check_not_empty,
    check_unique_key,
    check_value_range,
)

GRAO = "ano_mes"


def _agrega_selic_mensal(df_silver_selic: DataFrame) -> DataFrame:
    return (
        df_silver_selic.withColumn(GRAO, F.date_format("dt_referencia", "yyyy-MM"))
        .groupBy(GRAO)
        .agg(F.avg("vl_taxa").alias("selic_media_mes"))
    )


def _agrega_ipca_mensal(df_silver_ipca: DataFrame) -> DataFrame:
    return (
        df_silver_ipca.withColumn(GRAO, F.date_format("dt_referencia", "yyyy-MM"))
        .groupBy(GRAO)
        .agg(F.avg("vl_taxa").alias("ipca_mes"))
    )


def _calcula_indicadores(df: DataFrame) -> DataFrame:
    selic_mensal_equivalente = F.pow(1 + F.col("selic_media_mes") / 100, F.lit(1 / 12)) - 1
    df = df.withColumn(
        "juro_real_mes",
        ((1 + selic_mensal_equivalente) / (1 + F.col("ipca_mes") / 100) - 1) * 100,
    )

    janela_12m = Window.orderBy(GRAO).rowsBetween(-11, 0)
    df = df.withColumn("_log_fator_ipca", F.log(1 + F.col("ipca_mes") / 100))
    df = df.withColumn("_soma_log_12m", F.sum("_log_fator_ipca").over(janela_12m))
    df = df.withColumn("_qtd_meses_janela", F.count("_log_fator_ipca").over(janela_12m))
    df = df.withColumn(
        "taxa_acumulada_12m",
        F.when(F.col("_qtd_meses_janela") == 12, (F.exp("_soma_log_12m") - 1) * 100),
    )
    return df.drop("_log_fator_ipca", "_soma_log_12m", "_qtd_meses_janela")


def run_gold(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Recalcula a tabela Gold a partir das tabelas Silver mais atuais."""
    tabela_silver_selic = config.full_table("silver_selic")
    tabela_silver_ipca = config.full_table("silver_ipca")
    tabela_gold = config.full_table("gold_indicadores_mensais")

    df_selic_mensal = _agrega_selic_mensal(spark.table(tabela_silver_selic))
    df_ipca_mensal = _agrega_ipca_mensal(spark.table(tabela_silver_ipca))

    df_gold = df_selic_mensal.join(df_ipca_mensal, on=GRAO, how="inner")
    check_not_empty(df_gold, tabela_gold)
    check_no_gaps_in_months(df_gold, GRAO, tabela_gold)

    df_gold = _calcula_indicadores(df_gold).orderBy(GRAO)
    check_value_range(df_gold, "juro_real_mes", -50.0, 50.0, tabela_gold)

    # A Gold é uma agregação determinística a partir da Silver: recriar a
    # tabela inteira a cada execução é idempotente por construção (mesma
    # entrada sempre produz a mesma saída, sem risco de duplicar linhas).
    df_gold.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela_gold)

    df_resultado = spark.table(tabela_gold)
    check_unique_key(df_resultado, GRAO, tabela_gold)

    return df_resultado
