"""Checagens de qualidade de dados entre as camadas do pipeline.

Cada checagem levanta DataQualityError quando violada. As camadas do
pipeline (notebooks Bronze/Silver/Gold) devem deixar essa exceção propagar
sem capturá-la, para que a task do Databricks Workflow falhe de forma
explícita (task em vermelho) em vez de deixar dados inválidos avançarem
silenciosamente para a próxima camada.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(Exception):
    """Levantada quando uma checagem de qualidade de dados falha."""


def check_not_empty(df: DataFrame, nome_tabela: str) -> None:
    """Falha se o DataFrame/tabela não tiver nenhum registro."""
    total = df.count()
    if total == 0:
        raise DataQualityError(
            f"[{nome_tabela}] Checagem falhou: tabela/DataFrame está vazio."
        )


def check_column_not_null(
    df: DataFrame, coluna: str, nome_tabela: str, tolerancia: int = 0
) -> None:
    """Falha se houver mais nulos na coluna do que a tolerância permitida."""
    nulos = df.filter(F.col(coluna).isNull()).count()
    if nulos > tolerancia:
        raise DataQualityError(
            f"[{nome_tabela}] Checagem falhou: {nulos} valor(es) nulo(s) "
            f"na coluna '{coluna}' (tolerância: {tolerancia})."
        )


def check_unique_key(df: DataFrame, chave: str, nome_tabela: str) -> None:
    """Falha se a coluna informada como chave de negócio tiver duplicatas."""
    total = df.count()
    distintos = df.select(chave).distinct().count()
    if total != distintos:
        raise DataQualityError(
            f"[{nome_tabela}] Checagem falhou: chave de negócio '{chave}' "
            f"não é única. Total de linhas: {total}, valores distintos: {distintos}."
        )


def check_value_range(
    df: DataFrame, coluna: str, minimo: float, maximo: float, nome_tabela: str
) -> None:
    """Falha se algum valor da coluna estiver fora do intervalo plausível."""
    fora_do_intervalo = df.filter(
        (F.col(coluna) < minimo) | (F.col(coluna) > maximo)
    ).count()
    if fora_do_intervalo > 0:
        raise DataQualityError(
            f"[{nome_tabela}] Checagem falhou: {fora_do_intervalo} registro(s) "
            f"com '{coluna}' fora do intervalo [{minimo}, {maximo}]."
        )


def check_no_gaps_in_months(df: DataFrame, coluna_ano_mes: str, nome_tabela: str) -> None:
    """Falha se houver meses faltando na sequência (yyyy-MM) da série."""
    meses = [
        row[coluna_ano_mes]
        for row in df.select(coluna_ano_mes).distinct().orderBy(coluna_ano_mes).collect()
    ]
    if not meses:
        raise DataQualityError(f"[{nome_tabela}] Checagem falhou: nenhum mês encontrado.")

    esperados = _gera_sequencia_meses(meses[0], meses[-1])
    faltando = sorted(set(esperados) - set(meses))
    if faltando:
        raise DataQualityError(
            f"[{nome_tabela}] Checagem falhou: meses ausentes na série: {faltando}."
        )


def _gera_sequencia_meses(inicio: str, fim: str) -> list[str]:
    """Gera a lista de meses 'yyyy-MM' entre inicio e fim, inclusive."""
    ano_i, mes_i = int(inicio[:4]), int(inicio[5:7])
    ano_f, mes_f = int(fim[:4]), int(fim[5:7])

    sequencia = []
    ano, mes = ano_i, mes_i
    while (ano, mes) <= (ano_f, mes_f):
        sequencia.append(f"{ano:04d}-{mes:02d}")
        mes += 1
        if mes > 12:
            mes = 1
            ano += 1
    return sequencia
