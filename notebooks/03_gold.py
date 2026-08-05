# Databricks notebook source
# MAGIC %md
# MAGIC # Task Gold — Indicadores mensais consolidados
# MAGIC Orquestra a camada Gold: agrega SELIC e IPCA por mês, calcula juro real
# MAGIC e taxa acumulada em 12 meses. Lógica completa em `src/gold.py`.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from src.config import DEFAULT_CONFIG
from src.gold import run_gold

# COMMAND ----------

df_gold = run_gold(spark, DEFAULT_CONFIG)
print(f"gold_indicadores_mensais: {df_gold.count()} registros")
display(df_gold)