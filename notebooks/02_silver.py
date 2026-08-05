# Databricks notebook source
# MAGIC %md
# MAGIC # Task Silver — Tipagem, padronização e MERGE idempotente
# MAGIC Orquestra a camada Silver para SELIC e IPCA. Toda a lógica (tipagem,
# MAGIC tratamento de nulos, MERGE e checagens de qualidade) vive em
# MAGIC `src/silver.py` e `src/quality_checks.py`. Se qualquer checagem falhar,
# MAGIC a exceção `DataQualityError` propaga e a task falha explicitamente.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from src.config import DEFAULT_CONFIG
from src.silver import run_all_silver

# COMMAND ----------

resultados = run_all_silver(spark, DEFAULT_CONFIG)

for serie, df in resultados.items():
    print(f"silver_{serie}: {df.count()} registros")