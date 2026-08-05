# Databricks notebook source
# MAGIC %md
# MAGIC # Task Bronze — Ingestão incremental (Auto Loader)
# MAGIC Orquestra a ingestão dos arquivos `selic.json` e `ipca.json` do Volume
# MAGIC para as tabelas `bronze_selic` e `bronze_ipca`. Toda a lógica vive em
# MAGIC `src/bronze.py` — este notebook só chama a função e falha explicitamente
# MAGIC se qualquer exceção for levantada.

# COMMAND ----------

import os
import sys

# Torna o pacote `src` importável a partir deste notebook (estrutura de Repo:
# notebooks/ e src/ são pastas irmãs na raiz do projeto).
sys.path.append(os.path.abspath(".."))

from src.bronze import run_bronze
from src.config import DEFAULT_CONFIG

# COMMAND ----------

resultados = run_bronze(spark, DEFAULT_CONFIG)

for serie, df in resultados.items():
    total = df.count()
    print(f"bronze_{serie}: {total} registros")
    if total == 0:
        raise RuntimeError(f"Tabela bronze_{serie} ficou vazia após a ingestão.")