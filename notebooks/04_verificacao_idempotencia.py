# Databricks notebook source
# MAGIC %md
# MAGIC # Verificação de idempotência
# MAGIC
# MAGIC Como usar como evidência:
# MAGIC 1. Rode o Workflow completo (Bronze → Silver → Gold) uma primeira vez.
# MAGIC 2. Rode esta célula e **tire um print** do resultado (contagens por tabela).
# MAGIC 3. Rode o Workflow completo de novo, **sem alterar os arquivos no Volume**.
# MAGIC 4. Rode esta célula de novo e tire outro print.
# MAGIC 5. As contagens devem ser **idênticas** nas duas execuções — essa
# MAGIC    comparação lado a lado é a evidência de que o pipeline não duplica
# MAGIC    registros ao ser reexecutado.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath(".."))

from src.config import DEFAULT_CONFIG

config = DEFAULT_CONFIG

tabelas = [
    "bronze_selic",
    "bronze_ipca",
    "silver_selic",
    "silver_ipca",
    "gold_indicadores_mensais",
]

# COMMAND ----------

for tabela in tabelas:
    nome_completo = config.full_table(tabela)
    total = spark.table(nome_completo).count()
    print(f"{nome_completo}: {total} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checagem adicional: nenhuma chave duplicada nas tabelas Silver/Gold
# MAGIC Reforça a evidência indo além da contagem total — confirma que a chave
# MAGIC de negócio (`dt_referencia` na Silver, `ano_mes` na Gold) não tem
# MAGIC repetições após duas execuções.

# COMMAND ----------

from pyspark.sql import functions as F

checagens = [
    ("silver_selic", "dt_referencia"),
    ("silver_ipca", "dt_referencia"),
    ("gold_indicadores_mensais", "ano_mes"),
]

for tabela, chave in checagens:
    nome_completo = config.full_table(tabela)
    df = spark.table(nome_completo)
    total = df.count()
    distintos = df.select(chave).distinct().count()
    status = "OK - sem duplicatas" if total == distintos else "FALHOU - há duplicatas"
    print(f"{nome_completo}: total={total}, distintos={distintos} -> {status}")