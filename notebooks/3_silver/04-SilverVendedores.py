# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverVendedores
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.vendedores` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** normaliza `regional_code`, `canal_id`, `status`, `hire_date` multi-formato,
# MAGIC deduplicação de V004 e V008 (duplicatas reais detectadas na análise).

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{BRONZE}.vendedores"
TGT = f"{SILVER}.vendedores"

# COMMAND ----------

df = spark.table(SRC)

df_norm = (
    df
    .withColumn("seller_id",      upper(trim(col("seller_id"))))
    .withColumn("canal_id",       upper(trim(col("canal_id"))))
    .withColumn("status",         upper(trim(col("status"))))
    .withColumn("regional_code",  upper(trim(col("regional_code"))))
    .withColumn("hire_date",      parse_date_multi("hire_date"))
)

# Normalizar regional_code (sul → S, etc.)
from pyspark.sql.functions import create_map
from itertools import chain
REGION_MAP = {"SUL": "S", "NORTE": "N", "NORDESTE": "NE", "SUDESTE": "SE", "CENTRO": "CO"}
map_expr   = create_map([lit(x) for x in chain(*REGION_MAP.items())])
df_norm = df_norm.withColumn("regional_code",
    coalesce(map_expr[col("regional_code")], col("regional_code")))

# Deduplicação: manter 1 registro por seller_id (mais recente por hire_date)
w = Window.partitionBy("seller_id").orderBy(col("hire_date").desc())
df_silver = (
    df_norm
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumn("_silver_at", current_timestamp())
)

print(f"Bronze : {df.count():,}  |  Silver (dedup): {df_silver.count():,}")

# COMMAND ----------

write_delta(df_silver, TGT)
