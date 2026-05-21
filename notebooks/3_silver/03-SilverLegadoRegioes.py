# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverLegadoRegioes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.legado_regioes` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** normaliza `regional_code` (sul→S), remove região XX inativa,
# MAGIC deduplica por `active_flag=1`, prioriza registro com nome mais específico.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{BRONZE}.legado_regioes"
TGT = f"{SILVER}.legado_regioes"

# COMMAND ----------

df = spark.table(SRC)

# Normalização de regional_code abreviado vs extenso
REGION_CODE_MAP = {"SUL": "S", "NORTE": "N", "NORDESTE": "NE",
                   "CENTRO": "CO", "SUDESTE": "SE", "CO": "CO"}

from pyspark.sql.functions import create_map
from itertools import chain
map_expr = create_map([lit(x) for x in chain(*REGION_CODE_MAP.items())])

df_norm = (
    df
    .withColumn("regional_code", upper(trim(col("regional_code"))))
    .withColumn("regional_code",
        coalesce(map_expr[col("regional_code")], col("regional_code")))
    .withColumn("active_flag", col("active_flag").cast(IntegerType()))
)

# Remover região XX (inativa sem dados)
df_active = df_norm.filter(
    (col("active_flag") == 1) & (col("regional_code") != "XX")
)

# Deduplicar: manter 1 registro por regional_code (maior especificidade = nome mais longo)
w = Window.partitionBy("regional_code").orderBy(col("active_flag").desc())
df_silver = (
    df_active
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumn("_silver_at", current_timestamp())
)

print(f"Linhas bronze : {df.count():,}")
print(f"Linhas silver : {df_silver.count():,}")
df_silver.show(truncate=False)

# COMMAND ----------

write_delta(df_silver, TGT)
