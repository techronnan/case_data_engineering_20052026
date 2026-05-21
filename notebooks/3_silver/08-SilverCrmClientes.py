# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverCrmClientes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.crm_clientes` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** `customer_code` uppercase, normaliza UF/estado,
# MAGIC trim em campos de texto, deduplicação por `customer_code`.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{BRONZE}.crm_clientes"
TGT = f"{SILVER}.crm_clientes"

# COMMAND ----------

df = spark.table(SRC)

df_norm = (
    df
    .withColumn("customer_code", upper(trim(col("customer_code"))))
    .withColumn("name",          trim(col("name")))
    .withColumn("segment",       upper(trim(col("segment"))))
    .withColumn("city",          trim(col("city")))
    .withColumn("region_code",   upper(trim(col("region_code"))))
    .withColumn("created_at",    parse_date_multi("created_at"))
    .withColumn("_silver_at",    current_timestamp())
)

df_norm = normalize_uf(df_norm, "state")

# Deduplicar por customer_code (manter mais recente)
w = Window.partitionBy("customer_code").orderBy(col("created_at").desc())
df_silver = (
    df_norm
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

print(f"Bronze: {df.count():,}  |  Silver (dedup): {df_silver.count():,}")

# COMMAND ----------

write_delta(df_silver, TGT)
