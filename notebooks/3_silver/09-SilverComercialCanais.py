# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverComercialCanais
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.comercial_canais` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** `channel_id` uppercase, trim em campos de texto, status normalizado.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{BRONZE}.comercial_canais"
TGT = f"{SILVER}.comercial_canais"

# COMMAND ----------

df = spark.table(SRC)

df_silver = (
    df
    .withColumn("channel_id",   upper(trim(col("channel_id"))))
    .withColumn("channel_name", trim(col("channel_name")))
    .withColumn("channel_type", upper(trim(col("channel_type"))))
    .withColumn("status",       upper(trim(col("status"))))
    .withColumn("_silver_at",   current_timestamp())
)

print(f"Linhas : {df_silver.count():,}")
df_silver.show(truncate=False)

# COMMAND ----------

write_delta(df_silver, TGT)
