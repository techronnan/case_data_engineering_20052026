# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverLogisticaEntregas
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.logistica_entregas` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** flatten de structs aninhados (carrier, timestamps, destination),
# MAGIC normaliza UF, calcula `delivery_days`, flag `is_late`.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{BRONZE}.logistica_entregas"
TGT = f"{SILVER}.logistica_entregas"

# COMMAND ----------

df = spark.table(SRC)

df_flat = (
    df
    .select(
        upper(trim(col("delivery_id"))).alias("delivery_id"),
        upper(trim(col("order_ref"))).alias("order_id"),
        col("carrier.name").alias("carrier_name"),
        col("carrier.mode").alias("carrier_mode"),
        upper(trim(col("delivery_status"))).alias("delivery_status"),
        parse_timestamp_multi("timestamps.shipped_at").alias("shipped_at"),
        parse_timestamp_multi("timestamps.delivered_at").alias("delivered_at"),
        col("destination.state").alias("dest_state_raw"),
        col("destination.city").alias("dest_city"),
        col("cost").cast(DoubleType()).alias("cost"),
        col("_source_file"),
        col("_ingested_at"),
    )
    .withColumn("delivery_days", datediff(col("delivered_at"), col("shipped_at")))
    .withColumn("is_late",
        when(col("delivery_days") > 7, lit(True)).otherwise(lit(False)))
    .withColumn("_silver_at", current_timestamp())
)

# Normalizar UF
df_silver = normalize_uf(df_flat, "dest_state_raw")
df_silver = df_silver.withColumnRenamed("dest_state_raw", "dest_state")

print(f"Linhas : {df_silver.count():,}")
print(f"Atrasadas : {df_silver.filter(col('is_late')).count():,}")

# COMMAND ----------

write_delta(df_silver, TGT)
