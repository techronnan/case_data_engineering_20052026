# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimClientes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.dim_clientes` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de clientes com surrogate key. Granularidade: 1 linha por cliente.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{SILVER}.crm_clientes"
TGT = f"{GOLD}.dim_clientes"

# COMMAND ----------

df = spark.table(SRC)

w = Window.orderBy("customer_code")

df_dim = (
    df
    .select(
        "customer_code", "name", "segment",
        "city", "state", "region_code", "created_at"
    )
    .withColumn("customer_key", row_number().over(w))
    .select(
        col("customer_key"),
        col("customer_code").alias("customer_id"),
        col("name").alias("customer_name"),
        col("segment"),
        col("city"),
        col("state"),
        col("region_code"),
        col("created_at"),
    )
)

print(f"dim_clientes: {df_dim.count():,} linhas")

# COMMAND ----------

write_delta(df_dim, TGT)
