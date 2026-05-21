# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimCanais
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.dim_canais` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de canais de venda. Granularidade: 1 linha por canal.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{SILVER}.comercial_canais"
TGT = f"{GOLD}.dim_canais"

# COMMAND ----------

df = spark.table(SRC)

w = Window.orderBy("channel_id")

df_dim = (
    df
    .withColumn("channel_key", row_number().over(w))
    .select(
        col("channel_key"),
        col("channel_id"),
        col("channel_name"),
        col("channel_type"),
        col("status"),
    )
)

print(f"dim_canais: {df_dim.count():,} linhas")
df_dim.show(truncate=False)

# COMMAND ----------

write_delta(df_dim, TGT)
