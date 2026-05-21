# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimVendedores
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.dim_vendedores` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de vendedores. Join com dim_regioes e dim_canais para resolver FKs.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC_VEND   = f"{SILVER}.vendedores"
SRC_REG    = f"{GOLD}.dim_regioes"
SRC_CAN    = f"{GOLD}.dim_canais"
TGT        = f"{GOLD}.dim_vendedores"

# COMMAND ----------

df_vend = spark.table(SRC_VEND)
df_reg  = spark.table(SRC_REG).select("region_key", "regional_code")
df_can  = spark.table(SRC_CAN).select("channel_key", "channel_id")

w = Window.orderBy("seller_id")

df_dim = (
    df_vend
    .join(df_reg, df_vend["regional_code"] == df_reg["regional_code"], "left")
    .join(df_can, df_vend["canal_id"]      == df_can["channel_id"],     "left")
    .withColumn("seller_key", row_number().over(w))
    .select(
        col("seller_key"),
        col("seller_id"),
        col("seller_name").alias("seller_name"),
        col("region_key"),
        col("channel_key"),
        col("hire_date"),
        col("status"),
    )
)

print(f"dim_vendedores: {df_dim.count():,} linhas")
print(f"Sem região    : {df_dim.filter(col('region_key').isNull()).count():,}")
print(f"Sem canal     : {df_dim.filter(col('channel_key').isNull()).count():,}")

# COMMAND ----------

write_delta(df_dim, TGT)
