# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimRegioes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.dim_regioes` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de regiões geográficas. Granularidade: 1 linha por região.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{SILVER}.legado_regioes"
TGT = f"{GOLD}.dim_regioes"

# COMMAND ----------

df = spark.table(SRC)

w = Window.orderBy("regional_code")

df_dim = (
    df
    .withColumn("region_key", row_number().over(w))
    .select(
        col("region_key"),
        col("regional_code"),
        col("region_name").alias("region_name"),
        col("manager").alias("region_manager"),
        col("state"),
    )
)

print(f"dim_regioes: {df_dim.count():,} linhas")
df_dim.show(truncate=False)

# COMMAND ----------

write_delta(df_dim, TGT)
