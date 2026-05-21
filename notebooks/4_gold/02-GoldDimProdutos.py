# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimProdutos
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.dim_produtos` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de produtos com surrogate key. Granularidade: 1 linha por produto.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{SILVER}.cadastro_produtos"
TGT = f"{GOLD}.dim_produtos"

# COMMAND ----------

df = spark.table(SRC)

w = Window.orderBy("product_code")

df_dim = (
    df
    .filter(col("status") == "ATIVO")
    .withColumn("product_key", row_number().over(w))
    .select(
        col("product_key"),
        col("product_code").alias("product_id"),
        col("product_name"),
        col("category"),
        col("subcategory"),
        col("family"),
        col("list_price"),
        col("currency"),
        col("tags"),
        col("status"),
    )
)

print(f"dim_produtos: {df_dim.count():,} linhas")

# COMMAND ----------

write_delta(df_dim, TGT)
