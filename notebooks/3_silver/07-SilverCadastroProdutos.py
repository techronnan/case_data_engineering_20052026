# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverCadastroProdutos
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.cadastro_produtos` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** flatten multinível (product, pricing, attributes), status uppercase,
# MAGIC tags array → string concatenada com `|` para compatibilidade BI.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{BRONZE}.cadastro_produtos"
TGT = f"{SILVER}.cadastro_produtos"

# COMMAND ----------

df = spark.table(SRC)

df_silver = (
    df
    .select(
        upper(trim(col("product.product_id"))).alias("product_code"),
        col("product.name").alias("product_name"),
        col("product.category").alias("category"),
        col("product.subcategory").alias("subcategory"),
        upper(trim(col("product.status"))).alias("status"),
        col("pricing.list_price").cast(DoubleType()).alias("list_price"),
        col("pricing.currency").alias("currency"),
        col("attributes.family").alias("family"),
        concat_ws("|", col("attributes.tags")).alias("tags"),
        parse_timestamp_multi("updated_at").alias("updated_at"),
        col("_source_file"),
        col("_ingested_at"),
    )
    .withColumn("_silver_at", current_timestamp())
)

print(f"Linhas : {df_silver.count():,}")
df_silver.groupBy("category").count().orderBy("count", ascending=False).show()

# COMMAND ----------

write_delta(df_silver, TGT)
