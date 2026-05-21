# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeVendedores
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.bronze.vendedores` |
# MAGIC | Origem Fonte de Dados de Entrada | `sources/vendedores.csv` |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SOURCE_FILE  = f"{SOURCES_PATH}/vendedores.csv"
TARGET_TABLE = f"{BRONZE}.vendedores"

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("inferSchema", "false")
    .option("encoding", "UTF-8")
    .csv(SOURCE_FILE)
)

df = add_ingestion_metadata(df, SOURCE_FILE)

print(f"Linhas lidas : {df.count():,}")
df.printSchema()

# COMMAND ----------

write_delta(df, TARGET_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total, COUNT(DISTINCT seller_id) AS vendedores_distintos
# MAGIC FROM workspace.bronze.vendedores
