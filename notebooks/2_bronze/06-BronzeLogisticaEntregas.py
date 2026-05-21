# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeLogisticaEntregas
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.bronze.logistica_entregas` |
# MAGIC | Origem Fonte de Dados de Entrada | `sources/logistica_entregas.json` (JSON Array) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SOURCE_FILE  = f"{SOURCES_PATH}/logistica_entregas.json"
TARGET_TABLE = f"{BRONZE}.logistica_entregas"

# COMMAND ----------

# JSON Array: arquivo único com array de objetos (multiLine=True)
df = (
    spark.read
    .option("multiLine", "true")
    .json(SOURCE_FILE)
)

df = add_ingestion_metadata(df, SOURCE_FILE)

print(f"Linhas lidas : {df.count():,}")
print("Schema (inclui structs aninhados):")
df.printSchema()

# COMMAND ----------

write_delta(df, TARGET_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total, COUNT(DISTINCT delivery_id) AS entregas_distintas
# MAGIC FROM workspace.bronze.logistica_entregas
