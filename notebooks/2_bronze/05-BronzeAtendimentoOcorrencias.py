# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeAtendimentoOcorrencias
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.bronze.atendimento_ocorrencias` |
# MAGIC | Origem Fonte de Dados de Entrada | `sources/atendimento_ocorrencias.ndjson` (NDJSON) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SOURCE_FILE  = f"{SOURCES_PATH}/atendimento_ocorrencias.ndjson"
TARGET_TABLE = f"{BRONZE}.atendimento_ocorrencias"

# COMMAND ----------

# NDJSON = JSON Lines: cada linha é um objeto JSON independente (multiLine=False)
df = (
    spark.read
    .option("multiLine", "false")
    .json(SOURCE_FILE)
)

df = add_ingestion_metadata(df, SOURCE_FILE)

print(f"Linhas lidas : {df.count():,}")
df.printSchema()

# COMMAND ----------

write_delta(df, TARGET_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total, COUNT(DISTINCT ticket_id) AS tickets_distintos
# MAGIC FROM workspace.bronze.atendimento_ocorrencias
