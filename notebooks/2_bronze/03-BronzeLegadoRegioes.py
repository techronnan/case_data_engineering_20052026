# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeLegadoRegioes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.bronze.legado_regioes` |
# MAGIC | Origem Fonte de Dados de Entrada | `sources/legado_regioes_pipe.txt` |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SOURCE_FILE  = f"{SOURCES_PATH}/legado_regioes_pipe.txt"
TARGET_TABLE = f"{BRONZE}.legado_regioes"

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .option("sep", "|")
    .option("inferSchema", "false")
    .csv(SOURCE_FILE)
)

df = add_ingestion_metadata(df, SOURCE_FILE)

print(f"Linhas lidas : {df.count():,}")
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

write_delta(df, TARGET_TABLE)
