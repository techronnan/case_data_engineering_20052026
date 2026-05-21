# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeComercialCanais
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.bronze.comercial_canais` |
# MAGIC | Origem Fonte de Dados de Entrada | `sources/comercial_canais.xlsx` |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SOURCE_FILE  = f"{SOURCES_PATH}/comercial_canais.xlsx"
TARGET_TABLE = f"{BRONZE}.comercial_canais"

# COMMAND ----------

local_path = SOURCE_FILE.replace("/FileStore", "/dbfs/FileStore")

df_pd = pd.read_excel(local_path, dtype=str)
df = spark.createDataFrame(df_pd)
df = add_ingestion_metadata(df, SOURCE_FILE)

print(f"Linhas lidas : {df.count():,}")
df.show(truncate=False)

# COMMAND ----------

write_delta(df, TARGET_TABLE)
