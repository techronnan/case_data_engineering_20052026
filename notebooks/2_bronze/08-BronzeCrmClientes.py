# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeCrmClientes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.bronze.crm_clientes` |
# MAGIC | Origem Fonte de Dados de Entrada | `sources/crm_clientes_export.xlsx` |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |
# MAGIC
# MAGIC > **Nota:** Arquivos `.xlsx` não têm suporte nativo no Spark.
# MAGIC > Utiliza `pandas.read_excel()` + `spark.createDataFrame()` como alternativa ao `spark-excel`.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SOURCE_FILE  = f"{SOURCES_PATH}/crm_clientes_export.xlsx"
TARGET_TABLE = f"{BRONZE}.crm_clientes"

# COMMAND ----------

# Leitura via pandas (XLSX não tem suporte nativo no Spark)
# O prefixo /dbfs/ permite acesso ao DBFS via Python nativo
local_path = SOURCE_FILE.replace("/FileStore", "/dbfs/FileStore")

df_pd = pd.read_excel(local_path, dtype=str)
df = spark.createDataFrame(df_pd)
df = add_ingestion_metadata(df, SOURCE_FILE)

print(f"Linhas lidas : {df.count():,}")
print(f"Colunas      : {df.columns}")
df.printSchema()

# COMMAND ----------

write_delta(df, TARGET_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total, COUNT(DISTINCT customer_code) AS clientes_distintos
# MAGIC FROM workspace.bronze.crm_clientes
