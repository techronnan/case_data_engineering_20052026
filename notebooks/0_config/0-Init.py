# Databricks notebook source
# Entry point único de configuração do pipeline
# Carrega em sequência: Libs → Variables → Functions
# Uso: %run ../0_config/0-Init no início de cada notebook

# Configura timezone padrão para São Paulo (UTC-3)
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

print(f"[Init] Spark {spark.version} | TZ: America/Sao_Paulo")

# COMMAND ----------

# MAGIC %run ./1-Libs

# COMMAND ----------

# MAGIC %run ./2-Variables

# COMMAND ----------

# MAGIC %run ./3-Functions

# COMMAND ----------

print(f"✓ Config carregado | {PIPELINE_NAME} v{PIPELINE_VERSION} | Catalog: {CATALOG}")
