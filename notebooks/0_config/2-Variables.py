# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 2-Variables
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Variáveis globais de path, catalog e schema |
# MAGIC | Executado Via | `4-Config` — não executar diretamente |

# COMMAND ----------

# Unity Catalog
CATALOG        = "workspace"
BRONZE_SCHEMA  = "bronze"
SILVER_SCHEMA  = "silver"
GOLD_SCHEMA    = "gold"

# Referências completas (catalog.schema)
BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD   = f"{CATALOG}.{GOLD_SCHEMA}"

# DBFS — fontes brutas (após upload manual ou via CLI)
SOURCES_PATH = "/FileStore/case/sources"

# Identificação do pipeline
PIPELINE_NAME    = "case-data-engineering"
PIPELINE_VERSION = "1.0.0"
CREATED_BY       = "ronnan_ok@hotmail.com"

print(f"[Variables] Catalog : {CATALOG}")
print(f"[Variables] Bronze  : {BRONZE}")
print(f"[Variables] Silver  : {SILVER}")
print(f"[Variables] Gold    : {GOLD}")
print(f"[Variables] Sources : {SOURCES_PATH}")
