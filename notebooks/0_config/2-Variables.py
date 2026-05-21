# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///

# Widget para receber o parâmetro 'catalog' do job; permite alternar entre dev/staging/prod
dbutils.widgets.text("catalog", "dev")
CATALOG = dbutils.widgets.get("catalog")

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD   = f"{CATALOG}.{GOLD_SCHEMA}"

# COMMAND ----------

# DBTITLE 1,Paths - Sources e Landing

SOURCES_VOLUME = f"/Volumes/{CATALOG}/landing/storage_files/sources_data"

SOURCES_PATH = SOURCES_VOLUME

# Parquet puro gerado pela camada Landing; lido pelos notebooks Bronze via AutoLoader
# Estrutura: systems/{sistema}/{ano}/{mes}/{file_name_YYYYMMDDHHMMSS}.parquet
LANDING_PATH = f"/Volumes/{CATALOG}/landing/storage_files/systems"

# COMMAND ----------

# Armazenados em UC Volume — /tmp/ é efêmero por task em jobs serverless
CHECKPOINT_BASE = f"{SOURCES_VOLUME}/_checkpoints"
SCHEMA_BASE     = f"{SOURCES_VOLUME}/_cloudfiles_schema"

# COMMAND ----------

CONTROL_TABLE = f"{CATALOG}.monitoring.pipeline_controller"

# COMMAND ----------

PIPELINE_NAME    = "case-data-engineering"
PIPELINE_VERSION = "1.0.0"
CREATED_BY       = "ronnan_ok@hotmail.com"

STRATEGY_FULL  = "FULL"
STRATEGY_DELTA = "DELTA"

# COMMAND ----------

# DBTITLE 1,Aliases e Log

# Aliases var_* mantidos para compatibilidade com notebooks existentes
var_environment   = CATALOG
var_bronze_schema = BRONZE_SCHEMA
var_silver_schema = SILVER_SCHEMA
var_gold_schema   = GOLD_SCHEMA
var_bronze        = BRONZE
var_silver        = SILVER
var_gold          = GOLD

print(f"✓ Variables carregadas | Catalog: {CATALOG} | Bronze/Silver/Gold configurados")
