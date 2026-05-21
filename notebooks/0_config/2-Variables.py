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
# MAGIC | Finalidade | Variáveis globais de catalog, schema, paths e estratégia de carga |
# MAGIC | Executado Via | `4-Config` — não executar diretamente |

# COMMAND ----------

# Unity Catalog — 3-part naming
CATALOG       = "workspace"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD   = f"{CATALOG}.{GOLD_SCHEMA}"

# Volume Unity Catalog para arquivos de fonte (preferível ao DBFS FileStore)
SOURCES_VOLUME = "/Volumes/workspace/default/sources"

# Fallback DBFS (caso o Volume ainda não tenha os arquivos)
SOURCES_DBFS   = "/FileStore/case/sources"

# Path ativo (alterar para SOURCES_DBFS se necessário)
SOURCES_PATH   = SOURCES_VOLUME

# Identificação do pipeline
PIPELINE_NAME    = "case-data-engineering"
PIPELINE_VERSION = "1.0.0"
CREATED_BY       = "ronnan_ok@hotmail.com"

# Estratégias disponíveis
STRATEGY_FULL  = "FULL"   # Bronze + Gold Dims
STRATEGY_DELTA = "DELTA"  # Silver + Gold Facts

# Aliases padrão var_environment (compatibilidade com notebooks de referência)
var_environment   = CATALOG
var_bronze_schema = BRONZE_SCHEMA
var_silver_schema = SILVER_SCHEMA
var_gold_schema   = GOLD_SCHEMA
var_bronze        = BRONZE
var_silver        = SILVER
var_gold          = GOLD

print(f"[Variables] Catalog  : {CATALOG}")
print(f"[Variables] Bronze   : {BRONZE}")
print(f"[Variables] Silver   : {SILVER}")
print(f"[Variables] Gold     : {GOLD}")
print(f"[Variables] Sources  : {SOURCES_PATH}")
print(f"[Variables] Pipeline : {PIPELINE_NAME} v{PIPELINE_VERSION}")
