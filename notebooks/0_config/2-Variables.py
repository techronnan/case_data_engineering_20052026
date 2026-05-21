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
# MAGIC | Executado Via | `0-Init` — não executar diretamente |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação inicial. |
# MAGIC | 21/05/2026 | Ronnan           | Checkpoints e schema locations migrados de /tmp/ para DBFS (compatibilidade serverless). Adicionado CONTROL_TABLE para monitoramento. |

# COMMAND ----------

# Unity Catalog — catálogo dinâmico por ambiente (job parameter → widget)
dbutils.widgets.text("catalog", "workspace")
CATALOG       = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD   = f"{CATALOG}.{GOLD_SCHEMA}"

# COMMAND ----------

# UC Volume — workspace.default.sources (DBFS root desabilitado neste workspace)
SOURCES_VOLUME  = f"/Volumes/{CATALOG}/landing/storage_files/sources"

SOURCES_PATH    = SOURCES_VOLUME

# COMMAND ----------

# AutoLoader — subdiretórios dentro do mesmo volume (persistentes entre tasks)
CHECKPOINT_BASE = f"{SOURCES_VOLUME}/_checkpoints"
SCHEMA_BASE     = f"{SOURCES_VOLUME}/_cloudfiles_schema"

# COMMAND ----------

# Tabela controladora de execução do pipeline
CONTROL_TABLE = f"{CATALOG}.monitoring.pipeline_controller"

# COMMAND ----------

# Identificação do pipeline
PIPELINE_NAME    = "case-data-engineering"
PIPELINE_VERSION = "1.0.0"
CREATED_BY       = "ronnan_ok@hotmail.com"

# Estratégias disponíveis
STRATEGY_FULL  = "FULL"   # Bronze + Gold Dims
STRATEGY_DELTA = "DELTA"  # Silver + Gold Facts

# COMMAND ----------

# Aliases padrão var_environment (compatibilidade com notebooks de referência)
var_environment   = CATALOG
var_bronze_schema = BRONZE_SCHEMA
var_silver_schema = SILVER_SCHEMA
var_gold_schema   = GOLD_SCHEMA
var_bronze        = BRONZE
var_silver        = SILVER
var_gold          = GOLD

print(f"[Variables] Catalog      : {CATALOG}")
print(f"[Variables] Bronze       : {BRONZE}")
print(f"[Variables] Silver       : {SILVER}")
print(f"[Variables] Gold         : {GOLD}")
print(f"[Variables] Sources      : {SOURCES_PATH}")
print(f"[Variables] Checkpoints  : {CHECKPOINT_BASE}")
print(f"[Variables] Schema loc.  : {SCHEMA_BASE}")
print(f"[Variables] Monitor      : {CONTROL_TABLE}")
print(f"[Variables] Pipeline     : {PIPELINE_NAME} v{PIPELINE_VERSION}")
