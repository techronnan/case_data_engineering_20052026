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

# Arquivos raw lidos direto do Workspace Repos — sem necessidade de upload para Volume
_current_user = spark.sql("SELECT current_user()").first()[0]
SOURCES_PATH  = f"/Workspace/Repos/{_current_user}/case_data_engineering_20052026/sources"

# Parquet puro gerado pela camada Landing; lido pelos notebooks Bronze via AutoLoader
# Estrutura: systems/{sistema}/{ano}/{mes}/{file_name_YYYYMMDDHHMMSS}.parquet
LANDING_PATH = f"/Volumes/{CATALOG}/landing/storage_files/systems"

# COMMAND ----------

# Checkpoints e schema em Volume persistente — /tmp/ é efêmero por task em jobs serverless
_storage_volume = f"/Volumes/{CATALOG}/landing/storage_files"
CHECKPOINT_BASE = f"{_storage_volume}/_checkpoints"
SCHEMA_BASE     = f"{_storage_volume}/_cloudfiles_schema"

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

# COMMAND ----------

# DBTITLE 1,Landing Sources - Mapeamento de Arquivos
# Mapeamento: sistema → (nome, arquivo, formato, opções de leitura)
SOURCE_MAP = {
    "erp_cabecalho": {
        "name": "erp_pedidos_cabecalho",
        "file": "erp_pedidos_cabecalho_2025.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ";"}
    },
    "erp_itens": {
        "name": "erp_pedidos_itens",
        "file": "erp_pedidos_itens_2025.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ","}
    },
    "legado": {
        "name": "legado_regioes",
        "file": "legado_regioes_pipe.txt",
        "format": "txt",
        "options": {"header": True, "inferSchema": True, "sep": "|"}
    },
    "vendedores": {
        "name": "vendedores",
        "file": "vendedores.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ";"}
    },
    "atendimento": {
        "name": "atendimento_ocorrencias",
        "file": "atendimento_ocorrencias.ndjson",
        "format": "ndjson",
        "options": {}
    },
    "logistica": {
        "name": "logistica_entregas",
        "file": "logistica_entregas.json",
        "format": "json",
        "options": {"multiLine": True}
    },
    "produtos": {
        "name": "cadastro_produtos",
        "file": "cadastro_produtos_api_dump.json",
        "format": "json",
        "options": {"multiLine": True}
    },
    "crm": {
        "name": "crm_clientes",
        "file": "crm_clientes_export.xlsx",
        "format": "xlsx",
        "options": {}
    },
    "canais": {
        "name": "comercial_canais",
        "file": "comercial_canais.xlsx",
        "format": "xlsx",
        "options": {}
    },
}

EXPECTED_FILES = [
    "erp_pedidos_cabecalho_2025.csv",
    "erp_pedidos_itens_2025.csv",
    "legado_regioes_pipe.txt",
    "vendedores.csv",
    "atendimento_ocorrencias.ndjson",
    "logistica_entregas.json",
    "cadastro_produtos_api_dump.json",
    "crm_clientes_export.xlsx",
    "comercial_canais.xlsx",
]

print(f"✓ SOURCE_MAP e EXPECTED_FILES carregados | {len(EXPECTED_FILES)} arquivos esperados")
