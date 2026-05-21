# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# ============================================================================
# UNITY CATALOG - Estrutura de Catalog e Schemas
# ============================================================================

# Widget para receber o parâmetro 'catalog' do job (valor padrão: dev)
# Permite alternar entre ambientes: dev, staging, prod
dbutils.widgets.text("catalog", "dev")
CATALOG = dbutils.widgets.get("catalog")

# Schemas fixos dentro do catalog
BRONZE_SCHEMA = "bronze"  # Dados brutos ingeridos (AutoLoader + streaming)
SILVER_SCHEMA = "silver"  # Dados limpos e transformados
GOLD_SCHEMA   = "gold"    # Modelo dimensional (fatos + dimensões)

# Nomes completos (catalog.schema) para uso direto em queries
BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD   = f"{CATALOG}.{GOLD_SCHEMA}"

# COMMAND ----------

# DBTITLE 1,Paths - Sources e Landing
# ============================================================================
# PATHS - Arquivos e Landing Zone
# ============================================================================

# Volume UC para armazenamento de arquivos brutos
SOURCES_VOLUME = f"/Volumes/{CATALOG}/landing/storage_files/sources"

# SOURCES_PATH: Arquivos raw recém-carregados (CSV, JSON, XLSX, pipe-delimited)
# Usado APENAS pelo notebook Landing (00-LandingUploadSources)
SOURCES_PATH = SOURCES_VOLUME

# LANDING_PATH: Arquivos já convertidos para Parquet otimizado
# Organizado por sistema: /FileStore/case/landing/{sistema}/
# Usado pelos notebooks Bronze para leitura via AutoLoader
LANDING_PATH = "/FileStore/case/landing"

# COMMAND ----------

# ============================================================================
# AUTOLOADER - Checkpoints e Schema Inference
# ============================================================================

# Caminhos persistentes para metadados do AutoLoader (cloudFiles)
# Armazenados em UC Volume para garantir persistência entre execuções
# e entre tasks do job (evita /tmp/ que é efêmero)

CHECKPOINT_BASE = f"{SOURCES_VOLUME}/_checkpoints"        # Estado de progressão do streaming
SCHEMA_BASE     = f"{SOURCES_VOLUME}/_cloudfiles_schema"  # Schema inferido automaticamente

# COMMAND ----------

# ============================================================================
# MONITORAMENTO - Tabela de Controle do Pipeline
# ============================================================================

# Tabela para registrar execução de cada notebook/tabela
# Campos: tabela, inicio_execucao, fim_execucao, duracao_segundos, status, linhas_processadas, erro
CONTROL_TABLE = f"{CATALOG}.monitoring.pipeline_controller"

# COMMAND ----------

# ============================================================================
# METADADOS DO PIPELINE
# ============================================================================

# Identificação do pipeline
PIPELINE_NAME    = "case-data-engineering"
PIPELINE_VERSION = "1.0.0"
CREATED_BY       = "ronnan_ok@hotmail.com"

# Estratégias de carga disponíveis
STRATEGY_FULL  = "FULL"   # Carga completa: Bronze + Silver + Gold Dimensions
STRATEGY_DELTA = "DELTA"  # Carga incremental: Silver + Gold Facts (upserts)

# COMMAND ----------

# DBTITLE 1,Aliases e Log
# ============================================================================
# ALIASES - Compatibilidade com Notebooks de Referência
# ============================================================================

# Aliases legados com nomenclatura var_* (mantidos para compatibilidade)
var_environment   = CATALOG
var_bronze_schema = BRONZE_SCHEMA
var_silver_schema = SILVER_SCHEMA
var_gold_schema   = GOLD_SCHEMA
var_bronze        = BRONZE
var_silver        = SILVER
var_gold          = GOLD

print(f"✓ Variables carregadas | Catalog: {CATALOG} | Bronze/Silver/Gold configurados")
