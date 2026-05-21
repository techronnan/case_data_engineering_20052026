# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeErpPedidosCabecalho
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.bronze.erp_pedidos_cabecalho` |
# MAGIC | Origem Fonte de Dados de Entrada | `sources/erp_pedidos_cabecalho_2025.csv` |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SOURCE_FILE   = f"{SOURCES_PATH}/erp_pedidos_cabecalho_2025.csv"
TARGET_TABLE  = f"{BRONZE}.erp_pedidos_cabecalho"

# COMMAND ----------

df = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("inferSchema", "false")
    .option("encoding", "UTF-8")
    .csv(SOURCE_FILE)
)

df = add_ingestion_metadata(df, SOURCE_FILE)

print(f"Linhas lidas  : {df.count():,}")
print(f"Colunas       : {len(df.columns)}")
df.printSchema()

# COMMAND ----------

write_delta(df, TARGET_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                               AS total_linhas,
# MAGIC   COUNT(DISTINCT order_id)               AS pedidos_distintos,
# MAGIC   MIN(_ingested_at)                      AS primeira_ingestao,
# MAGIC   MAX(_ingested_at)                      AS ultima_ingestao
# MAGIC FROM workspace.bronze.erp_pedidos_cabecalho
