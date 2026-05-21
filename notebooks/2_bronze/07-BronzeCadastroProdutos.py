# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeCadastroProdutos
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.bronze.cadastro_produtos` |
# MAGIC | Origem Fonte de Dados de Entrada | `sources/cadastro_produtos_api_dump.json` (JSON Array aninhado) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SOURCE_FILE  = f"{SOURCES_PATH}/cadastro_produtos_api_dump.json"
TARGET_TABLE = f"{BRONZE}.cadastro_produtos"

# COMMAND ----------

df = (
    spark.read
    .option("multiLine", "true")
    .json(SOURCE_FILE)
)

df = add_ingestion_metadata(df, SOURCE_FILE)

print(f"Linhas lidas : {df.count():,}")
print("Schema (produto, pricing, attributes aninhados):")
df.printSchema()

# COMMAND ----------

write_delta(df, TARGET_TABLE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total
# MAGIC FROM workspace.bronze.cadastro_produtos
