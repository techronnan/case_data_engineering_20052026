# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 0-Init — Entry Point de Configuração
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Entry point único de configuração — carrega Libs, Variables e Functions em sequência |
# MAGIC | Como Usar | `%run ../0_config/0-Init` no início de cada notebook do pipeline |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Reestruturação: 0-Init passa a ser o entry point único substituindo 4-Config. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configurações do Spark

# COMMAND ----------

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

print(f"[Init] Spark {spark.version} | TZ: America/Sao_Paulo | AdaptiveQuery: ON")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregamento dos Módulos de Configuração

# COMMAND ----------

# MAGIC %run ./1-Libs

# COMMAND ----------

# MAGIC %run ./2-Variables

# COMMAND ----------

# MAGIC %run ./3-Functions

# COMMAND ----------

print("=" * 55)
print(f"  Config carregado | Pipeline: {PIPELINE_NAME} v{PIPELINE_VERSION}")
print(f"  Catalog : {CATALOG}")
print(f"  Bronze  : {BRONZE}  |  Silver: {SILVER}  |  Gold: {GOLD}")
print("=" * 55)
