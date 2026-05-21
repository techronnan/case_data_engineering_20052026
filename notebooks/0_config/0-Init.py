# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 0-Init
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Configurações iniciais do Spark e Delta Lake |
# MAGIC | Executado Via | `4-Config` — não executar diretamente |

# COMMAND ----------

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

print(f"[Init] Spark {spark.version} | TZ: America/Sao_Paulo | AdaptiveQuery: ON")
