# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 4-Config
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Notebook master de configuração — carrega Init, Libs, Variables e Functions |
# MAGIC | Como Usar | Adicionar `%run ../0_config/4-Config` no início de cada notebook do pipeline |

# COMMAND ----------

# MAGIC %run ./0-Init

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
