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
# MAGIC | Finalidade | Entry point único de configuração — carrega Libs, Variables, Functions e MonitoringLogs em sequência |
# MAGIC | Como Usar | `%run ../0_config/0-Init` no início de cada notebook do pipeline |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Reestruturação: 0-Init passa a ser o entry point único substituindo 4-Config. |
# MAGIC | 21/05/2026 | Ronnan           | Carrega 6-MonitoringLogs (cria pipeline_controller + funções de monitoramento). |

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

print(f"[Init] Spark {spark.version} | TZ: America/Sao_Paulo")

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

print("=" * 60)
print(f"  Config carregado | Pipeline: {PIPELINE_NAME} v{PIPELINE_VERSION}")
print(f"  Catalog  : {CATALOG}")
print(f"  Bronze   : {BRONZE}  |  Silver: {SILVER}  |  Gold: {GOLD}")
print(f"  Monitor  : {CONTROL_TABLE}")
print(f"  Checkpts : {CHECKPOINT_BASE}")
print("=" * 60)
