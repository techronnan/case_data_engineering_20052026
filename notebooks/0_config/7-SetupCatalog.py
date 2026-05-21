# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 7-SetupCatalog
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Setup único de infraestrutura Unity Catalog — cria catalog, schemas e volume se não existirem |
# MAGIC | Executado Via | Primeira task do job (antes do landing) — não faz parte do 0-Init |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 21/05/2026 | Ronnan           | Criação: padronização de infraestrutura por ambiente (dev / prod). |

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
CATALOG = dbutils.widgets.get("catalog")

print(f"[Setup] Ambiente alvo : {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Catálogo

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
print(f"[Setup] Catalog '{CATALOG}' OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Schemas (Bronze / Silver / Gold / Default)

# COMMAND ----------

for schema in ["bronze", "silver", "gold", "default"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`")
    print(f"[Setup]   {CATALOG}.{schema} OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Volume de Fontes (workspace.default.sources)

# COMMAND ----------

# Volume compartilhado entre ambientes — fica no catalog workspace (neutro)
spark.sql("CREATE SCHEMA IF NOT EXISTS `workspace`.`default`")
spark.sql("CREATE VOLUME IF NOT EXISTS `workspace`.`default`.`sources`")
print("[Setup] Volume workspace.default.sources OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resumo

# COMMAND ----------

result = spark.sql(f"""
    SELECT table_schema AS schema, COUNT(*) AS tabelas
    FROM {CATALOG}.information_schema.tables
    GROUP BY table_schema
    ORDER BY table_schema
""")

print("=" * 50)
print(f"  Catalog  : {CATALOG}")
print(f"  Schemas  : bronze | silver | gold | default")
print(f"  Volume   : /Volumes/workspace/default/sources")
print("=" * 50)
result.show()
