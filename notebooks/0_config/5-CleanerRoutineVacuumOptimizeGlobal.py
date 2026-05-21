# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 5-CleanerRoutineVacuumOptimizeGlobal
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Manutenção global das tabelas Delta: VACUUM + OPTIMIZE |
# MAGIC | Frequência Recomendada | Semanal ou após grandes cargas |
# MAGIC | Execução | Manual ou job dedicado — não faz parte do pipeline principal |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação inicial com listas hardcoded. |
# MAGIC | 21/05/2026 | Ronnan           | Refatorado para descoberta dinâmica via information_schema. |

# COMMAND ----------

# MAGIC %run ./0-Init

# COMMAND ----------

# MAGIC %md
# MAGIC ### Descoberta dinâmica de tabelas (bronze / silver / gold)

# COMMAND ----------

all_tables_df = spark.sql(f"""
    SELECT concat(table_catalog, '.', table_schema, '.', table_name) AS full_name,
           table_schema AS camada
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold')
      AND table_type = 'MANAGED'
    ORDER BY table_schema, table_name
""")

ALL_TABLES = [row.full_name for row in all_tables_df.collect()]
print(f"Tabelas encontradas no catálogo '{CATALOG}': {len(ALL_TABLES)}")
for t in ALL_TABLES:
    print(f"  {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### VACUUM — remove arquivos Delta obsoletos (retém 7 dias)

# COMMAND ----------

VACUUM_RETAIN_HOURS = 168  # 7 dias

for table in ALL_TABLES:
    try:
        spark.sql(f"VACUUM {table} RETAIN {VACUUM_RETAIN_HOURS} HOURS")
        print(f"[VACUUM] OK   {table}")
    except Exception as e:
        print(f"[VACUUM] SKIP {table} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE — compacta arquivos pequenos (Z-Order nas fatos)

# COMMAND ----------

ZORDER_CONFIG = {
    "fact_pedidos":      "order_date_key",
    "fact_itens_pedido": "order_key",
    "fact_entregas":     "order_key",
    "fact_ocorrencias":  "order_key",
}

for table in ALL_TABLES:
    table_name = table.split(".")[-1]
    try:
        if table_name in ZORDER_CONFIG:
            z_col = ZORDER_CONFIG[table_name]
            spark.sql(f"OPTIMIZE {table} ZORDER BY ({z_col})")
            print(f"[OPTIMIZE] OK  {table} (ZORDER BY {z_col})")
        else:
            spark.sql(f"OPTIMIZE {table}")
            print(f"[OPTIMIZE] OK  {table}")
    except Exception as e:
        print(f"[OPTIMIZE] SKIP {table} — {e}")
