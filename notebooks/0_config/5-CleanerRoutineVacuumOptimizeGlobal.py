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

# COMMAND ----------

# MAGIC %run ./4-Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listagem de Tabelas por Camada

# COMMAND ----------

BRONZE_TABLES = [
    f"{BRONZE}.erp_pedidos_cabecalho",
    f"{BRONZE}.erp_pedidos_itens",
    f"{BRONZE}.legado_regioes",
    f"{BRONZE}.vendedores",
    f"{BRONZE}.atendimento_ocorrencias",
    f"{BRONZE}.logistica_entregas",
    f"{BRONZE}.cadastro_produtos",
    f"{BRONZE}.crm_clientes",
    f"{BRONZE}.comercial_canais",
]

SILVER_TABLES = [
    f"{SILVER}.erp_pedidos_cabecalho",
    f"{SILVER}.erp_pedidos_itens",
    f"{SILVER}.legado_regioes",
    f"{SILVER}.vendedores",
    f"{SILVER}.atendimento_ocorrencias",
    f"{SILVER}.logistica_entregas",
    f"{SILVER}.cadastro_produtos",
    f"{SILVER}.crm_clientes",
    f"{SILVER}.comercial_canais",
]

GOLD_TABLES = [
    f"{GOLD}.dim_clientes",
    f"{GOLD}.dim_produtos",
    f"{GOLD}.dim_regioes",
    f"{GOLD}.dim_canais",
    f"{GOLD}.dim_vendedores",
    f"{GOLD}.dim_tempo",
    f"{GOLD}.fact_pedidos",
    f"{GOLD}.fact_itens_pedido",
    f"{GOLD}.fact_entregas",
    f"{GOLD}.fact_ocorrencias",
]

ALL_TABLES = BRONZE_TABLES + SILVER_TABLES + GOLD_TABLES
print(f"Total de tabelas gerenciadas: {len(ALL_TABLES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## VACUUM — Remove arquivos Delta obsoletos (retém 7 dias)

# COMMAND ----------

VACUUM_RETAIN_HOURS = 168  # 7 dias

for table in ALL_TABLES:
    try:
        spark.sql(f"VACUUM {table} RETAIN {VACUUM_RETAIN_HOURS} HOURS")
        print(f"[VACUUM] OK  {table}")
    except Exception as e:
        print(f"[VACUUM] SKIP {table} — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## OPTIMIZE — Compacta arquivos pequenos (Z-Order em tabelas de fato)

# COMMAND ----------

ZORDER_CONFIG = {
    f"{GOLD}.fact_pedidos":      "order_date_key",
    f"{GOLD}.fact_itens_pedido": "order_key",
    f"{GOLD}.fact_entregas":     "order_key",
    f"{GOLD}.fact_ocorrencias":  "order_key",
}

for table in ALL_TABLES:
    try:
        if table in ZORDER_CONFIG:
            z_col = ZORDER_CONFIG[table]
            spark.sql(f"OPTIMIZE {table} ZORDER BY ({z_col})")
            print(f"[OPTIMIZE] OK  {table} (ZORDER BY {z_col})")
        else:
            spark.sql(f"OPTIMIZE {table}")
            print(f"[OPTIMIZE] OK  {table}")
    except Exception as e:
        print(f"[OPTIMIZE] SKIP {table} — {e}")
