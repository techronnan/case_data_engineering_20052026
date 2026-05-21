# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactOcorrencias
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.fact_ocorrencias` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Granularidade: 1 linha por ticket de atendimento.
# MAGIC Conecta a `fact_pedidos` via `order_key` e a `dim_tempo` via `date_key`.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

TGT = f"{GOLD}.fact_ocorrencias"

# COMMAND ----------

so    = spark.table(f"{SILVER}.atendimento_ocorrencias")
f_ped = spark.table(f"{GOLD}.fact_pedidos").select("order_key", "order_id")
d_tmp = spark.table(f"{GOLD}.dim_tempo").select("date_key", "date")

w = Window.orderBy("ticket_id")

fact = (
    so
    .join(f_ped, so["order_id"] == f_ped["order_id"],                  "left")
    .join(d_tmp, so["created_at"].cast("date") == d_tmp["date"],       "left")
    .withColumn("ticket_key", row_number().over(w))
    .select(
        col("ticket_key"),
        col("order_key"),
        col("date_key").alias("created_date_key"),
        so["ticket_id"],
        so["order_id"],
        col("event_type"),
        col("severity"),
        col("status"),
        col("has_event_type"),
        col("has_severity"),
        col("created_at"),
        col("updated_at"),
    )
)

print(f"fact_ocorrencias  : {fact.count():,} linhas")
print(f"Sem order_key     : {fact.filter(col('order_key').isNull()).count():,}")

# COMMAND ----------

write_delta(fact, TGT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação Final do Star Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'fact_pedidos'      AS tabela, COUNT(*) AS linhas FROM workspace.gold.fact_pedidos
# MAGIC UNION ALL
# MAGIC SELECT 'fact_itens_pedido', COUNT(*) FROM workspace.gold.fact_itens_pedido
# MAGIC UNION ALL
# MAGIC SELECT 'fact_entregas',     COUNT(*) FROM workspace.gold.fact_entregas
# MAGIC UNION ALL
# MAGIC SELECT 'fact_ocorrencias',  COUNT(*) FROM workspace.gold.fact_ocorrencias
# MAGIC UNION ALL
# MAGIC SELECT 'dim_clientes',      COUNT(*) FROM workspace.gold.dim_clientes
# MAGIC UNION ALL
# MAGIC SELECT 'dim_produtos',      COUNT(*) FROM workspace.gold.dim_produtos
# MAGIC UNION ALL
# MAGIC SELECT 'dim_regioes',       COUNT(*) FROM workspace.gold.dim_regioes
# MAGIC UNION ALL
# MAGIC SELECT 'dim_canais',        COUNT(*) FROM workspace.gold.dim_canais
# MAGIC UNION ALL
# MAGIC SELECT 'dim_vendedores',    COUNT(*) FROM workspace.gold.dim_vendedores
# MAGIC UNION ALL
# MAGIC SELECT 'dim_tempo',         COUNT(*) FROM workspace.gold.dim_tempo
# MAGIC ORDER BY tabela
