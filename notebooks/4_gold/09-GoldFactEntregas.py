# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactEntregas
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.fact_entregas` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Granularidade: 1 linha por entrega. Conecta a `fact_pedidos` via `order_key`.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

TGT = f"{GOLD}.fact_entregas"

# COMMAND ----------

se    = spark.table(f"{SILVER}.logistica_entregas")
f_ped = spark.table(f"{GOLD}.fact_pedidos").select("order_key", "order_id")
d_tmp = spark.table(f"{GOLD}.dim_tempo").select("date_key", "date")

w = Window.orderBy("delivery_id")

fact = (
    se
    .join(f_ped, se["order_id"] == f_ped["order_id"], "left")
    .join(d_tmp.alias("d_ship"), se["shipped_at"].cast("date")   == col("d_ship.date"), "left")
    .join(d_tmp.alias("d_del"),  se["delivered_at"].cast("date") == col("d_del.date"),  "left")
    .withColumn("delivery_key", row_number().over(w))
    .select(
        col("delivery_key"),
        col("order_key"),
        se["delivery_id"],
        col("d_ship.date_key").alias("shipped_date_key"),
        col("d_del.date_key").alias("delivered_date_key"),
        col("carrier_name"),
        col("carrier_mode"),
        col("delivery_status"),
        col("dest_state"),
        col("dest_city"),
        col("cost"),
        col("delivery_days"),
        col("is_late"),
    )
)

print(f"fact_entregas: {fact.count():,} linhas")
print(f"Atrasadas    : {fact.filter(col('is_late')).count():,}")

# COMMAND ----------

write_delta(fact, TGT)
