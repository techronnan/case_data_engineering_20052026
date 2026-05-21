# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactPedidos
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.fact_pedidos` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Fato central do Star Schema. Granularidade: 1 linha por pedido.
# MAGIC Join com todas as dimensões para resolver surrogate keys.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

TGT = f"{GOLD}.fact_pedidos"

# COMMAND ----------

# Carregar Silver e dimensões
sp     = spark.table(f"{SILVER}.erp_pedidos_cabecalho")
d_cli  = spark.table(f"{GOLD}.dim_clientes").select("customer_key", "customer_id")
d_vend = spark.table(f"{GOLD}.dim_vendedores").select("seller_key", "seller_id")
d_can  = spark.table(f"{GOLD}.dim_canais").select("channel_key", "channel_id")
d_reg  = spark.table(f"{GOLD}.dim_regioes").select("region_key", "regional_code")
d_tmp  = spark.table(f"{GOLD}.dim_tempo").select("date_key", "date")

# COMMAND ----------

w = Window.orderBy("order_id")

fact = (
    sp
    .join(d_cli,  sp["customer_code"] == d_cli["customer_id"],      "left")
    .join(d_vend, sp["seller_id"]      == d_vend["seller_id"],       "left")
    .join(d_can,  sp["channel_id"]     == d_can["channel_id"],       "left")
    .join(d_reg,  sp["region_code"]    == d_reg["regional_code"],    "left")
    .join(d_tmp,  sp["order_date"]     == d_tmp["date"],             "left")
    .withColumn("order_key", row_number().over(w))
    .select(
        col("order_key"),
        sp["order_id"],
        col("date_key").alias("order_date_key"),
        col("customer_key"),
        col("seller_key"),
        col("channel_key"),
        col("region_key"),
        sp["status"],
        col("gross_amount"),
        col("discount_amount"),
        col("net_amount"),
        col("payment_source"),
        col("payment_priority"),
        sp["due_date"],
    )
)

print(f"fact_pedidos: {fact.count():,} linhas")
print(f"Sem cliente  : {fact.filter(col('customer_key').isNull()).count():,}")
print(f"Sem vendedor : {fact.filter(col('seller_key').isNull()).count():,}")
print(f"Sem data     : {fact.filter(col('order_date_key').isNull()).count():,}")

# COMMAND ----------

write_delta(fact, TGT)
