# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactItensPedido
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.fact_itens_pedido` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Granularidade: 1 linha por item de pedido.
# MAGIC Conecta a `fact_pedidos` via `order_key` e a `dim_produtos` via `product_key`.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

TGT = f"{GOLD}.fact_itens_pedido"

# COMMAND ----------

si      = spark.table(f"{SILVER}.erp_pedidos_itens")
f_ped   = spark.table(f"{GOLD}.fact_pedidos").select("order_key", "order_id")
d_prod  = spark.table(f"{GOLD}.dim_produtos").select("product_key", "product_id")

w = Window.orderBy("order_id", "item_seq")

fact = (
    si
    .join(f_ped,  si["order_id"]     == f_ped["order_id"],      "left")
    .join(d_prod, si["product_code"] == d_prod["product_id"],   "left")
    .withColumn("item_key", row_number().over(w))
    .select(
        col("item_key"),
        col("order_key"),
        col("product_key"),
        si["order_id"],
        col("item_seq"),
        col("quantity"),
        col("unit_price"),
        col("total_item"),
        col("item_status"),
        col("is_return"),
        col("total_item_diverge"),
    )
)

print(f"fact_itens_pedido: {fact.count():,} linhas")
print(f"Devoluções       : {fact.filter(col('is_return')).count():,}")

# COMMAND ----------

write_delta(fact, TGT)
