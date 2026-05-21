# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverErpPedidosItens
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.erp_pedidos_itens` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** `order_id`/`product_code` uppercase, `unit_price` decimal BR,
# MAGIC flag `is_return` para qty negativa, validação de `total_item`, status normalizado.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{BRONZE}.erp_pedidos_itens"
TGT = f"{SILVER}.erp_pedidos_itens"

# COMMAND ----------

df = spark.table(SRC)

df_silver = (
    df
    .withColumn("order_id",      upper(trim(col("order_id"))))
    .withColumn("product_code",  upper(trim(col("product_code"))))
    .withColumn("quantity",      col("quantity").cast(DoubleType()))
    .withColumn("unit_price",    normalize_decimal("unit_price"))
    .withColumn("total_item",    normalize_decimal("total_item"))
    .withColumn("item_status",   upper(trim(col("item_status"))))
    # Devoluções: qty negativa
    .withColumn("is_return",     col("quantity") < 0)
    # Validação: total_item esperado vs informado (tolerância 0.01)
    .withColumn("total_item_expected", col("quantity") * col("unit_price"))
    .withColumn("total_item_diverge",
        (abs(col("total_item") - col("total_item_expected")) > 0.01))
    .withColumn("_silver_at", current_timestamp())
)

print(f"Linhas        : {df_silver.count():,}")
print(f"Devoluções    : {df_silver.filter(col('is_return')).count():,}")
print(f"Divergências  : {df_silver.filter(col('total_item_diverge')).count():,}")

# COMMAND ----------

write_delta(df_silver, TGT)
