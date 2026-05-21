# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverErpPedidosCabecalho
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.erp_pedidos_cabecalho` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** normalização de datas (3 formatos), status canônico, `order_id` uppercase,
# MAGIC valores decimais com vírgula, extração de `payment_details` JSON, flag de status indefinido.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC   = f"{BRONZE}.erp_pedidos_cabecalho"
TGT   = f"{SILVER}.erp_pedidos_cabecalho"

# COMMAND ----------

df = spark.table(SRC)

df_silver = (
    df
    # Chaves normalizadas
    .withColumn("order_id",       upper(trim(col("order_id"))))
    .withColumn("customer_code",  upper(trim(col("customer_code"))))
    .withColumn("seller_id",      upper(trim(col("seller_id"))))
    .withColumn("channel_id",     upper(trim(col("channel_id"))))
    .withColumn("region_code",    upper(trim(col("region_code"))))
    # Datas normalizadas
    .withColumn("order_date",     parse_date_multi("order_date"))
    .withColumn("due_date",       parse_date_multi("due_date"))
    # Status canônico
    .withColumn("status",         normalize_status_pedido("status"))
    # Valores numéricos (vírgula → ponto)
    .withColumn("gross_amount",   normalize_decimal("gross_amount"))
    .withColumn("discount_amount",normalize_decimal("discount_amount"))
    .withColumn("net_amount",     normalize_decimal("net_amount"))
    # Extração do campo JSON payment_details
    .withColumn("payment_source",
        when(col("payment_details").isNotNull(),
             col("payment_details").getItem("source")).otherwise(lit(None)))
    .withColumn("payment_priority",
        when(col("payment_details").isNotNull(),
             col("payment_details").getItem("priority")).otherwise(lit(None)))
    # Flag de qualidade
    .withColumn("has_valid_status", col("status") != "INDEFINIDO")
    # Manter metadados de rastreabilidade
    .withColumn("_silver_at", current_timestamp())
    # Remover coluna JSON bruta (já extraída)
    .drop("payment_details")
)

print(f"Linhas entrada : {df.count():,}")
print(f"Linhas saída   : {df_silver.count():,}")

# COMMAND ----------

# Validação: pedidos sem order_id
nulls = df_silver.filter(col("order_id").isNull()).count()
print(f"order_id nulo  : {nulls}")

# Distribuição de status
df_silver.groupBy("status").count().orderBy("status").show()

# COMMAND ----------

write_delta(df_silver, TGT)
