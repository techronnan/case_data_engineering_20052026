# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverAtendimentoOcorrencias
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.silver.atendimento_ocorrencias` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** timestamps multi-formato, normaliza status/severity/event_type,
# MAGIC `order_id` uppercase, flags de campos nulos.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

SRC = f"{BRONZE}.atendimento_ocorrencias"
TGT = f"{SILVER}.atendimento_ocorrencias"

# COMMAND ----------

df = spark.table(SRC)

df_silver = (
    df
    .withColumn("ticket_id",   upper(trim(col("ticket_id"))))
    .withColumn("order_id",    upper(trim(col("order_id"))))
    .withColumn("status",      upper(trim(col("status"))))
    .withColumn("severity",    upper(trim(col("severity"))))
    .withColumn("event_type",  lower(trim(col("event_type"))))
    .withColumn("created_at",  parse_timestamp_multi("created_at"))
    .withColumn("updated_at",  parse_timestamp_multi("updated_at"))
    # Flags de qualidade
    .withColumn("has_event_type", col("event_type").isNotNull())
    .withColumn("has_severity",   col("severity").isNotNull())
    .withColumn("has_order_ref",  col("order_id").isNotNull())
    .withColumn("_silver_at", current_timestamp())
)

print(f"Linhas : {df_silver.count():,}")
df_silver.groupBy("status").count().show()
df_silver.groupBy("event_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------

write_delta(df_silver, TGT)
