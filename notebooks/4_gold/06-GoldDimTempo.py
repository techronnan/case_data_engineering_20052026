# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimTempo
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `workspace.gold.dim_tempo` |
# MAGIC | Origem Fonte de Dados de Entrada | Gerada sinteticamente (sequência de datas) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de tempo cobrindo 2024-01-01 a 2027-12-31.
# MAGIC `date_key` no formato YYYYMMDD (inteiro) para joins eficientes.

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

TGT = f"{GOLD}.dim_tempo"

# COMMAND ----------

from pyspark.sql.functions import (
    date_format, year, quarter, month, weekofyear,
    dayofweek, dayofmonth, date_add
)

dim_tempo = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'),
        to_date('2027-12-31'),
        interval 1 day
    )) AS date
""").select(
    date_format("date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    col("date"),
    year("date").alias("year"),
    quarter("date").alias("quarter"),
    month("date").alias("month"),
    date_format("date", "MMMM").alias("month_name"),
    date_format("date", "MMM").alias("month_abbr"),
    weekofyear("date").alias("week_of_year"),
    dayofmonth("date").alias("day_of_month"),
    dayofweek("date").alias("day_of_week"),
    date_format("date", "EEEE").alias("day_name"),
    when(dayofweek("date").isin(1, 7), lit(True)).otherwise(lit(False)).alias("is_weekend"),
)

print(f"dim_tempo: {dim_tempo.count():,} dias gerados")
dim_tempo.show(5)

# COMMAND ----------

write_delta(dim_tempo, TGT)
