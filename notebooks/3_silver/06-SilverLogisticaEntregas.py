# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverLogisticaEntregas
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.silver.logistica_entregas` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** flatten de structs aninhados (`carrier`, `timestamps`, `destination`),
# MAGIC normaliza UF (`dest_state`), calcula `delivery_days`, flag `is_late` (>7 dias).
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Padronização: dsRefChave, data_processamento, process_data_load/MERGE. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'logistica_entregas'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

df = spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}')

df_flat = (
    df
    .select(
        upper(trim(col("delivery_id"))).alias("delivery_id"),
        upper(trim(col("order_ref"))).alias("order_id"),
        col("carrier.name").alias("carrier_name"),
        col("carrier.mode").alias("carrier_mode"),
        upper(trim(col("delivery_status"))).alias("delivery_status"),
        parse_timestamp_multi_format("timestamps.shipped_at").alias("shipped_at"),
        parse_timestamp_multi_format("timestamps.delivered_at").alias("delivered_at"),
        col("destination.state").alias("dest_state"),
        col("destination.city").alias("dest_city"),
        col("cost").cast(DoubleType()).alias("cost"),
        col("rastreamento_source"),
    )
    .withColumn("delivery_days", datediff(col("delivered_at"), col("shipped_at")))
    .withColumn("is_late",
        when(col("delivery_days") > 7, lit(True)).otherwise(lit(False)))
)

df_flat = normalize_uf_column(df_flat, "dest_state")

df_silver = (
    df_flat
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('delivery_id'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"Linhas     : {df_silver.count():,}")
print(f"Atrasadas  : {df_silver.filter(col('is_late')).count():,}")

# COMMAND ----------

table_exists = spark.sql(f"""
    SELECT COUNT(*) FROM system.information_schema.tables
    WHERE table_catalog = '{nome_catalogo}'
      AND table_schema  = '{var_silver_schema}'
      AND table_name    = '{nome_tabela}'
""").collect()[0][0] > 0

df_silver.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df_silver, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.dsRefChave = source.dsRefChave
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''').display()

drop_v2checkpoint_feature(nome_gravacao_tabela)
