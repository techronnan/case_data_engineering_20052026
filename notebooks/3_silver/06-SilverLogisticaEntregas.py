# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverLogisticaEntregas
# MAGIC
# MAGIC **Tratamentos:** flatten de structs aninhados (`carrier`, `timestamps`, `destination`),
# MAGIC normaliza UF (`dest_state`), calcula `delivery_days`, flag `is_late` (>7 dias).

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'logistica_entregas'
tipo_carga           = 'delta'
chave_clusterby      = ['delivery_id']
chave_upsert         = 'delivery_id'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_flat = spark.sql("""
    SELECT
        upper(trim(delivery_id))         AS delivery_id,
        upper(trim(order_ref))           AS order_id,
        carrier_name,
        carrier_mode,
        upper(trim(delivery_status))     AS delivery_status,
        shipped_at,
        delivered_at,
        destination_state                AS dest_state,
        destination_city                 AS dest_city,
        try_cast(cost as double)         AS cost,
        rastreamento_source
    FROM v_source
""")

df_flat = (
    df_flat
    .withColumn("shipped_at",    parse_timestamp_multi_format("shipped_at"))
    .withColumn("delivered_at",  parse_timestamp_multi_format("delivered_at"))
    .withColumn("delivery_days", datediff(col("delivered_at"), col("shipped_at")))
    .withColumn("is_late",       datediff(col("delivered_at"), col("shipped_at")) > 7)
)

df_flat = normalize_uf_column(df_flat, "dest_state")

df_silver = (
    df_flat
    .withColumn("data_processamento", current_timestamp())
)


# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

df_silver.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df_silver, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.delivery_id = source.delivery_id
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
