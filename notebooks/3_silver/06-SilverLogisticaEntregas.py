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
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_flat = spark.sql("""
    SELECT
        upper(trim(delivery_id))         AS delivery_id,
        upper(trim(order_ref))           AS order_id,
        carrier.name                     AS carrier_name,
        carrier.mode                     AS carrier_mode,
        upper(trim(delivery_status))     AS delivery_status,
        coalesce(
            to_timestamp(`timestamps.shipped_at`,   "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(`timestamps.shipped_at`,   'yyyy-MM-dd HH:mm:ss'),
            to_timestamp(`timestamps.shipped_at`,   'dd/MM/yyyy HH:mm'),
            to_timestamp(`timestamps.shipped_at`,   'yyyy/MM/dd'),
            cast(to_date(`timestamps.shipped_at`,   'yyyy-MM-dd') as timestamp)
        )                                AS shipped_at,
        coalesce(
            to_timestamp(`timestamps.delivered_at`, "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(`timestamps.delivered_at`, 'yyyy-MM-dd HH:mm:ss'),
            to_timestamp(`timestamps.delivered_at`, 'dd/MM/yyyy HH:mm'),
            to_timestamp(`timestamps.delivered_at`, 'yyyy/MM/dd'),
            cast(to_date(`timestamps.delivered_at`, 'yyyy-MM-dd') as timestamp)
        )                                AS delivered_at,
        destination.state                AS dest_state,
        destination.city                 AS dest_city,
        cast(cost as double)             AS cost,
        rastreamento_source,
        datediff(
            coalesce(
                to_timestamp(`timestamps.delivered_at`, "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(`timestamps.delivered_at`, 'yyyy-MM-dd HH:mm:ss'),
                to_timestamp(`timestamps.delivered_at`, 'dd/MM/yyyy HH:mm'),
                to_timestamp(`timestamps.delivered_at`, 'yyyy/MM/dd'),
                cast(to_date(`timestamps.delivered_at`, 'yyyy-MM-dd') as timestamp)
            ),
            coalesce(
                to_timestamp(`timestamps.shipped_at`, "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(`timestamps.shipped_at`, 'yyyy-MM-dd HH:mm:ss'),
                to_timestamp(`timestamps.shipped_at`, 'dd/MM/yyyy HH:mm'),
                to_timestamp(`timestamps.shipped_at`, 'yyyy/MM/dd'),
                cast(to_date(`timestamps.shipped_at`, 'yyyy-MM-dd') as timestamp)
            )
        )                                AS delivery_days,
        datediff(
            coalesce(
                to_timestamp(`timestamps.delivered_at`, "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(`timestamps.delivered_at`, 'yyyy-MM-dd HH:mm:ss'),
                to_timestamp(`timestamps.delivered_at`, 'dd/MM/yyyy HH:mm'),
                to_timestamp(`timestamps.delivered_at`, 'yyyy/MM/dd'),
                cast(to_date(`timestamps.delivered_at`, 'yyyy-MM-dd') as timestamp)
            ),
            coalesce(
                to_timestamp(`timestamps.shipped_at`, "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(`timestamps.shipped_at`, 'yyyy-MM-dd HH:mm:ss'),
                to_timestamp(`timestamps.shipped_at`, 'dd/MM/yyyy HH:mm'),
                to_timestamp(`timestamps.shipped_at`, 'yyyy/MM/dd'),
                cast(to_date(`timestamps.shipped_at`, 'yyyy-MM-dd') as timestamp)
            )
        ) > 7                            AS is_late
    FROM v_source
""")

df_flat = normalize_uf_column(df_flat, "dest_state")

df_silver = (
    df_flat
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('delivery_id'), lit('NULL'))))
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
        ON target.dsRefChave = source.dsRefChave
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')

drop_v2checkpoint_feature(nome_gravacao_tabela)
