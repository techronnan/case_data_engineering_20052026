# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverErpPedidosCabecalho
# MAGIC
# MAGIC **Tratamentos:** normalização de datas (3 formatos), status canônico, `order_id` uppercase,
# MAGIC valores decimais com vírgula, extração de `payment_details` JSON, flag de status indefinido.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'erp_pedidos_cabecalho'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_raw = spark.sql("""
    SELECT
        upper(trim(order_id))        AS order_id,
        upper(trim(customer_code))   AS customer_code,
        upper(trim(seller_id))       AS seller_id,
        NULL                         AS channel_id,
        NULL                         AS region_code,
        order_date,
        promised_date,
        CASE
            WHEN upper(status_order) IN ('FATURADO')                                    THEN 'FATURADO'
            WHEN upper(status_order) IN ('CANCELADO')                                   THEN 'CANCELADO'
            WHEN upper(status_order) IN ('ENTREGUE')                                    THEN 'ENTREGUE'
            WHEN upper(status_order) IN ('EM_SEPARACAO','EM SEPARACAO','EM_SEPARAÇÃO') THEN 'EM_SEPARACAO'
            ELSE 'INDEFINIDO'
        END                          AS status,
        cast(regexp_replace(gross_amount,    ',', '.') as double) AS gross_amount,
        cast(regexp_replace(discount_amount, ',', '.') as double) AS discount_amount,
        cast(regexp_replace(net_amount,      ',', '.') as double) AS net_amount,
        get_json_object(payment_details, '$.source')   AS payment_source,
        get_json_object(payment_details, '$.priority') AS payment_priority,
        (CASE
            WHEN upper(status_order) IN ('FATURADO')                                    THEN 'FATURADO'
            WHEN upper(status_order) IN ('CANCELADO')                                   THEN 'CANCELADO'
            WHEN upper(status_order) IN ('ENTREGUE')                                    THEN 'ENTREGUE'
            WHEN upper(status_order) IN ('EM_SEPARACAO','EM SEPARACAO','EM_SEPARAÇÃO') THEN 'EM_SEPARACAO'
            ELSE 'INDEFINIDO'
        END) != 'INDEFINIDO'         AS has_valid_status,
        rastreamento_source
    FROM v_source
""")

df_silver = (
    df_raw
    .withColumn("order_date", parse_date_multi_format("order_date"))
    .withColumn("due_date",   parse_date_multi_format("promised_date"))
    .drop("promised_date")
    .withColumn("dsRefChave", concat(lit('>>'), coalesce(col('order_id'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"Linhas saída   : {df_silver.count():,}")

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
    ''').display()

drop_v2checkpoint_feature(nome_gravacao_tabela)
