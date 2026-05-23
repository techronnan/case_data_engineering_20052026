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
        order_date,
        promised_date,
        upper(trim(status_order))    AS status_order,
        cast(regexp_replace(gross_amount,    ',', '.') as double) AS gross_amount,
        cast(regexp_replace(discount_amount, ',', '.') as double) AS discount_amount,
        cast(regexp_replace(net_amount,      ',', '.') as double) AS net_amount,
        payment_details,
        last_update
    FROM v_source
""")

df_silver = (
    df_raw
    .withColumn("order_date", parse_date_multi_format("order_date"))
    .withColumn("due_date",   parse_date_multi_format("promised_date"))
    .drop("promised_date")
    .withColumn("data_processamento", current_timestamp())
)

# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

df_silver.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df_silver, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.order_id = source.order_id
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
