# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactEntregas
# MAGIC
# MAGIC Granularidade: 1 linha por entrega.
# MAGIC Conecta a `fact_pedidos` via `order_key` e a `dim_tempo` para datas de envio/entrega.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'fact_entregas'
tipo_carga           = 'delta'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_silver_schema}.logistica_entregas').createOrReplaceTempView('v_se')
spark.table(f'{var_environment}.{var_gold_schema}.fact_pedidos').createOrReplaceTempView('v_fp')
spark.table(f'{var_environment}.{var_gold_schema}.dim_tempo').createOrReplaceTempView('v_tmp')

fact = spark.sql("""
    SELECT
        row_number() OVER (ORDER BY se.delivery_id) AS delivery_key,
        fp.order_key,
        se.delivery_id,
        d_ship.date_key                             AS shipped_date_key,
        d_del.date_key                              AS delivered_date_key,
        se.carrier_name,
        se.carrier_mode,
        se.delivery_status,
        se.dest_state,
        se.dest_city,
        se.cost,
        se.delivery_days,
        se.is_late,
        current_timestamp()                         AS data_processamento
    FROM v_se se
    LEFT JOIN v_fp       fp     ON se.order_id                   = fp.order_id
    LEFT JOIN v_tmp      d_ship ON cast(se.shipped_at AS date)   = d_ship.date AND d_ship.InRegistroAtivo = 1
    LEFT JOIN v_tmp      d_del  ON cast(se.delivered_at AS date) = d_del.date  AND d_del.InRegistroAtivo  = 1
""")


# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

fact.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(fact, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.delivery_id = source.delivery_id
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
