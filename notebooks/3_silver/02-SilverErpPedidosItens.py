# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverErpPedidosItens
# MAGIC
# MAGIC **Tratamentos:** `order_id`/`product_code` uppercase, `unit_price` decimal BR,
# MAGIC flag `is_return` para qty negativa, validação de `total_item`, status normalizado.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'erp_pedidos_itens'
tipo_carga           = 'delta'
chave_clusterby      = ['order_id', 'item_seq']
chave_upsert         = 'order_id'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_silver = spark.sql("""
    SELECT
        upper(trim(order_id))                                        AS order_id,
        item_seq,
        upper(trim(product_code))                                    AS product_code,
        cast(quantity as double)                                     AS quantity,
        cast(regexp_replace(unit_price, ',', '.') as double)         AS unit_price,
        cast(regexp_replace(total_item, ',', '.') as double)         AS total_item,
        upper(trim(item_status))                                     AS item_status,
        current_timestamp()                                          AS data_processamento
    FROM v_source
""")

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
        ON target.order_id = source.order_id AND target.item_seq = source.item_seq
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
