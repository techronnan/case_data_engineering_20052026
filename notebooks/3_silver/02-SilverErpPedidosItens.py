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
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_silver = spark.sql("""
    SELECT
        upper(trim(order_id))                                        AS order_id,
        upper(trim(product_code))                                    AS product_code,
        cast(quantity as double)                                     AS quantity,
        cast(regexp_replace(unit_price, ',', '.') as double)         AS unit_price,
        cast(regexp_replace(total_item, ',', '.') as double)         AS total_item,
        upper(trim(item_status))                                     AS item_status,
        item_seq,
        rastreamento_source,
        cast(quantity as double) < 0                                 AS is_return,
        cast(quantity as double) * cast(regexp_replace(unit_price, ',', '.') as double)
                                                                     AS total_item_expected,
        abs(
            cast(regexp_replace(total_item, ',', '.') as double) -
            cast(quantity as double) * cast(regexp_replace(unit_price, ',', '.') as double)
        ) > 0.01                                                     AS total_item_diverge,
        concat('>>', coalesce(upper(trim(order_id)), 'NULL'),
               '>>', coalesce(cast(item_seq as string), 'NULL'))     AS dsRefChave,
        current_timestamp()                                          AS data_processamento
    FROM v_source
""")

print(f"Linhas        : {df_silver.count():,}")
print(f"Devoluções    : {df_silver.filter(col('is_return')).count():,}")
print(f"Divergências  : {df_silver.filter(col('total_item_diverge')).count():,}")

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
