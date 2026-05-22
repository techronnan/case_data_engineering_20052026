# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactItensPedido
# MAGIC
# MAGIC Granularidade: 1 linha por item de pedido.
# MAGIC Conecta a `fact_pedidos` via `order_key` e a `dim_produtos` via `product_key`.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'fact_itens_pedido'
tipo_carga           = 'delta'
chave_clusterby      = ['order_id', 'item_seq']
chave_upsert         = 'order_id'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_silver_schema}.erp_pedidos_itens').createOrReplaceTempView('v_si')
spark.table(f'{var_environment}.{var_gold_schema}.fact_pedidos').createOrReplaceTempView('v_fp')
spark.table(f'{var_environment}.{var_gold_schema}.dim_produtos').createOrReplaceTempView('v_prod')

fact = spark.sql("""
    SELECT
        row_number() OVER (ORDER BY si.order_id, si.item_seq) AS item_key,
        fp.order_key,
        prod.product_key,
        si.order_id,
        si.item_seq,
        si.quantity,
        si.unit_price,
        si.total_item,
        si.item_status,
        si.is_return,
        si.total_item_diverge,
        current_timestamp()                                    AS data_processamento
    FROM v_si si
    LEFT JOIN v_fp   fp   ON si.order_id     = fp.order_id
    LEFT JOIN v_prod prod ON si.product_code = prod.product_id AND prod.InRegistroAtivo = 1
""")


# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

fact.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(fact, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.order_id = source.order_id AND target.item_seq = source.item_seq
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
