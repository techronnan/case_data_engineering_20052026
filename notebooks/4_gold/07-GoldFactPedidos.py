# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactPedidos
# MAGIC
# MAGIC Fato central do Star Schema. Granularidade: 1 linha por pedido.
# MAGIC Join com todas as dimensões para resolver surrogate keys.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'fact_pedidos'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_silver_schema}.erp_pedidos_cabecalho').createOrReplaceTempView('v_sp')
spark.table(f'{var_environment}.{var_gold_schema}.dim_clientes').createOrReplaceTempView('v_cli')
spark.table(f'{var_environment}.{var_gold_schema}.dim_vendedores').createOrReplaceTempView('v_vend')
spark.table(f'{var_environment}.{var_gold_schema}.dim_canais').createOrReplaceTempView('v_can')
spark.table(f'{var_environment}.{var_gold_schema}.dim_regioes').createOrReplaceTempView('v_reg')
spark.table(f'{var_environment}.{var_gold_schema}.dim_tempo').createOrReplaceTempView('v_tmp')

fact = spark.sql("""
    SELECT
        row_number() OVER (ORDER BY sp.order_id)    AS order_key,
        sp.order_id,
        t.date_key                                  AS order_date_key,
        cli.customer_key,
        vend.seller_key,
        can.channel_key,
        reg.region_key,
        sp.status,
        sp.gross_amount,
        sp.discount_amount,
        sp.net_amount,
        sp.payment_source,
        sp.payment_priority,
        sp.due_date,
        concat('>>', coalesce(sp.order_id, 'NULL')) AS dsRefChave,
        current_timestamp()                         AS data_processamento
    FROM v_sp sp
    LEFT JOIN v_cli  cli  ON sp.customer_code = cli.customer_id   AND cli.InRegistroAtivo  = 1
    LEFT JOIN v_vend vend ON sp.seller_id     = vend.seller_id    AND vend.InRegistroAtivo = 1
    LEFT JOIN v_can  can  ON sp.channel_id    = can.channel_id    AND can.InRegistroAtivo  = 1
    LEFT JOIN v_reg  reg  ON sp.region_code   = reg.regional_code AND reg.InRegistroAtivo  = 1
    LEFT JOIN v_tmp  t    ON sp.order_date    = t.date            AND t.InRegistroAtivo    = 1
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
        ON target.dsRefChave = source.dsRefChave
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')

drop_v2checkpoint_feature(nome_gravacao_tabela)
