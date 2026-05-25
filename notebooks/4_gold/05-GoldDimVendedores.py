# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimVendedores
# MAGIC
# MAGIC Dimensão de vendedores. Surrogate keys de região e canal gerados inline a partir do Silver.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'dim_vendedores'
tipo_carga           = 'full'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_silver_schema}.vendedores').createOrReplaceTempView('v_vend')
spark.table(f'{var_environment}.{var_silver_schema}.legado_regioes').createOrReplaceTempView('v_reg_s')
spark.table(f'{var_environment}.{var_silver_schema}.comercial_canais').createOrReplaceTempView('v_can_s')

df_dim = spark.sql("""
    WITH reg_keys AS (
        SELECT regional_code, row_number() OVER (ORDER BY regional_code) AS region_key
        FROM v_reg_s
    ),
    can_keys AS (
        SELECT channel_id, row_number() OVER (ORDER BY channel_id) AS channel_key
        FROM v_can_s
    )
    SELECT
        row_number() OVER (ORDER BY v.seller_id) AS seller_key,
        v.seller_id,
        v.seller_name,
        rk.region_key,
        ck.channel_key,
        v.hire_date,
        v.status,
        1                                         AS InRegistroAtivo,
        current_timestamp()                       AS data_processamento
    FROM v_vend v
    LEFT JOIN reg_keys rk ON v.regional_code = rk.regional_code
    LEFT JOIN can_keys ck ON v.canal_id      = ck.channel_id
""")


# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

df_dim.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df_dim, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.seller_id = source.seller_id
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')