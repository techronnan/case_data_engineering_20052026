# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimVendedores
# MAGIC
# MAGIC Dimensão de vendedores. Join com `dim_regioes` e `dim_canais` para resolver FKs.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'dim_vendedores'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_silver_schema}.vendedores').createOrReplaceTempView('v_vend')
spark.table(f'{var_environment}.{var_gold_schema}.dim_regioes').createOrReplaceTempView('v_reg')
spark.table(f'{var_environment}.{var_gold_schema}.dim_canais').createOrReplaceTempView('v_can')

df_dim = spark.sql("""
    SELECT
        row_number() OVER (ORDER BY v.seller_id) AS seller_key,
        v.seller_id,
        v.seller_name,
        r.region_key,
        c.channel_key,
        v.hire_date,
        v.status,
        1                                         AS InRegistroAtivo,
        concat('>>', coalesce(v.seller_id, 'NULL')) AS dsRefChave,
        current_timestamp()                       AS data_processamento
    FROM v_vend v
    LEFT JOIN v_reg r ON v.regional_code = r.regional_code AND r.InRegistroAtivo = 1
    LEFT JOIN v_can c ON v.canal_id      = c.channel_id    AND c.InRegistroAtivo = 1
""")


# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

df_dim.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df_dim, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.dsRefChave = source.dsRefChave
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')