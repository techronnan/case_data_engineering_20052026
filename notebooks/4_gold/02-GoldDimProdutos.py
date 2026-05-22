# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimProdutos
# MAGIC
# MAGIC Dimensão de produtos com surrogate key. Granularidade: 1 linha por produto ativo.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'dim_produtos'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_silver_schema}.cadastro_produtos').createOrReplaceTempView('v_source')

df_dim = spark.sql("""
    SELECT
        row_number() OVER (ORDER BY product_code) AS product_key,
        product_code                               AS product_id,
        product_name,
        category,
        subcategory,
        family,
        list_price,
        currency,
        tags,
        status,
        1                                          AS InRegistroAtivo,
        concat('>>', coalesce(product_code, 'NULL')) AS dsRefChave,
        current_timestamp()                        AS data_processamento
    FROM v_source
    WHERE status = 'ATIVO'
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