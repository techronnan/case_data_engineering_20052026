# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverCadastroProdutos
# MAGIC
# MAGIC **Tratamentos:** flatten multinível (`product`, `pricing`, `attributes`), `status` uppercase,
# MAGIC tags array → string concatenada com `|` para compatibilidade BI, `updated_at` multi-formato.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'cadastro_produtos'
tipo_carga           = 'delta'
chave_clusterby      = ['product_code']
chave_upsert         = 'product_code'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_raw = spark.sql("""
    SELECT
        upper(trim(product_id))              AS product_code,
        product_name,
        category,
        subcategory,
        upper(trim(product_status))          AS status,
        list_price,
        currency,
        family,
        array_join(tags, '|')                AS tags,
        updated_at,
        rastreamento_source,
        current_timestamp()                  AS data_processamento
    FROM v_source
""")

df_silver = df_raw.withColumn("updated_at", parse_timestamp_multi_format("updated_at"))

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
        ON target.product_code = source.product_code
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
