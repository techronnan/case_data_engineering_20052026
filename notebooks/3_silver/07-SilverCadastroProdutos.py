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
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_silver = spark.sql("""
    SELECT
        upper(trim(product.product_id))      AS product_code,
        product.name                         AS product_name,
        product.category                     AS category,
        product.subcategory                  AS subcategory,
        upper(trim(product.status))          AS status,
        cast(pricing.list_price AS double)   AS list_price,
        pricing.currency                     AS currency,
        attributes.family                    AS family,
        array_join(attributes.tags, '|')     AS tags,
        coalesce(
            to_timestamp(updated_at, "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss'),
            to_timestamp(updated_at, 'dd/MM/yyyy HH:mm'),
            to_timestamp(updated_at, 'yyyy/MM/dd'),
            cast(to_date(updated_at, 'yyyy-MM-dd') AS timestamp)
        )                                    AS updated_at,
        rastreamento_source,
        concat('>>', coalesce(upper(trim(product.product_id)), 'NULL')) AS dsRefChave,
        current_timestamp()                  AS data_processamento
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
        ON target.dsRefChave = source.dsRefChave
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')

drop_v2checkpoint_feature(nome_gravacao_tabela)
