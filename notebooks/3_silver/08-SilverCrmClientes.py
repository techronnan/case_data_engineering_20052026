# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverCrmClientes
# MAGIC
# MAGIC **Tratamentos:** `customer_code` uppercase, normaliza UF/estado via `normalize_uf_column`,
# MAGIC `created_at` multi-formato, deduplicação por `customer_code` (registro mais recente).

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'crm_clientes'
tipo_carga           = 'delta'
chave_clusterby      = ['customer_code']
chave_upsert         = 'customer_code'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_dedup = spark.sql("""
    WITH ranked AS (
        SELECT
            upper(trim(customer_id))    AS customer_code,
            trim(nome_cliente)          AS name,
            upper(trim(segmento))       AS segment,
            trim(cidade)                AS city,
            estado                      AS state,
            NULL                        AS region_code,
            data_cadastro,
            row_number() OVER (
                PARTITION BY upper(trim(customer_id))
                ORDER BY
                    CASE
                        WHEN data_cadastro RLIKE '^\\d{4}[-/]\\d{2}[-/]\\d{2}'
                            THEN regexp_replace(data_cadastro, '^(\\d{4})[-/](\\d{2})[-/](\\d{2}).*', '$1-$2-$3')
                        WHEN data_cadastro RLIKE '^\\d{2}/\\d{2}/\\d{4}'
                            THEN regexp_replace(data_cadastro, '^(\\d{2})/(\\d{2})/(\\d{4}).*', '$3-$2-$1')
                        ELSE NULL
                    END DESC NULLS LAST
            ) AS _rn
        FROM v_source
    )
    SELECT * EXCEPT (_rn) FROM ranked WHERE _rn = 1
""")

df_silver = (
    normalize_uf_column(df_dedup, "state")
    .withColumn("created_at", parse_date_multi_format("data_cadastro"))
    .drop("data_cadastro")
    .withColumn("data_processamento", current_timestamp())
)

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
        ON target.customer_code = source.customer_code
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
