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
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_dedup = spark.sql("""
    WITH ranked AS (
        SELECT
            upper(trim(customer_code))  AS customer_code,
            trim(name)                  AS name,
            upper(trim(segment))        AS segment,
            trim(city)                  AS city,
            state,
            upper(trim(region_code))    AS region_code,
            coalesce(
                to_date(created_at, 'yyyy-MM-dd'),
                to_date(created_at, 'yyyy/MM/dd'),
                to_date(created_at, 'dd/MM/yyyy'),
                to_date(created_at, 'MM/dd/yyyy')
            )                           AS created_at,
            row_number() OVER (
                PARTITION BY upper(trim(customer_code))
                ORDER BY coalesce(
                    to_date(created_at, 'yyyy-MM-dd'),
                    to_date(created_at, 'yyyy/MM/dd'),
                    to_date(created_at, 'dd/MM/yyyy'),
                    to_date(created_at, 'MM/dd/yyyy')
                ) DESC NULLS LAST
            )                           AS _rn
        FROM v_source
    )
    SELECT * EXCEPT (_rn) FROM ranked WHERE _rn = 1
""")

df_silver = (
    normalize_uf_column(df_dedup, "state")
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('customer_code'), lit('NULL'))))
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
        ON target.dsRefChave = source.dsRefChave
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')

drop_v2checkpoint_feature(nome_gravacao_tabela)
