# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverVendedores
# MAGIC
# MAGIC **Tratamentos:** normaliza `regional_code` (extenso → sigla), `canal_id`/`status`/`seller_id`
# MAGIC uppercase, `hire_date` multi-formato, deduplicação por `seller_id` (mais recente).

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'vendedores'
tipo_carga           = 'delta'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_raw = spark.sql("""
    WITH normalizado AS (
        SELECT
            upper(trim(seller_id))   AS seller_id,
            upper(trim(canal_id))    AS canal_id,
            upper(trim(status))      AS status,
            CASE upper(trim(regional_code))
                WHEN 'SUL'      THEN 'S'
                WHEN 'NORTE'    THEN 'N'
                WHEN 'NORDESTE' THEN 'NE'
                WHEN 'SUDESTE'  THEN 'SE'
                WHEN 'CENTRO'   THEN 'CO'
                ELSE upper(trim(regional_code))
            END                      AS regional_code,
            hire_date,
            seller_name,
            rastreamento_source
        FROM v_source
    ),
    dedup AS (
        SELECT *,
            row_number() OVER (
                PARTITION BY seller_id
                ORDER BY
                    CASE
                        WHEN hire_date RLIKE '^\\d{4}[-/]\\d{2}[-/]\\d{2}'
                            THEN regexp_replace(hire_date, '^(\\d{4})[-/](\\d{2})[-/](\\d{2}).*', '$1-$2-$3')
                        WHEN hire_date RLIKE '^\\d{2}/\\d{2}/\\d{4}'
                            THEN regexp_replace(hire_date, '^(\\d{2})/(\\d{2})/(\\d{4}).*', '$3-$2-$1')
                        ELSE NULL
                    END DESC NULLS LAST
            ) AS _rn
        FROM normalizado
    )
    SELECT seller_id, canal_id, status, regional_code, hire_date, seller_name, rastreamento_source
    FROM dedup WHERE _rn = 1
""")

df_silver = (
    df_raw
    .withColumn("hire_date", parse_date_multi_format("hire_date"))
    .withColumn("data_processamento", current_timestamp())
)

# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

df_silver.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df_silver, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.seller_id = source.seller_id
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
