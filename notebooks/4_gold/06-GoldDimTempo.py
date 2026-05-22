# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimTempo
# MAGIC
# MAGIC Dimensão de tempo cobrindo 2024-01-01 a 2027-12-31.
# MAGIC `date_key` no formato YYYYMMDD (inteiro) para joins eficientes.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'dim_tempo'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

df_dim = spark.sql("""
    SELECT
        cast(date_format(date, 'yyyyMMdd') AS int) AS date_key,
        date,
        year(date)                                 AS year,
        quarter(date)                              AS quarter,
        month(date)                                AS month,
        date_format(date, 'MMMM')                  AS month_name,
        date_format(date, 'MMM')                   AS month_abbr,
        weekofyear(date)                           AS week_of_year,
        dayofmonth(date)                           AS day_of_month,
        dayofweek(date)                            AS day_of_week,
        date_format(date, 'EEEE')                  AS day_name,
        dayofweek(date) IN (1, 7)                  AS is_weekend,
        1                                          AS InRegistroAtivo,
        concat('>>', date_format(date, 'yyyyMMdd')) AS dsRefChave,
        current_timestamp()                        AS data_processamento
    FROM (
        SELECT explode(sequence(
            to_date('2024-01-01'),
            to_date('2027-12-31'),
            interval 1 day
        )) AS date
    )
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