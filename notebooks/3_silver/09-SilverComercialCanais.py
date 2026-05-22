# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverComercialCanais
# MAGIC
# MAGIC **Tratamentos:** `channel_id`/`channel_type`/`status` uppercase, trim em campos de texto.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'comercial_canais'
tipo_carga           = 'full'
chave_clusterby      = ['channel_id']
chave_upsert         = 'channel_id'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_silver = spark.sql("""
    SELECT
        upper(trim(id_canal))    AS channel_id,
        trim(nome_canal)         AS channel_name,
        upper(trim(tipo_canal))  AS channel_type,
        CASE WHEN upper(cast(ativo as string)) IN ('TRUE', '1', 'SIM', 'S', 'YES', 'ATIVO')
             THEN 'ATIVO' ELSE 'INATIVO' END AS status,
        current_timestamp()      AS data_processamento
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
        ON target.channel_id = source.channel_id
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
