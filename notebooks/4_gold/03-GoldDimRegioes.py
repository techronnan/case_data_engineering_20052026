# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimRegioes
# MAGIC
# MAGIC Dimensão de regiões geográficas. Granularidade: 1 linha por região ativa.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'dim_regioes'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_silver_schema}.legado_regioes').createOrReplaceTempView('v_source')

df_dim = spark.sql("""
    SELECT
        row_number() OVER (ORDER BY regional_code) AS region_key,
        regional_code,
        region_name,
        manager                                    AS region_manager,
        state,
        1                                          AS InRegistroAtivo,
        concat('>>', coalesce(regional_code, 'NULL')) AS dsRefChave,
        current_timestamp()                        AS data_processamento
    FROM v_source
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

drop_v2checkpoint_feature(nome_gravacao_tabela)
