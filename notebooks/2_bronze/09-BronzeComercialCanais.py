# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeComercialCanais
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.bronze.comercial_canais` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Landing (XLSX) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |
# MAGIC
# MAGIC > **Nota:** Arquivos `.xlsx` não têm suporte pelo AutoLoader. Utiliza `pandas.read_excel()` + `spark.createDataFrame()`.
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação do notebook e padronização com dsRefChave e process_data/MERGE. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'comercial_canais'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_bronze_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_bronze_schema}/{nome_tabela}'
SOURCE_FILE             = f"{SOURCES_PATH}/comercial_canais.xlsx"

print(f'nome_gravacao_tabela    : {nome_gravacao_tabela}')
print(f'caminho_gravacao_tabela : {caminho_gravacao_tabela}')

# COMMAND ----------

local_path = SOURCE_FILE.replace("/FileStore", "/dbfs/FileStore")

df_pd = pd.read_excel(local_path, dtype=str)
df = spark.createDataFrame(df_pd)

df = (
    df
    .withColumn('rastreamento_source', lit(SOURCE_FILE))
    .withColumn('data_processamento',  current_timestamp())
    .withColumn('dsRefChave', concat(lit('>>'), coalesce(col('channel_id'), lit('NULL'))))
)

print(f"Linhas lidas : {df.count():,}")

# COMMAND ----------

table_exists = spark.sql(f"""
    SELECT COUNT(*) FROM system.information_schema.tables
    WHERE table_catalog = '{nome_catalogo}'
      AND table_schema  = '{var_bronze_schema}'
      AND table_name    = '{nome_tabela}'
""").collect()[0][0] > 0

df.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.dsRefChave = source.dsRefChave
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''').display()

drop_v2checkpoint_feature(nome_gravacao_tabela)
