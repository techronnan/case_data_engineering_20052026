# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimClientes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.gold.dim_clientes` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de clientes com surrogate key. Granularidade: 1 linha por cliente.
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Padronização: dsRefChave, InRegistroAtivo, process_data_load/MERGE. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'dim_clientes'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

df = spark.table(f'{var_environment}.{var_silver_schema}.crm_clientes')

w = Window.orderBy("customer_code")

df_dim = (
    df
    .select(
        "customer_code", "name", "segment",
        "city", "state", "region_code", "created_at"
    )
    .withColumn("customer_key", row_number().over(w))
    .select(
        col("customer_key"),
        col("customer_code").alias("customer_id"),
        col("name").alias("customer_name"),
        col("segment"),
        col("city"),
        col("state"),
        col("region_code"),
        col("created_at"),
    )
    .withColumn("InRegistroAtivo",   lit(1))
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('customer_id'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"dim_clientes: {df_dim.count():,} linhas")

# COMMAND ----------

table_exists = spark.sql(f"""
    SELECT COUNT(*) FROM system.information_schema.tables
    WHERE table_catalog = '{nome_catalogo}'
      AND table_schema  = '{var_gold_schema}'
      AND table_name    = '{nome_tabela}'
""").collect()[0][0] > 0

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
    ''').display()

drop_v2checkpoint_feature(nome_gravacao_tabela)
