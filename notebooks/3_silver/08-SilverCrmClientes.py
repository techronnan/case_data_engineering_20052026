# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverCrmClientes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.silver.crm_clientes` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** `customer_code` uppercase, normaliza UF/estado via `normalize_uf_column`,
# MAGIC `created_at` multi-formato, deduplicação por `customer_code` (registro mais recente).
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Padronização: dsRefChave, data_processamento, process_data_load/MERGE. |

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

df = spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}')

df_norm = (
    df
    .withColumn("customer_code", upper(trim(col("customer_code"))))
    .withColumn("name",          trim(col("name")))
    .withColumn("segment",       upper(trim(col("segment"))))
    .withColumn("city",          trim(col("city")))
    .withColumn("region_code",   upper(trim(col("region_code"))))
    .withColumn("created_at",    parse_date_multi_format("created_at"))
)

df_norm = normalize_uf_column(df_norm, "state")

w = Window.partitionBy("customer_code").orderBy(col("created_at").desc_nulls_last())
df_silver = (
    df_norm
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('customer_code'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"Bronze: {df.count():,}  |  Silver (dedup): {df_silver.count():,}")

# COMMAND ----------

table_exists = spark.sql(f"""
    SELECT COUNT(*) FROM system.information_schema.tables
    WHERE table_catalog = '{nome_catalogo}'
      AND table_schema  = '{var_silver_schema}'
      AND table_name    = '{nome_tabela}'
""").collect()[0][0] > 0

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
    ''').display()

drop_v2checkpoint_feature(nome_gravacao_tabela)
