# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimVendedores
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.gold.dim_vendedores` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de vendedores. Join com `dim_regioes` e `dim_canais` para resolver FKs.
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
nome_tabela          = 'dim_vendedores'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

df_vend = spark.table(f'{var_environment}.{var_silver_schema}.vendedores')
df_reg  = spark.table(f'{var_environment}.{var_gold_schema}.dim_regioes') \
               .filter(col('InRegistroAtivo') == 1) \
               .select('region_key', 'regional_code')
df_can  = spark.table(f'{var_environment}.{var_gold_schema}.dim_canais') \
               .filter(col('InRegistroAtivo') == 1) \
               .select('channel_key', 'channel_id')

w = Window.orderBy("seller_id")

df_dim = (
    df_vend
    .join(df_reg, df_vend["regional_code"] == df_reg["regional_code"], "left")
    .join(df_can, df_vend["canal_id"]      == df_can["channel_id"],    "left")
    .withColumn("seller_key", row_number().over(w))
    .select(
        col("seller_key"),
        df_vend["seller_id"],
        col("seller_name"),
        col("region_key"),
        col("channel_key"),
        col("hire_date"),
        df_vend["status"],
    )
    .withColumn("InRegistroAtivo",   lit(1))
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(df_vend["seller_id"], lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"dim_vendedores: {df_dim.count():,} linhas")
print(f"Sem região    : {df_dim.filter(col('region_key').isNull()).count():,}")
print(f"Sem canal     : {df_dim.filter(col('channel_key').isNull()).count():,}")

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
