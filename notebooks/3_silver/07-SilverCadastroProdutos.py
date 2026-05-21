# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverCadastroProdutos
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.silver.cadastro_produtos` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** flatten multinível (`product`, `pricing`, `attributes`), `status` uppercase,
# MAGIC tags array → string concatenada com `|` para compatibilidade BI, `updated_at` multi-formato.
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
nome_tabela          = 'cadastro_produtos'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

df = spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}')

df_silver = (
    df
    .select(
        upper(trim(col("product.product_id"))).alias("product_code"),
        col("product.name").alias("product_name"),
        col("product.category").alias("category"),
        col("product.subcategory").alias("subcategory"),
        upper(trim(col("product.status"))).alias("status"),
        col("pricing.list_price").cast(DoubleType()).alias("list_price"),
        col("pricing.currency").alias("currency"),
        col("attributes.family").alias("family"),
        concat_ws("|", col("attributes.tags")).alias("tags"),
        parse_timestamp_multi_format("updated_at").alias("updated_at"),
        col("rastreamento_source"),
    )
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('product_code'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"Linhas : {df_silver.count():,}")
df_silver.groupBy("category").count().orderBy("count", ascending=False).show()

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
