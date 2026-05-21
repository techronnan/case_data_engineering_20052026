# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverErpPedidosItens
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.silver.erp_pedidos_itens` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** `order_id`/`product_code` uppercase, `unit_price` decimal BR,
# MAGIC flag `is_return` para qty negativa, validação de `total_item`, status normalizado.
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
nome_tabela          = 'erp_pedidos_itens'
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
    .withColumn("order_id",      upper(trim(col("order_id"))))
    .withColumn("product_code",  upper(trim(col("product_code"))))
    .withColumn("quantity",      col("quantity").cast(DoubleType()))
    .withColumn("unit_price",    normalize_decimal_value("unit_price"))
    .withColumn("total_item",    normalize_decimal_value("total_item"))
    .withColumn("item_status",   upper(trim(col("item_status"))))
    .withColumn("is_return",     col("quantity") < 0)
    .withColumn("total_item_expected", col("quantity") * col("unit_price"))
    .withColumn("total_item_diverge",
        (abs(col("total_item") - col("total_item_expected")) > 0.01))
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('order_id'), lit('NULL')),
               lit('>>'), coalesce(col('item_seq').cast('string'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"Linhas        : {df_silver.count():,}")
print(f"Devoluções    : {df_silver.filter(col('is_return')).count():,}")
print(f"Divergências  : {df_silver.filter(col('total_item_diverge')).count():,}")

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
