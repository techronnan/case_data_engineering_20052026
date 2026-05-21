# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverErpPedidosCabecalho
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.silver.erp_pedidos_cabecalho` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** normalização de datas (3 formatos), status canônico, `order_id` uppercase,
# MAGIC valores decimais com vírgula, extração de `payment_details` JSON, flag de status indefinido.
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
nome_tabela          = 'erp_pedidos_cabecalho'
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
    .withColumn("order_id",        upper(trim(col("order_id"))))
    .withColumn("customer_code",   upper(trim(col("customer_code"))))
    .withColumn("seller_id",       upper(trim(col("seller_id"))))
    .withColumn("channel_id",      upper(trim(col("channel_id"))))
    .withColumn("region_code",     upper(trim(col("region_code"))))
    .withColumn("order_date",      parse_date_multi_format("order_date"))
    .withColumn("due_date",        parse_date_multi_format("due_date"))
    .withColumn("status",          normalize_status_pedido("status"))
    .withColumn("gross_amount",    normalize_decimal_value("gross_amount"))
    .withColumn("discount_amount", normalize_decimal_value("discount_amount"))
    .withColumn("net_amount",      normalize_decimal_value("net_amount"))
    .withColumn("payment_source",
        when(col("payment_details").isNotNull(),
             col("payment_details").getItem("source")).otherwise(lit(None)))
    .withColumn("payment_priority",
        when(col("payment_details").isNotNull(),
             col("payment_details").getItem("priority")).otherwise(lit(None)))
    .withColumn("has_valid_status", col("status") != "INDEFINIDO")
    .drop("payment_details")
    .withColumn("dsRefChave",         concat(lit('>>'), coalesce(col('order_id'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"Linhas entrada : {df.count():,}")
print(f"Linhas saída   : {df_silver.count():,}")

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
