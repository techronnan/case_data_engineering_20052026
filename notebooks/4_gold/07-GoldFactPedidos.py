# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactPedidos
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.gold.fact_pedidos` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Fato central do Star Schema. Granularidade: 1 linha por pedido.
# MAGIC Join com todas as dimensões para resolver surrogate keys.
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
nome_tabela          = 'fact_pedidos'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

sp     = spark.table(f'{var_environment}.{var_silver_schema}.erp_pedidos_cabecalho')
d_cli  = spark.table(f'{var_environment}.{var_gold_schema}.dim_clientes') \
              .filter(col('InRegistroAtivo') == 1).select('customer_key', col('customer_id'))
d_vend = spark.table(f'{var_environment}.{var_gold_schema}.dim_vendedores') \
              .filter(col('InRegistroAtivo') == 1).select('seller_key', col('seller_id'))
d_can  = spark.table(f'{var_environment}.{var_gold_schema}.dim_canais') \
              .filter(col('InRegistroAtivo') == 1).select('channel_key', col('channel_id'))
d_reg  = spark.table(f'{var_environment}.{var_gold_schema}.dim_regioes') \
              .filter(col('InRegistroAtivo') == 1).select('region_key', col('regional_code'))
d_tmp  = spark.table(f'{var_environment}.{var_gold_schema}.dim_tempo') \
              .filter(col('InRegistroAtivo') == 1).select('date_key', col('date'))

w = Window.orderBy("order_id")

fact = (
    sp
    .join(d_cli,  sp["customer_code"] == d_cli["customer_id"],   "left")
    .join(d_vend, sp["seller_id"]     == d_vend["seller_id"],    "left")
    .join(d_can,  sp["channel_id"]    == d_can["channel_id"],    "left")
    .join(d_reg,  sp["region_code"]   == d_reg["regional_code"], "left")
    .join(d_tmp,  sp["order_date"]    == d_tmp["date"],          "left")
    .withColumn("order_key", row_number().over(w))
    .select(
        col("order_key"),
        sp["order_id"],
        col("date_key").alias("order_date_key"),
        col("customer_key"),
        col("seller_key"),
        col("channel_key"),
        col("region_key"),
        sp["status"],
        col("gross_amount"),
        col("discount_amount"),
        col("net_amount"),
        col("payment_source"),
        col("payment_priority"),
        sp["due_date"],
    )
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(sp["order_id"], lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"fact_pedidos  : {fact.count():,} linhas")
print(f"Sem cliente   : {fact.filter(col('customer_key').isNull()).count():,}")
print(f"Sem vendedor  : {fact.filter(col('seller_key').isNull()).count():,}")
print(f"Sem data      : {fact.filter(col('order_date_key').isNull()).count():,}")

# COMMAND ----------

table_exists = spark.sql(f"""
    SELECT COUNT(*) FROM system.information_schema.tables
    WHERE table_catalog = '{nome_catalogo}'
      AND table_schema  = '{var_gold_schema}'
      AND table_name    = '{nome_tabela}'
""").collect()[0][0] > 0

fact.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(fact, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert)
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
