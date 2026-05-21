# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactEntregas
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.gold.fact_entregas` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Granularidade: 1 linha por entrega.
# MAGIC Conecta a `fact_pedidos` via `order_key` e a `dim_tempo` para datas de envio/entrega.
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
nome_tabela          = 'fact_entregas'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

se    = spark.table(f'{var_environment}.{var_silver_schema}.logistica_entregas')
f_ped = spark.table(f'{var_environment}.{var_gold_schema}.fact_pedidos') \
             .select('order_key', col('order_id'))
d_tmp = spark.table(f'{var_environment}.{var_gold_schema}.dim_tempo') \
             .filter(col('InRegistroAtivo') == 1).select('date_key', col('date'))

w = Window.orderBy(se["delivery_id"])

fact = (
    se
    .join(f_ped,              se["order_id"] == f_ped["order_id"],                      "left")
    .join(d_tmp.alias("d_ship"), se["shipped_at"].cast("date")   == col("d_ship.date"), "left")
    .join(d_tmp.alias("d_del"),  se["delivered_at"].cast("date") == col("d_del.date"),  "left")
    .withColumn("delivery_key", row_number().over(w))
    .select(
        col("delivery_key"),
        col("order_key"),
        se["delivery_id"],
        col("d_ship.date_key").alias("shipped_date_key"),
        col("d_del.date_key").alias("delivered_date_key"),
        col("carrier_name"),
        col("carrier_mode"),
        col("delivery_status"),
        col("dest_state"),
        col("dest_city"),
        col("cost"),
        col("delivery_days"),
        col("is_late"),
    )
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(se["delivery_id"], lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"fact_entregas : {fact.count():,} linhas")
print(f"Atrasadas     : {fact.filter(col('is_late')).count():,}")

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
