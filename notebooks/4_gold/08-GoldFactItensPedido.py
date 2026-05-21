# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactItensPedido
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.gold.fact_itens_pedido` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Granularidade: 1 linha por item de pedido.
# MAGIC Conecta a `fact_pedidos` via `order_key` e a `dim_produtos` via `product_key`.
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
nome_tabela          = 'fact_itens_pedido'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

si     = spark.table(f'{var_environment}.{var_silver_schema}.erp_pedidos_itens')
f_ped  = spark.table(f'{var_environment}.{var_gold_schema}.fact_pedidos') \
              .select('order_key', col('order_id'))
d_prod = spark.table(f'{var_environment}.{var_gold_schema}.dim_produtos') \
              .filter(col('InRegistroAtivo') == 1).select('product_key', col('product_id'))

w = Window.orderBy(si["order_id"], col("item_seq"))

fact = (
    si
    .join(f_ped,  si["order_id"]     == f_ped["order_id"],    "left")
    .join(d_prod, si["product_code"] == d_prod["product_id"], "left")
    .withColumn("item_key", row_number().over(w))
    .select(
        col("item_key"),
        col("order_key"),
        col("product_key"),
        si["order_id"],
        col("item_seq"),
        col("quantity"),
        col("unit_price"),
        col("total_item"),
        col("item_status"),
        col("is_return"),
        col("total_item_diverge"),
    )
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(si["order_id"], lit('NULL')),
               lit('>>'), coalesce(col('item_seq').cast('string'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"fact_itens_pedido : {fact.count():,} linhas")
print(f"Devoluções        : {fact.filter(col('is_return')).count():,}")

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
