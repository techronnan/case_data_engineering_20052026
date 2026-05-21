# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactOcorrencias
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.gold.fact_ocorrencias` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Silver |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Granularidade: 1 linha por ticket de atendimento.
# MAGIC Conecta a `fact_pedidos` via `order_key` e a `dim_tempo` via `created_date_key`.
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
nome_tabela          = 'fact_ocorrencias'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

so    = spark.table(f'{var_environment}.{var_silver_schema}.atendimento_ocorrencias')
f_ped = spark.table(f'{var_environment}.{var_gold_schema}.fact_pedidos') \
             .select('order_key', col('order_id'))
d_tmp = spark.table(f'{var_environment}.{var_gold_schema}.dim_tempo') \
             .filter(col('InRegistroAtivo') == 1).select('date_key', col('date'))

w = Window.orderBy(so["ticket_id"])

fact = (
    so
    .join(f_ped, so["order_id"] == f_ped["order_id"],               "left")
    .join(d_tmp, so["created_at"].cast("date") == d_tmp["date"],    "left")
    .withColumn("ticket_key", row_number().over(w))
    .select(
        col("ticket_key"),
        col("order_key"),
        col("date_key").alias("created_date_key"),
        so["ticket_id"],
        so["order_id"],
        col("event_type"),
        col("severity"),
        so["status"],
        col("has_event_type"),
        col("has_severity"),
        col("created_at"),
        col("updated_at"),
    )
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(so["ticket_id"], lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"fact_ocorrencias  : {fact.count():,} linhas")
print(f"Sem order_key     : {fact.filter(col('order_key').isNull()).count():,}")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação Final do Star Schema

# COMMAND ----------

spark.sql(f"""
    SELECT 'fact_pedidos'      AS tabela, COUNT(*) AS linhas FROM {var_environment}.{var_gold_schema}.fact_pedidos
    UNION ALL
    SELECT 'fact_itens_pedido', COUNT(*) FROM {var_environment}.{var_gold_schema}.fact_itens_pedido
    UNION ALL
    SELECT 'fact_entregas',     COUNT(*) FROM {var_environment}.{var_gold_schema}.fact_entregas
    UNION ALL
    SELECT 'fact_ocorrencias',  COUNT(*) FROM {var_environment}.{var_gold_schema}.fact_ocorrencias
    UNION ALL
    SELECT 'dim_clientes',      COUNT(*) FROM {var_environment}.{var_gold_schema}.dim_clientes
    UNION ALL
    SELECT 'dim_produtos',      COUNT(*) FROM {var_environment}.{var_gold_schema}.dim_produtos
    UNION ALL
    SELECT 'dim_regioes',       COUNT(*) FROM {var_environment}.{var_gold_schema}.dim_regioes
    UNION ALL
    SELECT 'dim_canais',        COUNT(*) FROM {var_environment}.{var_gold_schema}.dim_canais
    UNION ALL
    SELECT 'dim_vendedores',    COUNT(*) FROM {var_environment}.{var_gold_schema}.dim_vendedores
    UNION ALL
    SELECT 'dim_tempo',         COUNT(*) FROM {var_environment}.{var_gold_schema}.dim_tempo
    ORDER BY tabela
""").display()
