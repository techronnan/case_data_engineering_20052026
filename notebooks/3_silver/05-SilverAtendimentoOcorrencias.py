# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverAtendimentoOcorrencias
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.silver.atendimento_ocorrencias` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** timestamps multi-formato, normaliza `status`/`severity`/`event_type`,
# MAGIC `ticket_id`/`order_id` uppercase, flags de qualidade de campos nulos.
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
nome_tabela          = 'atendimento_ocorrencias'
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
    .withColumn("ticket_id",   upper(trim(col("ticket_id"))))
    .withColumn("order_id",    upper(trim(col("order_id"))))
    .withColumn("status",      upper(trim(col("status"))))
    .withColumn("severity",    upper(trim(col("severity"))))
    .withColumn("event_type",  lower(trim(col("event_type"))))
    .withColumn("created_at",  parse_timestamp_multi_format("created_at"))
    .withColumn("updated_at",  parse_timestamp_multi_format("updated_at"))
    .withColumn("has_event_type", col("event_type").isNotNull())
    .withColumn("has_severity",   col("severity").isNotNull())
    .withColumn("has_order_ref",  col("order_id").isNotNull())
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('ticket_id'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"Linhas : {df_silver.count():,}")
df_silver.groupBy("status").count().show()
df_silver.groupBy("event_type").count().orderBy("count", ascending=False).show()

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
