# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverAtendimentoOcorrencias
# MAGIC
# MAGIC **Tratamentos:** timestamps multi-formato, normaliza `status`/`severity`/`event_type`,
# MAGIC `ticket_id`/`order_id` uppercase, flags de qualidade de campos nulos.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'atendimento_ocorrencias'
tipo_carga           = 'delta'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_raw = spark.sql("""
    SELECT
        upper(trim(ticket_id))   AS ticket_id,
        upper(trim(order_id))    AS order_id,
        upper(trim(status))      AS status,
        upper(trim(severity))    AS severity,
        lower(trim(event_type))  AS event_type,
        created_at,
        lower(trim(event_type)) IS NOT NULL AS has_event_type,
        upper(trim(severity))   IS NOT NULL AS has_severity,
        upper(trim(order_id))   IS NOT NULL AS has_order_ref,
        rastreamento_source,
        current_timestamp()                                    AS data_processamento
    FROM v_source
""")

df_silver = df_raw.withColumn("created_at", parse_timestamp_multi_format("created_at"))

# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

df_silver.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df_silver, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.ticket_id = source.ticket_id
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')
