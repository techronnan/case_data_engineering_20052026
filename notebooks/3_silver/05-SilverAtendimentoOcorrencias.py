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

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_silver = spark.sql("""
    SELECT
        upper(trim(ticket_id))   AS ticket_id,
        upper(trim(order_id))    AS order_id,
        upper(trim(status))      AS status,
        upper(trim(severity))    AS severity,
        lower(trim(event_type))  AS event_type,
        coalesce(
            to_timestamp(created_at, "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(created_at, 'yyyy-MM-dd HH:mm:ss'),
            to_timestamp(created_at, 'dd/MM/yyyy HH:mm'),
            to_timestamp(created_at, 'yyyy/MM/dd'),
            cast(to_date(created_at, 'yyyy-MM-dd') as timestamp)
        )                        AS created_at,
        coalesce(
            to_timestamp(updated_at, "yyyy-MM-dd'T'HH:mm:ss"),
            to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss'),
            to_timestamp(updated_at, 'dd/MM/yyyy HH:mm'),
            to_timestamp(updated_at, 'yyyy/MM/dd'),
            cast(to_date(updated_at, 'yyyy-MM-dd') as timestamp)
        )                        AS updated_at,
        lower(trim(event_type)) IS NOT NULL                          AS has_event_type,
        upper(trim(severity))   IS NOT NULL                          AS has_severity,
        upper(trim(order_id))   IS NOT NULL                          AS has_order_ref,
        rastreamento_source,
        concat('>>', coalesce(upper(trim(ticket_id)), 'NULL'))        AS dsRefChave,
        current_timestamp()                                          AS data_processamento
    FROM v_source
""")

print(f"Linhas : {df_silver.count():,}")
df_silver.groupBy("status").count().show()
df_silver.groupBy("event_type").count().orderBy("count", ascending=False).show()

# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

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
