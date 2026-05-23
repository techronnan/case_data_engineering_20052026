# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldFactOcorrencias
# MAGIC
# MAGIC Granularidade: 1 linha por ticket de atendimento.
# MAGIC Conecta a `fact_pedidos` via `order_key` e a `dim_tempo` via `created_date_key`.

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'fact_ocorrencias'
tipo_carga           = 'delta'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_silver_schema}.atendimento_ocorrencias').createOrReplaceTempView('v_so')
spark.table(f'{var_environment}.{var_gold_schema}.fact_pedidos').createOrReplaceTempView('v_fp')
spark.table(f'{var_environment}.{var_gold_schema}.dim_tempo').createOrReplaceTempView('v_tmp')

fact = spark.sql("""
    SELECT
        row_number() OVER (ORDER BY so.ticket_id) AS ticket_key,
        fp.order_key,
        t.date_key                                AS created_date_key,
        so.ticket_id,
        so.order_id,
        so.event_type,
        so.severity,
        so.status,
        so.has_event_type,
        so.has_severity,
        so.created_at,
        current_timestamp()                       AS data_processamento
    FROM v_so so
    LEFT JOIN v_fp  fp ON so.order_id                 = fp.order_id
    LEFT JOIN v_tmp t  ON cast(so.created_at AS date) = t.date AND t.InRegistroAtivo = 1
""")


# COMMAND ----------

table_exists = spark.catalog.tableExists(nome_gravacao_tabela)

fact.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(fact, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela)
else:
    print('Entrou na condição MERGE')
    spark.sql(f'''
        MERGE INTO {nome_gravacao_tabela} AS target
        USING df_incremental AS source
        ON target.ticket_id = source.ticket_id
        WHEN MATCHED AND source.data_processamento >= target.data_processamento THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')