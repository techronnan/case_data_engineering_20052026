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

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_silver = spark.sql("""
    SELECT
        upper(trim(order_id))        AS order_id,
        upper(trim(customer_code))   AS customer_code,
        upper(trim(seller_id))       AS seller_id,
        upper(trim(channel_id))      AS channel_id,
        upper(trim(region_code))     AS region_code,
        coalesce(
            to_date(order_date, 'yyyy-MM-dd'),
            to_date(order_date, 'yyyy/MM/dd'),
            to_date(order_date, 'dd/MM/yyyy'),
            to_date(order_date, 'MM/dd/yyyy')
        )                            AS order_date,
        coalesce(
            to_date(due_date, 'yyyy-MM-dd'),
            to_date(due_date, 'yyyy/MM/dd'),
            to_date(due_date, 'dd/MM/yyyy'),
            to_date(due_date, 'MM/dd/yyyy')
        )                            AS due_date,
        CASE
            WHEN upper(status) IN ('FATURADO')                                    THEN 'FATURADO'
            WHEN upper(status) IN ('CANCELADO')                                   THEN 'CANCELADO'
            WHEN upper(status) IN ('ENTREGUE')                                    THEN 'ENTREGUE'
            WHEN upper(status) IN ('EM_SEPARACAO','EM SEPARACAO','EM_SEPARAÇÃO') THEN 'EM_SEPARACAO'
            ELSE 'INDEFINIDO'
        END                          AS status,
        cast(regexp_replace(gross_amount,    ',', '.') as double) AS gross_amount,
        cast(regexp_replace(discount_amount, ',', '.') as double) AS discount_amount,
        cast(regexp_replace(net_amount,      ',', '.') as double) AS net_amount,
        payment_details.source       AS payment_source,
        payment_details.priority     AS payment_priority,
        (CASE
            WHEN upper(status) IN ('FATURADO')                                    THEN 'FATURADO'
            WHEN upper(status) IN ('CANCELADO')                                   THEN 'CANCELADO'
            WHEN upper(status) IN ('ENTREGUE')                                    THEN 'ENTREGUE'
            WHEN upper(status) IN ('EM_SEPARACAO','EM SEPARACAO','EM_SEPARAÇÃO') THEN 'EM_SEPARACAO'
            ELSE 'INDEFINIDO'
        END) != 'INDEFINIDO'         AS has_valid_status,
        rastreamento_source,
        concat('>>', coalesce(upper(trim(order_id)), 'NULL')) AS dsRefChave,
        current_timestamp()          AS data_processamento
    FROM v_source
""")

print(f"Linhas saída   : {df_silver.count():,}")

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
