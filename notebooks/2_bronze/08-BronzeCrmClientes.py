# Databricks notebook source
# DBTITLE 1,BronzeCrmClientes

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

# DBTITLE 1,Parâmetros
container_source = 'crm'
nome_arquivo     = 'crm_clientes'
file_name_saida  = 'crm_clientes'

# COMMAND ----------

# DBTITLE 1,Inicialização

# COMMAND ----------

# DBTITLE 1,Inicializa contexto
var_renomear, var_merge, table_id, merge_condition, caminho_leitura, caminho_gravacao, schemalocal, checkpoint, nome_tabela = initialize_bronze_context(
    container_source=container_source,
    nome_arquivo=nome_arquivo,
    file_name_saida=file_name_saida,
)

# COMMAND ----------

# DBTITLE 1,Leitura via AutoLoader - Parquet
# MAGIC %md
# MAGIC ### Leitura via AutoLoader com Evolução de Schema

# COMMAND ----------

# DBTITLE 1,Leitura Parquet
dfReadStream = (
    spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'parquet')
    .option('cloudFiles.inferColumnTypes', 'true')
    .option('cloudFiles.schemaLocation', schemalocal)
    .option('cloudFiles.schemaEvolutionMode', 'addNewColumns')
    .load(caminho_leitura)
    .withColumn('rastreamento_source', col('_metadata.file_path'))
)

for row in var_renomear:
    dfReadStream = dfReadStream.withColumnRenamed(row['de'], row['para_alias'])

dfReadStream = dfReadStream.withColumn(
    'dsRefChave',
    concat(lit('>>'), coalesce(col('customer_id'), lit('NULL')))
)

# COMMAND ----------

# DBTITLE 1,Gravação Delta
# MAGIC %md
# MAGIC ### Gravação via Streaming com Upsert Delta

# COMMAND ----------

# DBTITLE 1,Stream write
streamQuery = (
    dfReadStream.writeStream
    .format('delta')
    .outputMode('append')
    .foreachBatch(upsert_delta_live(
        nome_tabela=nome_tabela,
        caminho_gravacao=caminho_gravacao,
        merge_condition=merge_condition,
        table_id=table_id,
        order_key='rastreamento_source',
    ))
    .queryName(nome_tabela)
    .trigger(availableNow=True)
    .option('checkpointLocation', checkpoint)
    .start()
)

import time as _time
_t0 = _time.time()

try:
    streamQuery.awaitTermination()
    _n_rows = spark.table(nome_tabela).count()
    log_table_execution(nome_tabela, round(_time.time() - _t0, 2), 'SUCESSO', _n_rows)
except Exception as _monitor_e:
    log_table_execution(nome_tabela, round(_time.time() - _t0, 2), 'FALHA', 0, str(_monitor_e))
    raise
