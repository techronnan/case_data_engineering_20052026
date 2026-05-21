# Databricks notebook source
# MAGIC %md
# MAGIC # Entidade BronzeLogisticaEntregas
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.bronze.logistica_entregas` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Landing (JSON Array com structs aninhados) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação do notebook e padronização para AutoLoader com upsert por dsRefChave. |
# MAGIC | 21/05/2026 | Ronnan           | Monitoramento: log_table_execution registrado após awaitTermination. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

container_source = 'logistica'
nome_arquivo     = 'logistica_entregas'
file_name_saida  = 'logistica_entregas'

# COMMAND ----------

var_renomear, var_merge, table_id, merge_condition, caminho_leitura, caminho_gravacao, schemalocal, checkpoint, nome_tabela = initialize_bronze_context(
    container_source=container_source,
    nome_arquivo=nome_arquivo,
    file_name_saida=file_name_saida,
)

# COMMAND ----------

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
    concat(lit('>>'), coalesce(col('delivery_id'), lit('NULL')))
)

# COMMAND ----------

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
