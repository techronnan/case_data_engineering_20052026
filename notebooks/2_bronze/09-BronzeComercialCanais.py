# Databricks notebook source
# MAGIC %md
# MAGIC # Entidade BronzeComercialCanais
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.bronze.comercial_canais` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Landing (XLSX) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |
# MAGIC
# MAGIC > **Nota:** Arquivos `.xlsx` não têm suporte pelo AutoLoader. Utiliza `openpyxl` para leitura direta + `spark.createDataFrame()` — sem dependência de pandas.
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação do notebook e padronização com dsRefChave e process_data/MERGE. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

# DBTITLE 1,Parâmetros
container_source = 'canais'
nome_arquivo     = 'comercial_canais'
file_name_saida  = 'comercial_canais'

# COMMAND ----------

# DBTITLE 1,Inicializa contexto
var_renomear, var_merge, table_id, merge_condition, caminho_leitura, caminho_gravacao, schemalocal, checkpoint, nome_tabela = initialize_bronze_context(
    container_source=container_source,
    nome_arquivo=nome_arquivo,
    file_name_saida=file_name_saida,
)

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
    concat(lit('>>'), coalesce(col('channel_id'), lit('NULL')))
)

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
