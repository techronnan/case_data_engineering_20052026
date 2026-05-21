# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeAtendimentoOcorrencias
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.bronze.atendimento_ocorrencias` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Landing (NDJSON) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Bronze |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação do notebook e padronização para AutoLoader com upsert por dsRefChave. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

container_source = 'atendimento'
nome_arquivo     = 'atendimento_ocorrencias'
file_name_saida  = 'atendimento_ocorrencias'

# COMMAND ----------

var_renomear, var_merge, table_id, merge_condition, caminho_leitura, caminho_gravacao, schemalocal, checkpoint, nome_tabela = initialize_bronze_context(
    container_source=container_source,
    nome_arquivo=nome_arquivo,
    file_name_saida=file_name_saida,
)

# COMMAND ----------

# NDJSON = JSON Lines (uma linha por registro), multiLine=false é o padrão do AutoLoader
dfReadStream = (
    spark.readStream.format('cloudFiles')
    .option('cloudFiles.format', 'json')
    .option('multiLine', 'false')
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
    concat(lit('>>'), coalesce(col('ticket_id'), lit('NULL')))
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

streamQuery.awaitTermination()
