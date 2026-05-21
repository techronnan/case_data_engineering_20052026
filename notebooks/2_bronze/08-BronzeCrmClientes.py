# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade BronzeCrmClientes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.bronze.crm_clientes` |
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

# MAGIC %md
# MAGIC ### Parâmetros de Gravação

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'crm_clientes'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_bronze_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_bronze_schema}/{nome_tabela}'
SOURCE_FILE             = f"{SOURCES_PATH}/crm_clientes_export.xlsx"

print(f'nome_gravacao_tabela    : {nome_gravacao_tabela}')
print(f'caminho_gravacao_tabela : {caminho_gravacao_tabela}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leitura via openpyxl (XLSX → Spark)

# COMMAND ----------

import openpyxl

local_path = SOURCE_FILE.replace("/FileStore", "/dbfs/FileStore")

wb    = openpyxl.load_workbook(local_path, data_only=True)
sheet = wb.active
rows  = list(sheet.values)

headers   = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
data_rows = [
    tuple(str(v) if v is not None else None for v in row)
    for row in rows[1:]
]

df = spark.createDataFrame(data_rows, schema=headers)

df = (
    df
    .withColumn('rastreamento_source', lit(SOURCE_FILE))
    .withColumn('data_processamento',  current_timestamp())
    .withColumn('dsRefChave', concat(lit('>>'), coalesce(col('customer_code'), lit('NULL'))))
)

print(f"Linhas lidas : {df.count():,}")
print(f"Colunas      : {df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gravação com process_data / MERGE

# COMMAND ----------

table_exists = spark.sql(f"""
    SELECT COUNT(*) FROM system.information_schema.tables
    WHERE table_catalog = '{nome_catalogo}'
      AND table_schema  = '{var_bronze_schema}'
      AND table_name    = '{nome_tabela}'
""").collect()[0][0] > 0

df.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert)
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
