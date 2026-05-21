# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverVendedores
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.silver.vendedores` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** normaliza `regional_code` (extenso → sigla), `canal_id`/`status`/`seller_id`
# MAGIC uppercase, `hire_date` multi-formato, deduplicação por `seller_id` (mais recente).
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
nome_tabela          = 'vendedores'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

from pyspark.sql.functions import create_map
from itertools import chain

df = spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}')

REGION_MAP = {"SUL": "S", "NORTE": "N", "NORDESTE": "NE", "SUDESTE": "SE", "CENTRO": "CO"}
map_expr   = create_map([lit(x) for x in chain(*REGION_MAP.items())])

df_norm = (
    df
    .withColumn("seller_id",     upper(trim(col("seller_id"))))
    .withColumn("canal_id",      upper(trim(col("canal_id"))))
    .withColumn("status",        upper(trim(col("status"))))
    .withColumn("regional_code", upper(trim(col("regional_code"))))
    .withColumn("hire_date",     parse_date_multi_format("hire_date"))
    .withColumn("regional_code",
        coalesce(map_expr[col("regional_code")], col("regional_code")))
)

w = Window.partitionBy("seller_id").orderBy(col("hire_date").desc_nulls_last())
df_silver = (
    df_norm
    .withColumn("_rn", row_number().over(w))
    .filter(col("_rn") == 1)
    .drop("_rn")
    .withColumn("dsRefChave",
        concat(lit('>>'), coalesce(col('seller_id'), lit('NULL'))))
    .withColumn("data_processamento", current_timestamp())
)

print(f"Bronze : {df.count():,}  |  Silver (dedup): {df_silver.count():,}")

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
