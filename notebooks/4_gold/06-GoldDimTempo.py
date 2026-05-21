# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade GoldDimTempo
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.gold.dim_tempo` |
# MAGIC | Origem Fonte de Dados de Entrada | Gerada sinteticamente (sequência de datas) |
# MAGIC | Destino Fonte de Dados de Saída | Camada Gold |
# MAGIC
# MAGIC Dimensão de tempo cobrindo 2024-01-01 a 2027-12-31.
# MAGIC `date_key` no formato YYYYMMDD (inteiro) para joins eficientes.
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Padronização: dsRefChave, InRegistroAtivo, process_data_load/MERGE. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

nome_catalogo        = var_environment
nome_tabela          = 'dim_tempo'
tipo_carga           = 'full'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

from pyspark.sql.functions import (
    date_format, year, quarter, month, weekofyear,
    dayofweek, dayofmonth
)

df_dim = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'),
        to_date('2027-12-31'),
        interval 1 day
    )) AS date
""").select(
    date_format("date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
    col("date"),
    year("date").alias("year"),
    quarter("date").alias("quarter"),
    month("date").alias("month"),
    date_format("date", "MMMM").alias("month_name"),
    date_format("date", "MMM").alias("month_abbr"),
    weekofyear("date").alias("week_of_year"),
    dayofmonth("date").alias("day_of_month"),
    dayofweek("date").alias("day_of_week"),
    date_format("date", "EEEE").alias("day_name"),
    when(dayofweek("date").isin(1, 7), lit(True)).otherwise(lit(False)).alias("is_weekend"),
).withColumn("InRegistroAtivo",   lit(1)) \
 .withColumn("dsRefChave",
     concat(lit('>>'), col("date_key").cast("string"))) \
 .withColumn("data_processamento", current_timestamp())

print(f"dim_tempo: {df_dim.count():,} dias gerados")
df_dim.show(5)

# COMMAND ----------

table_exists = spark.sql(f"""
    SELECT COUNT(*) FROM system.information_schema.tables
    WHERE table_catalog = '{nome_catalogo}'
      AND table_schema  = '{var_gold_schema}'
      AND table_name    = '{nome_tabela}'
""").collect()[0][0] > 0

df_dim.createOrReplaceTempView('df_incremental')

if tipo_carga == 'full' or not table_exists:
    print('Primeira Carga ou Carga Full')
    process_data_load(df_dim, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert)
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
