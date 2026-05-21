# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Entidade SilverLegadoRegioes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Tabela de Dados de Saída | `{environment}.silver.legado_regioes` |
# MAGIC | Origem Fonte de Dados de Entrada | Camada Bronze |
# MAGIC | Destino Fonte de Dados de Saída | Camada Silver |
# MAGIC
# MAGIC **Tratamentos:** normalização de `regional_code` (extenso → sigla), filtro `active_flag=1`,
# MAGIC remove região XX inativa, deduplicação por `regional_code`.
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
nome_tabela          = 'legado_regioes'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'

nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
print(f'nome_gravacao_tabela : {nome_gravacao_tabela}')

# COMMAND ----------

spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}').createOrReplaceTempView('v_source')

df_silver = spark.sql("""
    WITH normalizado AS (
        SELECT
            CASE upper(trim(regional_code))
                WHEN 'SUL'      THEN 'S'
                WHEN 'NORTE'    THEN 'N'
                WHEN 'NORDESTE' THEN 'NE'
                WHEN 'CENTRO'   THEN 'CO'
                WHEN 'SUDESTE'  THEN 'SE'
                WHEN 'CO'       THEN 'CO'
                ELSE upper(trim(regional_code))
            END                                AS regional_code,
            cast(active_flag as int)           AS active_flag,
            region_name,
            manager,
            state,
            rastreamento_source
        FROM v_source
        WHERE cast(active_flag as int) = 1
          AND upper(trim(regional_code)) != 'XX'
    ),
    dedup AS (
        SELECT *,
            row_number() OVER (PARTITION BY regional_code ORDER BY active_flag DESC) AS _rn
        FROM normalizado
    )
    SELECT
        regional_code,
        active_flag,
        region_name,
        manager,
        state,
        rastreamento_source,
        concat('>>', coalesce(regional_code, 'NULL')) AS dsRefChave,
        current_timestamp()                           AS data_processamento
    FROM dedup
    WHERE _rn = 1
""")

print(f"Linhas silver : {df_silver.count():,}")

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
