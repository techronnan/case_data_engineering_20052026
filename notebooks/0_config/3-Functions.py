# Databricks notebook source
def parse_date_multi_format(col_name: str):
    """Regex: normaliza qualquer string de data para DateType (yyyy-MM-dd)."""
    c = col(col_name)
    normalized = (
        when(c.rlike(r'^\d{4}[-/]\d{2}[-/]\d{2}'),
             regexp_replace(c, r'^(\d{4})[-/](\d{2})[-/](\d{2}).*', '$1-$2-$3'))
        .when(c.rlike(r'^\d{2}/\d{2}/\d{4}'),
             regexp_replace(c, r'^(\d{2})/(\d{2})/(\d{4}).*', '$3-$2-$1'))
        .otherwise(lit(None).cast('string'))
    )
    return to_date(normalized, 'yyyy-MM-dd')


def parse_timestamp_multi_format(col_name: str):
    """Regex: normaliza qualquer string de data/hora para TimestampType (yyyy-MM-dd HH:mm:ss)."""
    c = col(col_name)
    normalized = (
        when(c.rlike(r'^\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}'),
             regexp_replace(c, r'^(\d{4})[-/](\d{2})[-/](\d{2})[T ](\d{2}:\d{2}:\d{2}).*', '$1-$2-$3 $4'))
        .when(c.rlike(r'^\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}$'),
             concat(regexp_replace(c, r'^(\d{4})[-/](\d{2})[-/](\d{2})[T ](\d{2}:\d{2})$', '$1-$2-$3 $4'), lit(':00')))
        .when(c.rlike(r'^\d{4}[-/]\d{2}[-/]\d{2}$'),
             concat(regexp_replace(c, r'^(\d{4})[-/](\d{2})[-/](\d{2})$', '$1-$2-$3'), lit(' 00:00:00')))
        .when(c.rlike(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'),
             regexp_replace(c, r'^(\d{2})/(\d{2})/(\d{4}) (\d{2}:\d{2}:\d{2})$', '$3-$2-$1 $4'))
        .when(c.rlike(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'),
             concat(regexp_replace(c, r'^(\d{2})/(\d{2})/(\d{4}) (\d{2}:\d{2})$', '$3-$2-$1 $4'), lit(':00')))
        .when(c.rlike(r'^\d{2}/\d{2}/\d{4}$'),
             concat(regexp_replace(c, r'^(\d{2})/(\d{2})/(\d{4})$', '$3-$2-$1'), lit(' 00:00:00')))
        .otherwise(lit(None).cast('string'))
    )
    return to_timestamp(normalized, 'yyyy-MM-dd HH:mm:ss')

# COMMAND ----------

UF_MAP = {
    "sao paulo": "SP", "são paulo": "SP", "s. paulo": "SP",
    "rio de janeiro": "RJ", "minas gerais": "MG",
    "bahia": "BA", "parana": "PR", "paraná": "PR",
    "rio grande do sul": "RS", "santa catarina": "SC",
    "s. catarina": "SC", "sta catarina": "SC",
    "goias": "GO", "goiás": "GO", "mato grosso": "MT",
    "mato grosso do sul": "MS", "espirito santo": "ES",
    "espírito santo": "ES", "pernambuco": "PE",
    "ceara": "CE", "ceará": "CE", "amazonas": "AM",
    "para": "PA", "pará": "PA", "maranhao": "MA",
    "maranhão": "MA", "piaui": "PI", "piauí": "PI",
    "alagoas": "AL", "sergipe": "SE", "rondonia": "RO",
    "rondônia": "RO", "tocantins": "TO", "acre": "AC",
    "amapa": "AP", "amapá": "AP", "roraima": "RR",
    "distrito federal": "DF",
}


def normalize_uf_column(df: DataFrame, col_name: str) -> DataFrame:
    """Normaliza coluna de estado: nome por extenso → sigla UF."""
    from pyspark.sql.functions import create_map
    from itertools import chain
    mapping_expr = create_map([lit(x) for x in chain(*UF_MAP.items())])
    return df.withColumn(
        col_name,
        coalesce(
            when(upper(trim(col(col_name))).rlike("^[A-Z]{2}$"), upper(trim(col(col_name)))),
            mapping_expr[lower(trim(col(col_name)))],
            col(col_name)
        )
    )


# COMMAND ----------

# DBTITLE 1,initialize_bronze_context
def initialize_bronze_context(container_source: str, nome_arquivo: str, file_name_saida: str):
    """Inicializa paths e parâmetros de configuração para notebooks Bronze com AutoLoader.

    Lê arquivos Parquet da LANDING_PATH (já convertidos pela camada landing).
    Usa CHECKPOINT_BASE e SCHEMA_BASE (DBFS) para garantir persistência em execuções
    serverless e multi-task jobs — /tmp/ é efêmero por task e não deve ser usado.
    """
    # Lê da landing zone (Parquet otimizado), não mais dos arquivos brutos
    caminho_leitura  = f"{LANDING_PATH}/{container_source}/"
    caminho_gravacao = f"/dbfs/mnt/delta/{BRONZE_SCHEMA}/{file_name_saida}"
    schemalocal      = f"{SCHEMA_BASE}/{container_source}/{file_name_saida}"
    checkpoint       = f"{CHECKPOINT_BASE}/{BRONZE_SCHEMA}/{container_source}/{file_name_saida}"
    nome_tabela      = f"{BRONZE}.{file_name_saida}"
    table_id         = file_name_saida.replace("-", "_").replace(".", "_")
    merge_condition  = "target.dsRefChave = source.dsRefChave"
    var_renomear     = []
    var_merge        = {"key": "dsRefChave"}
    print(f"[initialize_bronze_context] Leitura     : {caminho_leitura} (Parquet)")
    print(f"[initialize_bronze_context] Gravação    : {caminho_gravacao}")
    print(f"[initialize_bronze_context] Tabela      : {nome_tabela}")
    print(f"[initialize_bronze_context] Checkpoint  : {checkpoint}")
    print(f"[initialize_bronze_context] Schema loc  : {schemalocal}")
    return var_renomear, var_merge, table_id, merge_condition, caminho_leitura, caminho_gravacao, schemalocal, checkpoint, nome_tabela


def upsert_delta_live(nome_tabela, caminho_gravacao, merge_condition, table_id, order_key='rastreamento_source'):
    """Retorna função foreachBatch para upsert idempotente em tabela Delta via streaming."""
    import pyspark.sql.functions as _F
    from pyspark.sql import Window as _W

    def inner(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        w = _W.partitionBy("dsRefChave").orderBy(_F.col(order_key).desc())
        deduped = (batch_df
                   .withColumn("_row_rank", _F.row_number().over(w))
                   .filter(_F.col("_row_rank") == 1)
                   .drop("_row_rank"))
        view_name = f"_batch_{table_id}"
        deduped.createOrReplaceTempView(view_name)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {nome_tabela}
            USING DELTA
            AS SELECT * FROM {view_name} WHERE 1=0
        """)
        spark.sql(f"""
            MERGE INTO {nome_tabela} AS target
            USING {view_name} AS source
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    return inner

# COMMAND ----------

def process_data_load(df, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela):
    """Carga completa (overwrite) na primeira execução ou quando tipo_carga='full'. Registra na pipeline_controller."""
    import time
    _inicio = time.time()
    _erro   = ''
    _linhas = 0
    try:
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(nome_gravacao_tabela))
        _linhas = spark.table(nome_gravacao_tabela).count()
        print(f"  [FULL LOAD] {nome_gravacao_tabela} → {_linhas:,} linhas gravadas")
        _status = 'SUCESSO'
    except Exception as _e:
        _erro   = str(_e)
        _status = 'FALHA'
        print(f"  [FULL LOAD ERRO] {nome_gravacao_tabela} : {_erro}")
        raise
    finally:
        _duracao = round(time.time() - _inicio, 2)
        log_table_execution(nome_gravacao_tabela, _duracao, _status, _linhas, _erro)
    return _linhas


# COMMAND ----------

def log_table_execution(tabela: str, duracao_segundos: float = 0.0,
                        status: str = 'SUCESSO', linhas: int = 0, erro: str = '') -> None:
    """Registra resultado de execução de uma tabela na pipeline_controller.

    Fire-and-forget: erros de log não interrompem o pipeline.

    Parâmetros:
        tabela            : nome completo da tabela (catalog.schema.nome)
        duracao_segundos  : tempo de execução em segundos
        status            : 'SUCESSO' ou 'FALHA'
        linhas            : quantidade de linhas processadas
        erro              : mensagem de erro (vazio em caso de sucesso)
    """
    try:
        from pyspark.sql import Row
        from datetime import datetime
        camada = 'gold' if '.gold.' in tabela else \
                 'silver' if '.silver.' in tabela else \
                 'bronze' if '.bronze.' in tabela else 'landing'
        _now = datetime.now()
        row = Row(
            tabela_nome        = tabela,
            camada             = camada,
            status_execucao    = status,
            linhas_processadas = int(linhas),
            data_execucao      = _now,
            ultima_atualizacao = _now,
            duracao_segundos   = float(duracao_segundos),
            mensagem_erro      = erro[:2000] if erro else '',
            pipeline_versao    = PIPELINE_VERSION,
        )
        df_log = spark.createDataFrame([row])
        (df_log.write
               .format("delta")
               .mode("append")
               .option("mergeSchema", "true")
               .saveAsTable(CONTROL_TABLE))
        print(f"  [MONITOR] {tabela} | {status} | {linhas:,} linhas | {duracao_segundos:.1f}s")
    except Exception as _e:
        # Monitoramento não deve interromper pipeline
        print(f"  [MONITOR WARN] Falha ao registrar log: {_e}")

# COMMAND ----------

print("✓ Functions carregadas | Parse, Normalize, AutoLoader, Write, Monitoring")
