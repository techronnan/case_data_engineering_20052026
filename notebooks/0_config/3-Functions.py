# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 3-Functions
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Funções utilitárias: parse, normalização, ingestão, carga, certificação e monitoramento |
# MAGIC | Executado Via | `0-Init` — não executar diretamente |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Renomeação de todas as funções para padrão 3 palavras snake_case. |
# MAGIC | 21/05/2026 | Ronnan           | `initialize_bronze_context` usa CHECKPOINT_BASE/SCHEMA_BASE (serverless). Adicionado `log_table_execution`. `process_data_load` registra automaticamente na pipeline_controller. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de Data e Timestamp

# COMMAND ----------

def parse_date_multi_format(col_name: str):
    """Normaliza datas em múltiplos formatos para DateType."""
    return coalesce(
        to_date(col(col_name), "yyyy-MM-dd"),
        to_date(col(col_name), "yyyy/MM/dd"),
        to_date(col(col_name), "dd/MM/yyyy"),
        to_date(col(col_name), "MM/dd/yyyy"),
    )


def parse_timestamp_multi_format(col_name: str):
    """Normaliza timestamps em múltiplos formatos."""
    return coalesce(
        to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss"),
        to_timestamp(col(col_name), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col(col_name), "dd/MM/yyyy HH:mm"),
        to_timestamp(col(col_name), "yyyy/MM/dd"),
        to_date(col(col_name), "yyyy-MM-dd").cast(TimestampType()),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de Normalização

# COMMAND ----------

def normalize_decimal_value(col_name: str):
    """Converte número com vírgula decimal (BR) para DoubleType."""
    return regexp_replace(col(col_name), ",", ".").cast(DoubleType())


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


STATUS_PEDIDO_MAP = {
    "FATURADO": "FATURADO", "faturado": "FATURADO",
    "CANCELADO": "CANCELADO", "cancelado": "CANCELADO",
    "ENTREGUE": "ENTREGUE",  "entregue": "ENTREGUE",
    "EM_SEPARACAO": "EM_SEPARACAO", "em_separacao": "EM_SEPARACAO",
    "em separacao": "EM_SEPARACAO", "EM SEPARACAO": "EM_SEPARACAO",
}


def normalize_status_pedido(col_name: str):
    """Mapeia status de pedido para vocabulário canônico."""
    from pyspark.sql.functions import create_map
    from itertools import chain
    mapping_expr = create_map([lit(x) for x in chain(*STATUS_PEDIDO_MAP.items())])
    return coalesce(
        mapping_expr[col(col_name)],
        when(col(col_name).isNull(), lit("INDEFINIDO")),
        lit("INDEFINIDO")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de Ingestão — Metadados Bronze

# COMMAND ----------

def add_ingestion_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Adiciona colunas de rastreabilidade (_source_file, _ingested_at) para Bronze."""
    return (
        df
        .withColumn("_source_file", lit(source_file))
        .withColumn("_ingested_at", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estratégias de Carga

# COMMAND ----------

def write_full_table(df: DataFrame, table: str, overwrite_schema: bool = True) -> int:
    """Estratégia FULL LOAD — substitui toda a tabela (overwrite)."""
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", str(overwrite_schema).lower())
       .saveAsTable(table))
    count = spark.table(table).count()
    print(f"  [FULL] {table} → {count:,} linhas gravadas")
    return count


def write_delta_merge(df: DataFrame, table: str, pk_cols: list,
                      temp_view: str = "_src_updates") -> int:
    """Estratégia DELTA — MERGE INTO (upsert) por chave(s) primária(s)."""
    df.createOrReplaceTempView(temp_view)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING DELTA
        AS SELECT * FROM {temp_view} WHERE 1 = 0
    """)
    on_clause = " AND ".join([f"tgt.`{c}` = src.`{c}`" for c in pk_cols])
    result = spark.sql(f"""
        MERGE INTO {table} AS tgt
        USING {temp_view}  AS src
        ON {on_clause}
        WHEN MATCHED     THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    count = spark.table(table).count()
    print(f"  [DELTA] {table} → {count:,} linhas totais | MERGE por {pk_cols}")
    return count


# Alias retrocompatível
write_delta = write_full_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Certificação de Qualidade

# COMMAND ----------

def certify_table_quality(table: str, pk_cols: list, min_rows: int = 1) -> int:
    """Certifica qualidade básica após carga: contagem mínima, PKs sem nulos, sem duplicatas."""
    df    = spark.table(table)
    total = df.count()
    errors = []

    if total < min_rows:
        errors.append(f"Linhas insuficientes: {total:,} < mínimo {min_rows:,}")

    for pk in pk_cols:
        nulls = df.filter(col(pk).isNull()).count()
        if nulls > 0:
            errors.append(f"PK '{pk}' com {nulls:,} valor(es) nulo(s)")

    if pk_cols:
        dupes = total - df.dropDuplicates(pk_cols).count()
        if dupes > 0:
            errors.append(f"Duplicatas por {pk_cols}: {dupes:,} registros")

    if errors:
        print(f"\n  [CERT FALHOU] {table}")
        for e in errors:
            print(f"    ✗ {e}")
        raise AssertionError(f"Certificação falhou em {table}: {len(errors)} problema(s)")

    print(f"  [CERT OK] {table} | {total:,} linhas | PKs {pk_cols} válidas")
    return total

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de Contexto Bronze (AutoLoader)

# COMMAND ----------

def initialize_bronze_context(container_source: str, nome_arquivo: str, file_name_saida: str):
    """Inicializa paths e parâmetros de configuração para notebooks Bronze com AutoLoader.

    Usa CHECKPOINT_BASE e SCHEMA_BASE (DBFS) para garantir persistência em execuções
    serverless e multi-task jobs — /tmp/ é efêmero por task e não deve ser usado.
    """
    caminho_leitura  = f"{SOURCES_PATH}/{container_source}/"
    caminho_gravacao = f"/delta/{BRONZE_SCHEMA}/{file_name_saida}"
    schemalocal      = f"{SCHEMA_BASE}/{container_source}/{file_name_saida}"
    checkpoint       = f"{CHECKPOINT_BASE}/{BRONZE_SCHEMA}/{container_source}/{file_name_saida}"
    nome_tabela      = f"{BRONZE}.{file_name_saida}"
    table_id         = file_name_saida.replace("-", "_").replace(".", "_")
    merge_condition  = "target.dsRefChave = source.dsRefChave"
    var_renomear     = []
    var_merge        = {"key": "dsRefChave"}
    print(f"[initialize_bronze_context] Leitura     : {caminho_leitura}")
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
        if batch_df.rdd.isEmpty():
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
            LOCATION '{caminho_gravacao}'
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

# MAGIC %md
# MAGIC ## Funções de Carga Silver / Gold

# COMMAND ----------

def process_data_load(df, tipo_carga, nome_gravacao_tabela, caminho_gravacao_tabela, chave_clusterby, chave_upsert):
    """Carga completa (overwrite) usada na primeira execução ou quando tipo_carga='full'.

    Registra automaticamente resultado na pipeline_controller após a gravação.
    """
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


def drop_v2checkpoint_feature(table_name: str):
    """Remove v2Checkpoint feature da tabela Delta se existir (compatibilidade entre runtimes)."""
    try:
        spark.sql(f"ALTER TABLE {table_name} DROP FEATURE v2Checkpoint IF EXISTS")
    except Exception:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoramento — log_table_execution

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
        camada = 'gold' if '.gold.' in tabela else \
                 'silver' if '.silver.' in tabela else \
                 'bronze' if '.bronze.' in tabela else 'landing'
        row = Row(
            tabela_nome        = tabela,
            camada             = camada,
            status_execucao    = status,
            linhas_processadas = int(linhas),
            data_execucao      = current_timestamp(),
            ultima_atualizacao = current_timestamp(),
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

print("[Functions] Carregadas:")
print("  parse_date_multi_format, parse_timestamp_multi_format")
print("  normalize_decimal_value, normalize_uf_column, normalize_status_pedido")
print("  add_ingestion_metadata")
print("  write_full_table, write_delta_merge, write_delta (alias)")
print("  certify_table_quality")
print("  initialize_bronze_context, upsert_delta_live")
print("  process_data_load, drop_v2checkpoint_feature")
print("  log_table_execution")
