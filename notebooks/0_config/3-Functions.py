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
# MAGIC | Finalidade | Funções utilitárias: parse, normalização, estratégias de carga, certificação |
# MAGIC | Executado Via | `4-Config` — não executar diretamente |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de Data e Timestamp

# COMMAND ----------

def parse_date_multi(col_name: str):
    """Normaliza datas em múltiplos formatos para DateType."""
    return coalesce(
        to_date(col(col_name), "yyyy-MM-dd"),
        to_date(col(col_name), "yyyy/MM/dd"),
        to_date(col(col_name), "dd/MM/yyyy"),
        to_date(col(col_name), "MM/dd/yyyy"),
    )


def parse_timestamp_multi(col_name: str):
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

def normalize_decimal(col_name: str):
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


def normalize_uf(df: DataFrame, col_name: str) -> DataFrame:
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

def write_full(df: DataFrame, table: str, overwrite_schema: bool = True) -> int:
    """
    Estratégia FULL LOAD — substitui toda a tabela.

    Uso recomendado:
    - Bronze: sempre (ingere bruto completo)
    - Gold Dimensões: sempre (rebuild completo a partir do Silver)
    - Gold Fatos pequenos: opcional

    Parâmetros
    ----------
    df              : DataFrame a gravar
    table           : nome 3-part Unity Catalog (catalog.schema.table)
    overwrite_schema: permite alterar schema na sobrescrita (padrão True)
    """
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
    """
    Estratégia DELTA — MERGE INTO (upsert) por chave(s) primária(s).

    Uso recomendado:
    - Silver: incremental sobre dados já existentes
    - Gold Fatos: permite reprocessamento sem duplicar linhas

    Parâmetros
    ----------
    df         : DataFrame com registros novos/atualizados
    table      : nome 3-part Unity Catalog (catalog.schema.table)
    pk_cols    : lista de colunas que formam a chave de MERGE
    temp_view  : nome da view temporária usada no SQL de MERGE
    """
    df.createOrReplaceTempView(temp_view)

    # Cria tabela vazia se não existir (preserva schema em runs subsequentes)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Certificação de Qualidade

# COMMAND ----------

def certify(table: str, pk_cols: list, min_rows: int = 1) -> int:
    """
    Certifica qualidade básica após a carga:
    - Contagem mínima de linhas
    - PKs sem nulos
    - Ausência de duplicatas por PK

    Lança AssertionError se alguma verificação falhar.
    """
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

print("[Functions] Carregadas:")
print("  parse_date_multi, parse_timestamp_multi")
print("  normalize_decimal, normalize_uf, normalize_status_pedido")
print("  add_ingestion_metadata")
print("  write_full, write_delta_merge")
print("  certify")
