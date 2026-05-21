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
# MAGIC | Finalidade | Funções utilitárias reutilizáveis por todos os notebooks |
# MAGIC | Executado Via | `4-Config` — não executar diretamente |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de Data

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
    "sao paulo": "SP", "são paulo": "SP", "s. paulo": "SP", "s paulo": "SP",
    "rio de janeiro": "RJ", "minas gerais": "MG",
    "bahia": "BA", "parana": "PR", "paraná": "PR",
    "rio grande do sul": "RS", "santa catarina": "SC",
    "s. catarina": "SC", "sta catarina": "SC",
    "goias": "GO", "goiás": "GO", "mato grosso": "MT",
    "mato grosso do sul": "MS", "espirito santo": "ES",
    "espírito santo": "ES", "pernambuco": "PE", "ceara": "CE",
    "ceará": "CE", "amazonas": "AM", "para": "PA", "pará": "PA",
    "maranhao": "MA", "maranhão": "MA", "piaui": "PI", "piauí": "PI",
    "alagoas": "AL", "sergipe": "SE", "rondonia": "RO",
    "rondônia": "RO", "tocantins": "TO", "acre": "AC",
    "amapa": "AP", "amapá": "AP", "roraima": "RR",
    "distrito federal": "DF",
}


def normalize_uf(df: DataFrame, col_name: str) -> DataFrame:
    """Normaliza coluna de estado: nome completo → sigla UF."""
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
    "ENTREGUE": "ENTREGUE", "entregue": "ENTREGUE",
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
# MAGIC ## Funções de Ingestão e Escrita

# COMMAND ----------

def add_ingestion_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Adiciona colunas de rastreabilidade em tabelas Bronze."""
    return (
        df
        .withColumn("_source_file", lit(source_file))
        .withColumn("_ingested_at", current_timestamp())
    )


def write_delta(df: DataFrame, table: str, mode: str = "overwrite"):
    """Grava DataFrame como Delta Table no Unity Catalog."""
    df.write.format("delta").mode(mode).saveAsTable(table)
    count = spark.table(table).count()
    print(f"[OK] {table} — {count:,} linhas gravadas (mode={mode})")

# COMMAND ----------

print("[Functions] Funções carregadas: parse_date_multi, parse_timestamp_multi,")
print("            normalize_decimal, normalize_uf, normalize_status_pedido,")
print("            add_ingestion_metadata, write_delta")
