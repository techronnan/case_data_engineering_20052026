# Databricks notebook source
# Instalação de dependências externas
# openpyxl: necessário para leitura de arquivos Excel (.xlsx) na camada Landing
%pip install -q openpyxl

# COMMAND ----------

# ============================================================================
# IMPORTS CENTRALIZADOS DO PIPELINE
# ============================================================================

# --- PySpark Core ---
from pyspark.sql import DataFrame

# --- Funções de Manipulação de Colunas ---
from pyspark.sql.functions import (
    col, lit, concat, upper, lower, trim, regexp_replace, coalesce,  # String/Transformação
    to_date, to_timestamp, date_format,                              # Datas/Timestamps
    year, quarter, month, weekofyear, dayofweek, datediff,           # Extração de componentes de data
    when, isnull, isnan,                                             # Condicional/Nulos
    concat_ws, explode, sequence, from_json,                         # Arrays/JSON
    monotonically_increasing_id, current_timestamp, current_date,    # IDs/Metadados
    row_number, dense_rank,                                          # Window functions
    sum as _sum, count as _count, avg as _avg,                       # Agregações (alias para evitar conflito com Python built-ins)
    max as _max, min as _min
)

# --- Tipos de Dados ---
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DateType, TimestampType, BooleanType
)

# --- Window Functions ---
from pyspark.sql.window import Window

# --- Biblioteca Externa: Excel ---
import openpyxl

print("[Libs] Imports concluídos.")
