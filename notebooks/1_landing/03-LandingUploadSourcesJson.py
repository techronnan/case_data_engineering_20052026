# Databricks notebook source
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Landing — Upload Sources JSON
# MAGIC
# MAGIC ## Objetivo
# MAGIC Processar arquivos JSON (multiline) da pasta sources/ e converter para Parquet na landing zone.
# MAGIC
# MAGIC ## Arquivos Processados
# MAGIC * cadastro_produtos_api_dump.json (multiline)
# MAGIC * logistica_entregas.json (multiline)
# MAGIC
# MAGIC ## Estrutura de Saída
# MAGIC `systems/{nome_arquivo}/{ano}/{mes}/{nome_arquivo}_YYYYMMDDHHMMSS.parquet`

# COMMAND ----------

# DBTITLE 1,Inicialização
# MAGIC %run ../0_config/0-Init

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos Arquivos

# COMMAND ----------

# DBTITLE 1,Validação
_inicio_landing = time.time()
_erros_landing = []

print("Validando presença dos arquivos JSON...")
try:
    present = set(os.listdir(SOURCES_PATH))
    json_files = [f for f in EXPECTED_FILES if f.endswith('.json')]
    missing = []
    
    for fname in json_files:
        status = "OK" if fname in present else "FALTANDO"
        if status != "OK":
            missing.append(fname)
            _erros_landing.append(fname)
        print(f"  [{status}]  {fname}")
    
    if missing:
        print(f"\n  {len(missing)} arquivo(s) JSON faltando.")
    else:
        print(f"\n  Todos os {len(json_files)} arquivos JSON presentes.")
        
except Exception as e:
    _erros_landing.append(str(e))
    print(f"Erro ao validar arquivos: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento e Conversão

# COMMAND ----------

# DBTITLE 1,Processamento JSON
_erros_conversao = []
_total_registros = 0

_now = datetime.now()
_ano = _now.strftime("%Y")
_mes = _now.strftime("%m")
_timestamp = _now.strftime("%Y%m%d%H%M%S")

print(f"Convertendo arquivos JSON para Parquet... [{_timestamp}]\n")

def normalize_produtos(row):
    product = row.get("product", {})
    pricing = row.get("pricing", {})
    attributes = row.get("attributes", {})
    # Corrige list_price para float, se possível
    list_price = pricing.get("list_price")
    try:
        list_price = float(list_price) if list_price is not None else None
    except Exception:
        list_price = None
    return {
        "product_id": product.get("product_id"),
        "product_name": product.get("name"),
        "category": product.get("category"),
        "subcategory": product.get("subcategory"),
        "product_status": product.get("status"),
        "list_price": list_price,
        "currency": pricing.get("currency"),
        "family": attributes.get("family"),
        "tags": attributes.get("tags"),
        "updated_at": row.get("updated_at")
    }

def normalize_logistica(row):
    required_fields = [
        "carrier", "cost", "delivery_id", "delivery_status",
        "destination", "order_ref", "timestamps"
    ]
    out = {field: row.get(field, None) for field in required_fields}
    carrier = out.get("carrier", {})
    out["carrier_name"] = carrier.get("name") if isinstance(carrier, dict) else None
    out["carrier_mode"] = carrier.get("mode") if isinstance(carrier, dict) else None
    destination = out.get("destination", {})
    out["destination_state"] = destination.get("state") if isinstance(destination, dict) else None
    out["destination_city"] = destination.get("city") if isinstance(destination, dict) else None
    timestamps = out.get("timestamps", {})
    out["shipped_at"] = timestamps.get("shipped_at") if isinstance(timestamps, dict) else None
    out["delivered_at"] = timestamps.get("delivered_at") if isinstance(timestamps, dict) else None
    return out

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "json":
        continue
        
    name = config["name"]
    fname = config["file"]
    opts = config.get("options", {})
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    
    try:
        print(f"  [{sistema}] Lendo {fname} (json multiline)...")
        
        with open(src_path, "r", encoding="utf-8") as _f:
            data = json.load(_f)
            rows = data if isinstance(data, list) else [data]
        
        # Normalização dos dados
        if fname == "cadastro_produtos_api_dump.json":
            rows = [normalize_produtos(r) for r in rows]
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
            schema = StructType([
                StructField("product_id", StringType(), True),
                StructField("product_name", StringType(), True),
                StructField("category", StringType(), True),
                StructField("subcategory", StringType(), True),
                StructField("product_status", StringType(), True),
                StructField("list_price", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("family", StringType(), True),
                StructField("tags", ArrayType(StringType()), True),
                StructField("updated_at", StringType(), True),
            ])
            df = spark.createDataFrame(rows, schema=schema)
        elif fname == "logistica_entregas.json":
            rows = [normalize_logistica(r) for r in rows]
            df = spark.createDataFrame(rows)
        else:
            df = spark.createDataFrame(rows)
        
        count = df.count()
        _total_registros += count
        
        print(f"  [{sistema}] Gravando {count:,} registros → {dst_file}")
        
        tmp_dir = f"{dst_dir}/_tmp_{_timestamp}"
        dbutils.fs.mkdirs(dst_dir)
        df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)
        
        part = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".parquet")][0]
        dbutils.fs.mv(part, dst_file)
        dbutils.fs.rm(tmp_dir, recurse=True)
        
        print(f"  [OK] {fname} convertido com sucesso\n")
        
    except Exception as e:
        _erros_conversao.append(f"{fname}: {str(e)}")
        print(f"  [ERRO] {fname}: {e}\n")

print(f"Conversão JSON concluída. Total: {_total_registros:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoramento

# COMMAND ----------

# DBTITLE 1,Log de Execução
_duracao_landing = round(time.time() - _inicio_landing, 2)
_todos_erros = _erros_landing + _erros_conversao
_status_landing = 'FALHA' if _todos_erros else 'SUCESSO'
_msg_erro = ' | '.join(_todos_erros) if _todos_erros else ''

log_table_execution(
    tabela=f'{CATALOG}.monitoring.landing_sources_json',
    duracao_segundos=_duracao_landing,
    status=_status_landing,
    linhas=_total_registros,
    erro=_msg_erro,
)

print(f"\n[Landing JSON] Status: {_status_landing} | {_duracao_landing:.1f}s | {_total_registros:,} registros")
if _todos_erros:
    print(f"Erros: {_msg_erro}")

# COMMAND ----------

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "json":
        continue

    name = config["name"]
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    
    try:
        print(f"\n[sample] {dst_file}")
        df_sample = spark.read.parquet(dst_file)
        display(df_sample.limit(3))
    except Exception as e:
        print(f"[ERRO sample] {dst_file}: {e}")

# COMMAND ----------


