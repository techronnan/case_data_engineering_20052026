# Databricks notebook source
# DBTITLE 1,LandingUploadSourcesJson

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

_inicio = time.time()
_erros_val = []

present = set(os.listdir(SOURCES_PATH))
for fname in [f for f in EXPECTED_FILES if f.endswith('.json')]:
    ok = fname in present
    if not ok:
        _erros_val.append(fname)
    print(f"  [{'OK' if ok else 'FALTANDO'}]  {fname}")

# COMMAND ----------

def _normalize_produtos(row):
    product    = row.get("product", {})
    pricing    = row.get("pricing", {})
    attributes = row.get("attributes", {})
    list_price = pricing.get("list_price")
    try:
        list_price = float(list_price) if list_price is not None else None
    except Exception:
        list_price = None
    return {
        "product_id":     product.get("product_id"),
        "product_name":   product.get("name"),
        "category":       product.get("category"),
        "subcategory":    product.get("subcategory"),
        "product_status": product.get("status"),
        "list_price":     list_price,
        "currency":       pricing.get("currency"),
        "family":         attributes.get("family"),
        "tags":           attributes.get("tags"),
        "updated_at":     row.get("updated_at"),
    }

def _normalize_logistica(row):
    carrier     = row.get("carrier") or {}
    destination = row.get("destination") or {}
    timestamps  = row.get("timestamps") or {}
    return {
        "delivery_id":       row.get("delivery_id"),
        "order_ref":         row.get("order_ref"),
        "delivery_status":   row.get("delivery_status"),
        "cost":              row.get("cost"),
        "carrier_name":      carrier.get("name")      if isinstance(carrier, dict)      else None,
        "carrier_mode":      carrier.get("mode")      if isinstance(carrier, dict)      else None,
        "destination_state": destination.get("state") if isinstance(destination, dict)  else None,
        "destination_city":  destination.get("city")  if isinstance(destination, dict)  else None,
        "shipped_at":        timestamps.get("shipped_at")   if isinstance(timestamps, dict) else None,
        "delivered_at":      timestamps.get("delivered_at") if isinstance(timestamps, dict) else None,
    }

# COMMAND ----------

_erros_conv = []
_total = 0
_now = datetime.now()
_ano, _mes, _timestamp = _now.strftime("%Y"), _now.strftime("%m"), _now.strftime("%Y%m%d%H%M%S")

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "json":
        continue
    name  = config["name"]
    fname = config["file"]
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir  = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    try:
        with open(src_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        rows = data if isinstance(data, list) else [data]

        if fname == "cadastro_produtos_api_dump.json":
            from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
            rows = [_normalize_produtos(r) for r in rows]
            schema = StructType([
                StructField("product_id",     StringType(), True),
                StructField("product_name",   StringType(), True),
                StructField("category",       StringType(), True),
                StructField("subcategory",    StringType(), True),
                StructField("product_status", StringType(), True),
                StructField("list_price",     DoubleType(), True),
                StructField("currency",       StringType(), True),
                StructField("family",         StringType(), True),
                StructField("tags",           ArrayType(StringType()), True),
                StructField("updated_at",     StringType(), True),
            ])
            df = spark.createDataFrame(rows, schema=schema)
        elif fname == "logistica_entregas.json":
            rows = [_normalize_logistica(r) for r in rows]
            df = spark.createDataFrame(rows)
        else:
            df = spark.createDataFrame(rows)

        count = df.count()
        _total += count
        tmp = f"{dst_dir}/_tmp_{_timestamp}"
        try: dbutils.fs.rm(f"{LANDING_PATH}/{name}", recurse=True)
        except Exception: pass
        dbutils.fs.mkdirs(dst_dir)
        df.coalesce(1).write.mode("overwrite").parquet(tmp)
        part = [f.path for f in dbutils.fs.ls(tmp) if f.name.endswith(".parquet")][0]
        dbutils.fs.mv(part, dst_file)
        dbutils.fs.rm(tmp, recurse=True)
        print(f"  [OK] {fname} → {count:,} registros")
    except Exception as e:
        _erros_conv.append(f"{fname}: {e}")
        print(f"  [ERRO] {fname}: {e}")

print(f"\nTotal: {_total:,} registros")

# COMMAND ----------

_duracao = round(time.time() - _inicio, 2)
_todos_erros = _erros_val + _erros_conv
_status   = 'FALHA' if _todos_erros else 'SUCESSO'
_msg_erro = ' | '.join(str(e) for e in _todos_erros) if _todos_erros else ''

log_table_execution(
    tabela=f'{CATALOG}.monitoring.landing_sources_json',
    duracao_segundos=_duracao,
    status=_status,
    linhas=_total,
    erro=_msg_erro,
)
print(f"[Landing JSON] {_status} | {_duracao:.1f}s | {_total:,} registros")
if _todos_erros:
    raise Exception(f"Erros na landing JSON: {_msg_erro}")

# COMMAND ----------

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "json":
        continue
    name = config["name"]
    dst_file = f"{LANDING_PATH}/{name}/{_ano}/{_mes}/{name}_{_timestamp}.parquet"
    try:
        display(spark.read.parquet(dst_file).limit(3))
    except Exception as e:
        print(f"[ERRO sample] {dst_file}: {e}")
