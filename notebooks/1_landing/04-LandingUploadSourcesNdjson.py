# Databricks notebook source
# DBTITLE 1,LandingUploadSourcesNdjson

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

_inicio = time.time()
_erros_val = []

present = set(os.listdir(SOURCES_PATH))
for fname in [f for f in EXPECTED_FILES if f.endswith('.ndjson')]:
    ok = fname in present
    if not ok:
        _erros_val.append(fname)
    print(f"  [{'OK' if ok else 'FALTANDO'}]  {fname}")

# COMMAND ----------

_erros_conv = []
_total = 0
_now = datetime.now()
_ano, _mes, _timestamp = _now.strftime("%Y"), _now.strftime("%m"), _now.strftime("%Y%m%d%H%M%S")

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "ndjson":
        continue
    name  = config["name"]
    fname = config["file"]
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir  = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    try:
        rows = []
        with open(src_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))

        if not rows:
            raise ValueError(f"{fname}: arquivo vazio ou inválido")

        df = spark.createDataFrame(rows)

        if "metadata" in df.columns:
            meta_type = df.schema["metadata"].dataType
            if isinstance(meta_type, StructType):
                for field in meta_type.fields:
                    df = df.withColumn(f"metadata_{field.name}", col("metadata").getItem(field.name))
            elif isinstance(meta_type, MapType):
                keys_rows = df.select(map_keys(col("metadata"))).distinct().collect()
                flat_keys = {k for row in keys_rows if row[0] for k in row[0]}
                for k in flat_keys:
                    df = df.withColumn(f"metadata_{k}", col("metadata").getItem(k))
            df = df.drop("metadata")

        count = df.count()
        _total += count
        tmp = f"{dst_dir}/_tmp_{_timestamp}"
        try: dbutils.fs.rm(f"{LANDING_PATH}/{name}", recurse=True)
        except Exception: pass
        dbutils.fs.mkdirs(dst_dir)
        df.coalesce(1).write.mode("overwrite").parquet(tmp)
        part_files = [f.path for f in dbutils.fs.ls(tmp) if f.name.endswith(".parquet")]
        if not part_files:
            raise ValueError(f"{fname}: nenhum parquet gerado")
        dbutils.fs.mv(part_files[0], dst_file)
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
    tabela=f'{CATALOG}.monitoring.landing_sources_ndjson',
    duracao_segundos=_duracao,
    status=_status,
    linhas=_total,
    erro=_msg_erro,
)
print(f"[Landing NDJSON] {_status} | {_duracao:.1f}s | {_total:,} registros")
if _todos_erros:
    raise Exception(f"Erros na landing NDJSON: {_msg_erro}")

# COMMAND ----------

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "ndjson":
        continue
    name = config["name"]
    dst_file = f"{LANDING_PATH}/{name}/{_ano}/{_mes}/{name}_{_timestamp}.parquet"
    try:
        display(spark.read.parquet(dst_file).limit(3))
    except Exception as e:
        print(f"[ERRO sample] {dst_file}: {e}")
