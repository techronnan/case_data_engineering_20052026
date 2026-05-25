# Databricks notebook source
# DBTITLE 1,LandingUploadSourcesCsv

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

_inicio = time.time()
_erros_val = []

present = set(os.listdir(SOURCES_PATH))
for fname in [f for f in EXPECTED_FILES if f.endswith('.csv')]:
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
    if config.get("format") != "csv":
        continue
    name  = config["name"]
    fname = config["file"]
    sep   = config.get("options", {}).get("sep", ",")
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir  = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    try:
        with open(src_path, newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f, delimiter=sep))
        df    = spark.createDataFrame(rows)
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
    tabela=f'{CATALOG}.monitoring.landing_sources_csv',
    duracao_segundos=_duracao,
    status=_status,
    linhas=_total,
    erro=_msg_erro,
)
print(f"[Landing CSV] {_status} | {_duracao:.1f}s | {_total:,} registros")
if _todos_erros:
    raise Exception(f"Erros na landing CSV: {_msg_erro}")

# COMMAND ----------

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "csv":
        continue
    name = config["name"]
    dst_file = f"{LANDING_PATH}/{name}/{_ano}/{_mes}/{name}_{_timestamp}.parquet"
    try:
        display(spark.read.parquet(dst_file).limit(3))
    except Exception as e:
        print(f"[ERRO sample] {dst_file}: {e}")
