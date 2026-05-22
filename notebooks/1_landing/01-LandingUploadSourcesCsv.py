# Databricks notebook source
# DBTITLE 1,Landing Upload Sources - CSV Only

# MAGIC %run ../0_config/0-Init

_inicio_landing = time.time()
_erros_landing = []

try:
    present = set(os.listdir(SOURCES_PATH))
    missing = []
    for fname in EXPECTED_FILES:
        if fname not in present:
            missing.append(fname)
            _erros_landing.append(fname)
        print(f"  [{'OK' if fname in present else 'FALTANDO'}]  {fname}")
    if missing:
        print(f"  {len(missing)} arquivo(s) faltando — execute o upload antes de continuar.")
    else:
        print(f"  Todos os {len(EXPECTED_FILES)} arquivos presentes. Prosseguindo com organização.")
except Exception as e:
    _erros_landing.append(str(e))
    print(f"Erro ao acessar {SOURCES_PATH}: {e}")

_erros_conversao = []
_total_registros = 0
_now = datetime.now()
_ano = _now.strftime("%Y")
_mes = _now.strftime("%m")
_timestamp = _now.strftime("%Y%m%d%H%M%S")

print(f"Convertendo arquivos CSV para Parquet... [{_timestamp}]\n")

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "csv":
        continue
    name = config["name"]
    fname = config["file"]
    opts = config.get("options", {})
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    try:
        print(f"  [{sistema}] Lendo {fname} (csv)...")
        sep = opts.get("sep", ",")
        with open(src_path, newline="", encoding="utf-8-sig") as _f:
            rows = list(csv.DictReader(_f, delimiter=sep))
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
    except Exception as e:
        _erros_conversao.append(f"{fname}: {str(e)}")
        print(f"  [ERRO] {fname}: {e}\n")

print(f"\nConversão CSV concluída. Total de registros processados: {_total_registros:,}")

# Registro de monitoramento
_duracao_landing = round(time.time() - _inicio_landing, 2)
_todos_erros = _erros_landing + _erros_conversao
_status_landing = 'FALHA' if _todos_erros else 'SUCESSO'
_msg_erro = ' | '.join(_todos_erros) if _todos_erros else ''

log_table_execution(
    tabela=f'{CATALOG}.monitoring.landing_sources_csv',
    duracao_segundos=_duracao_landing,
    status=_status_landing,
    linhas=_total_registros,
    erro=_msg_erro,
)

print(f"\n[Landing CSV] Status: {_status_landing} | {_duracao_landing:.1f}s | {_total_registros:,} registros")
