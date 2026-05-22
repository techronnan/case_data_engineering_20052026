# Databricks notebook source
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Landing — Upload Sources TXT
# MAGIC
# MAGIC ## Objetivo
# MAGIC Processar arquivos TXT (pipe-delimited) da pasta sources/ e converter para Parquet na landing zone.
# MAGIC
# MAGIC ## Arquivos Processados
# MAGIC * legado_regioes_pipe.txt (sep=|)
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

print("Validando presença dos arquivos TXT...")
try:
    present = set(os.listdir(SOURCES_PATH))
    txt_files = [f for f in EXPECTED_FILES if f.endswith('.txt')]
    missing = []
    
    for fname in txt_files:
        status = "OK" if fname in present else "FALTANDO"
        if status != "OK":
            missing.append(fname)
            _erros_landing.append(fname)
        print(f"  [{status}]  {fname}")
    
    if missing:
        print(f"\n  {len(missing)} arquivo(s) TXT faltando.")
    else:
        print(f"\n  Todos os {len(txt_files)} arquivos TXT presentes.")
        
except Exception as e:
    _erros_landing.append(str(e))
    print(f"Erro ao validar arquivos: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento e Conversão

# COMMAND ----------

# DBTITLE 1,Processamento TXT
_erros_conversao = []
_total_registros = 0

_now = datetime.now()
_ano = _now.strftime("%Y")
_mes = _now.strftime("%m")
_timestamp = _now.strftime("%Y%m%d%H%M%S")

print(f"Convertendo arquivos TXT para Parquet... [{_timestamp}]\n")

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "txt":
        continue
        
    name = config["name"]
    fname = config["file"]
    opts = config.get("options", {})
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    
    try:
        print(f"  [{sistema}] Lendo {fname} (txt pipe-delimited)...")
        
        # Ler TXT com separador pipe
        sep = opts.get("sep", "|")
        with open(src_path, newline="", encoding="utf-8-sig") as _f:
            rows = list(csv.DictReader(_f, delimiter=sep))
        
        # Criar DataFrame Spark
        df = spark.createDataFrame(rows)
        count = df.count()
        _total_registros += count
        
        print(f"  [{sistema}] Gravando {count:,} registros → {dst_file}")
        
        # Gravar como Parquet
        tmp_dir = f"{dst_dir}/_tmp_{_timestamp}"
        dbutils.fs.mkdirs(dst_dir)
        df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)
        
        # Renomear arquivo único
        part = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".parquet")][0]
        dbutils.fs.mv(part, dst_file)
        dbutils.fs.rm(tmp_dir, recurse=True)
        
        print(f"  [OK] {fname} convertido com sucesso\n")
        
    except Exception as e:
        _erros_conversao.append(f"{fname}: {str(e)}")
        print(f"  [ERRO] {fname}: {e}\n")

print(f"Conversão TXT concluída. Total: {_total_registros:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoramento

# COMMAND ----------

# DBTITLE 1,Log de Execução
_duracao_landing = round(time.time() - _inicio_landing, 2)
_todos_erros = _erros_landing + _erros_conversao
_status_landing = 'FALHA' if _todos_erros else 'SUCESSO'
_msg_erro = ' | '.join(_todos_erros) if _todos_erros else ''

try:
    log_table_execution(
        tabela=f'{CATALOG}.monitoring.landing_sources_txt',
        duracao_segundos=_duracao_landing,
        status=_status_landing,
        linhas=_total_registros,
        erro=_msg_erro,
    )
except Exception as e:
    _status_landing = 'FALHA'
    _msg_erro += f' | Falha ao registrar log: {e}'

print(f"\n[Landing TXT] Status: {_status_landing} | {_duracao_landing:.1f}s | {_total_registros:,} registros")
if _msg_erro:
    print(f"Erros: {_msg_erro}")

# COMMAND ----------

print("\nVerificando saída dos arquivos Parquet (3 linhas cada):\n")
for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "txt":
        continue

    name = config["name"]
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    try:
        df_out = spark.read.parquet(dst_file)
        print(f"\n[{sistema}] {dst_file} (3 linhas):")
        display(df_out.limit(3))
    except Exception as e:
        print(f"[ERRO] Falha ao ler {dst_file}: {e}")
