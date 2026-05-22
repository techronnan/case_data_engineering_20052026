# Databricks notebook source
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Landing — Upload Sources NDJSON
# MAGIC
# MAGIC ## Objetivo
# MAGIC Processar arquivos NDJSON (newline-delimited JSON) da pasta sources/ e converter para Parquet na landing zone.
# MAGIC
# MAGIC ## Arquivos Processados
# MAGIC * atendimento_ocorrencias.ndjson (line-delimited JSON)
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

print("Validando presença dos arquivos NDJSON...")
try:
    present = set(os.listdir(SOURCES_PATH))
    ndjson_files = [f for f in EXPECTED_FILES if f.endswith('.ndjson')]
    missing = []
    
    for fname in ndjson_files:
        status = "OK" if fname in present else "FALTANDO"
        if status != "OK":
            missing.append(fname)
            _erros_landing.append(fname)
        print(f"  [{status}]  {fname}")
    
    if missing:
        print(f"\n  {len(missing)} arquivo(s) NDJSON faltando.")
    else:
        print(f"\n  Todos os {len(ndjson_files)} arquivos NDJSON presentes.")
        
except Exception as e:
    _erros_landing.append(str(e))
    print(f"Erro ao validar arquivos: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento e Conversão

# COMMAND ----------

# DBTITLE 1,Processamento NDJSON
_erros_conversao = []
_total_registros = 0

_now = datetime.now()
_ano = _now.strftime("%Y")
_mes = _now.strftime("%m")
_timestamp = _now.strftime("%Y%m%d%H%M%S")

print(f"Convertendo arquivos NDJSON para Parquet... [{_timestamp}]\n")

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "ndjson":
        continue
        
    name = config["name"]
    fname = config["file"]
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    
    try:
        print(f"  [{sistema}] Lendo {fname} (ndjson)...")
        
        # Ler NDJSON (cada linha é um JSON)
        rows = []
        with open(src_path, "r", encoding="utf-8") as _f:
            for line in _f:
                line = line.strip()
                if line:
                    obj = json.loads(line)
                    if obj is not None:
                        rows.append(obj)
        
        if not rows:
            raise ValueError("Arquivo NDJSON vazio ou inválido")
        
        df = spark.createDataFrame(rows)
        if "metadata" in df.columns:
            metadata_schema = df.schema["metadata"].dataType
            if isinstance(metadata_schema, StructType):
                for field in metadata_schema.fields:
                    df = df.withColumn(f"metadata_{field.name}", col("metadata").getItem(field.name))
            elif isinstance(metadata_schema, MapType):
                keys = df.select(map_keys(col("metadata"))).distinct().collect()
                keys_flat = set()
                for row in keys:
                    if row[0] is not None:
                        keys_flat.update(row[0])
                for k in keys_flat:
                    df = df.withColumn(f"metadata_{k}", col("metadata").getItem(k))
            df = df.drop("metadata")
        
        count = df.count()
        _total_registros += count
        
        print(f"  [{sistema}] Gravando {count:,} registros → {dst_file}")
        
        tmp_dir = f"{dst_dir}/_tmp_{_timestamp}"
        dbutils.fs.mkdirs(dst_dir)
        df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)
        
        part_files = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".parquet")]
        if not part_files:
            raise ValueError("Nenhum arquivo Parquet gerado")
        dbutils.fs.mv(part_files[0], dst_file)
        dbutils.fs.rm(tmp_dir, recurse=True)
        
        print(f"  [OK] {fname} convertido com sucesso\n")
        
    except Exception as e:
        _erros_conversao.append(f"{fname}: {str(e)}")
        print(f"  [ERRO] {fname}: {e}\n")

print(f"Conversão NDJSON concluída. Total: {_total_registros:,} registros")

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
    tabela=f'{CATALOG}.monitoring.landing_sources_ndjson',
    duracao_segundos=_duracao_landing,
    status=_status_landing,
    linhas=_total_registros,
    erro=_msg_erro,
)

print(f"\n[Landing NDJSON] Status: {_status_landing} | {_duracao_landing:.1f}s | {_total_registros:,} registros")
if _todos_erros:
    print(f"Erros: {_msg_erro}")

# COMMAND ----------

for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "ndjson":
        continue

    name = config["name"]
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    
    try:
        print(f"\n[{sistema}] Visualizando 3 linhas do Parquet: {dst_file}")
        df_out = spark.read.parquet(dst_file)
        display(df_out.limit(3))
    except Exception as e:
        print(f"[ERRO] Falha ao ler {dst_file}: {e}")
