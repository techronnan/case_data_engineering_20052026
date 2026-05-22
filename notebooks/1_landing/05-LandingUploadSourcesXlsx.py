# Databricks notebook source
# MAGIC %md
# MAGIC # Landing — Upload Sources XLSX
# MAGIC
# MAGIC ## Objetivo
# MAGIC Processar arquivos Excel (.xlsx) da pasta sources/ e converter para Parquet na landing zone.
# MAGIC
# MAGIC ## Arquivos Processados
# MAGIC * crm_clientes_export.xlsx → crm_clientes
# MAGIC * comercial_canais.xlsx → comercial_canais
# MAGIC
# MAGIC ## Estrutura de Saída
# MAGIC `systems/{nome_arquivo}/{ano}/{mes}/{nome_arquivo}_YYYYMMDDHHMMSS.parquet`

# COMMAND ----------

# DBTITLE 1,Inicialização
# MAGIC %run ../0_config/0-Init

# COMMAND ----------

# DBTITLE 1,Variáveis de Configuração
# Formato de arquivo a processar
formato_arquivo = 'xlsx'

# Arquivos esperados no SOURCE_MAP
arquivos_esperados = [
    'crm_clientes_export.xlsx',
    'comercial_canais.xlsx'
]

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Validação dos Arquivos Source

# COMMAND ----------

# DBTITLE 1,Validação de Presença
# Inicia cronômetro e lista de erros
_inicio_landing = time.time()
_erros_landing = []

print("Validando presença dos arquivos Excel...")

try:
    # Lista arquivos presentes no diretório source
    present = set(os.listdir(SOURCES_PATH))
    
    # Filtra apenas arquivos XLSX da lista esperada
    xlsx_files = [f for f in EXPECTED_FILES if f.endswith('.xlsx')]
    missing = []
    
    # Valida cada arquivo XLSX esperado
    for fname in xlsx_files:
        status = "OK" if fname in present else "FALTANDO"
        if status != "OK":
            missing.append(fname)
            _erros_landing.append(fname)
        print(f"  [{status}]  {fname}")
    
    # Relatório de validação
    if missing:
        print(f"\n  [AVISO] {len(missing)} arquivo(s) Excel faltando.")
    else:
        print(f"\n  [OK] Todos os {len(xlsx_files)} arquivos Excel presentes.")
        
except Exception as e:
    _erros_landing.append(str(e))
    print(f"[ERRO] Erro ao validar arquivos: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Processamento e Conversão para Parquet

# COMMAND ----------

# DBTITLE 1,Conversão Excel → Parquet
# Inicializa contadores e listas de erro
_erros_conversao = []
_total_registros = 0

# Obtém timestamp atual para nomenclatura dos arquivos
_now = datetime.now()
_ano = _now.strftime("%Y")
_mes = _now.strftime("%m")
_timestamp = _now.strftime("%Y%m%d%H%M%S")

print(f"Convertendo arquivos Excel para Parquet... [{_timestamp}]\n")

# Itera sobre cada sistema mapeado no SOURCE_MAP
for sistema, config in SOURCE_MAP.items():
    # Filtra apenas arquivos Excel
    if config.get("format") != "xlsx":
        continue
        
    # Extrai configurações do arquivo
    name = config["name"]
    fname = config["file"]
    
    # Define caminhos de origem e destino
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    
    try:
        print(f"  [{sistema}] Lendo {fname} (xlsx)...")
        
        # Lê Excel usando openpyxl (biblioteca já importada no Init)
        wb = openpyxl.load_workbook(src_path, read_only=True, data_only=True)
        ws = wb.active
        
        # Obtém headers da primeira linha
        headers = [cell.value for cell in next(ws.iter_rows(max_row=1))]
        
        # Lê dados a partir da linha 2 (pula header)
        rows = [
            {headers[i]: cell for i, cell in enumerate(row)}
            for row in ws.iter_rows(min_row=2, values_only=True)
        ]
        wb.close()
        
        # Cria DataFrame Spark a partir dos dados lidos
        df = spark.createDataFrame(rows)
        count = df.count()
        _total_registros += count
        
        print(f"  [{sistema}] Gravando {count:,} registros -> {dst_file}")
        
        # Grava como Parquet em diretório temporário
        tmp_dir = f"{dst_dir}/_tmp_{_timestamp}"
        dbutils.fs.mkdirs(dst_dir)
        df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)
        
        # Renomeia arquivo único gerado (remove prefixo part-*)
        part = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".parquet")][0]
        dbutils.fs.mv(part, dst_file)
        dbutils.fs.rm(tmp_dir, recurse=True)
        
        print(f"  [OK] {fname} convertido com sucesso\n")
        
    except Exception as e:
        _erros_conversao.append(f"{fname}: {str(e)}")
        print(f"  [ERRO] {fname}: {e}\n")

print(f"\n{'='*70}")
print(f"Conversão Excel concluída. Total: {_total_registros:,} registros processados")
print(f"{'='*70}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Registro de Monitoramento

# COMMAND ----------

# DBTITLE 1,Log de Execução
# Calcula duração total da execução
_duracao_landing = round(time.time() - _inicio_landing, 2)

# Consolida todos os erros (validação + conversão)
_todos_erros = _erros_landing + _erros_conversao
_status_landing = 'FALHA' if _todos_erros else 'SUCESSO'
_msg_erro = ' | '.join(_todos_erros) if _todos_erros else ''

# Registra execução na tabela de monitoramento
log_table_execution(
    tabela=f'{CATALOG}.monitoring.landing_sources_xlsx',
    duracao_segundos=_duracao_landing,
    status=_status_landing,
    linhas=_total_registros,
    erro=_msg_erro,
)

# Exibe resumo final
print(f"\n{'='*70}")
print(f"[Landing XLSX] Status: {_status_landing} | {_duracao_landing:.1f}s | {_total_registros:,} registros")
if _todos_erros:
    print(f"[AVISO] Erros encontrados: {_msg_erro}")
else:
    print(f"[OK] Execução concluída com sucesso!")
print(f"{'='*70}")

# COMMAND ----------

# Exibe as 3 primeiras linhas de cada arquivo Parquet gerado
for sistema, config in SOURCE_MAP.items():
    if config.get("format") != "xlsx":
        continue
    name = config["name"]
    dst_dir = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"
    try:
        df_parquet = spark.read.parquet(dst_file)
        print(f"\n--- {name} ({dst_file}) ---")
        display(df_parquet.limit(3))
    except Exception as e:
        print(f"[ERRO] Erro ao ler {dst_file}: {e}")
