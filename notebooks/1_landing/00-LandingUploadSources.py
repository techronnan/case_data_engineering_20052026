# Databricks notebook source
# DBTITLE 1,Documentação
# MAGIC %md
# MAGIC # Landing — Conversão e Organização das Fontes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Ler arquivos brutos em diversos formatos, converter para Parquet otimizado e organizar em landing zone por sistema |
# MAGIC | Origem Fonte de Dados de Entrada | UC Volume `sources_data/` — arquivos raw intocados (CSV, JSON, XLSX, NDJSON, pipe-delimited) |
# MAGIC | Destino Fonte de Dados de Saída | UC Volume `systems/{sistema}/{ano}/{mes}/{file_name_YYYYMMDDHHMMSS}.parquet` — Parquet puro, sem Delta |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação do notebook e organização em subdiretórios. |
# MAGIC | 21/05/2026 | Ronnan           | Parametrização via EXPECTED_FILES e SOURCE_MAP. Monitoramento. |
# MAGIC | 21/05/2026 | Ronnan           | **REFATORAÇÃO COMPLETA**: Conversão de formatos raw → Parquet otimizado na landing zone. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

# MAGIC %md
# MAGIC ## Origem das Fontes
# MAGIC
# MAGIC Os arquivos raw são lidos diretamente do Workspace Repos, na pasta `sources/` do próprio repositório.
# MAGIC Nenhum upload manual é necessário — o caminho é resolvido automaticamente via `current_user()`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos Arquivos na Raiz

# COMMAND ----------

# DBTITLE 1,Conversão para Parquet
# MAGIC %md
# MAGIC ## Conversão para Parquet e Salvamento em Landing Zone
# MAGIC
# MAGIC Cada arquivo bruto é lido no formato original, convertido para Parquet otimizado
# MAGIC e salvo em seu subdiretório na landing zone.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro de Monitoramento

# COMMAND ----------

# DBTITLE 1,Parâmetros e Mapeamentos
EXPECTED_FILES = [
    "erp_pedidos_cabecalho_2025.csv",
    "erp_pedidos_itens_2025.csv",
    "legado_regioes_pipe.txt",
    "vendedores.csv",
    "atendimento_ocorrencias.ndjson",
    "logistica_entregas.json",
    "cadastro_produtos_api_dump.json",
    "crm_clientes_export.xlsx",
    "comercial_canais.xlsx",
]

# Mapeamento: subdiretório → (arquivo, formato, opções de leitura)
SOURCE_MAP = {
    "erp_cabecalho": {
        "name": "erp_pedidos_cabecalho",
        "file": "erp_pedidos_cabecalho_2025.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ";"}
    },
    "erp_itens": {
        "name": "erp_pedidos_itens",
        "file": "erp_pedidos_itens_2025.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ","}
    },
    "legado": {
        "name": "legado_regioes",
        "file": "legado_regioes_pipe.txt",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": "|"}
    },
    "vendedores": {
        "name": "vendedores",
        "file": "vendedores.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ";"}
    },
    "atendimento": {
        "name": "atendimento_ocorrencias",
        "file": "atendimento_ocorrencias.ndjson",
        "format": "json",
        "options": {}
    },
    "logistica": {
        "name": "logistica_entregas",
        "file": "logistica_entregas.json",
        "format": "json",
        "options": {"multiLine": True}
    },
    "produtos": {
        "name": "cadastro_produtos",
        "file": "cadastro_produtos_api_dump.json",
        "format": "json",
        "options": {"multiLine": True}
    },
    "crm": {
        "name": "crm_clientes",
        "file": "crm_clientes_export.xlsx",
        "format": "excel",
        "options": {}
    },
    "canais": {
        "name": "comercial_canais",
        "file": "comercial_canais.xlsx",
        "format": "excel",
        "options": {}
    },
}

# Caminho de destino para landing zone
print(f"SOURCES_PATH  : {SOURCES_PATH}")
print(f"LANDING_PATH  : {LANDING_PATH}")
print(f"Arquivos esperados : {len(EXPECTED_FILES)}")
print(f"Sistemas mapeados  : {list(SOURCE_MAP.keys())}")

# COMMAND ----------

import time

_inicio_landing = time.time()
_erros_landing  = []

try:
    present = set(os.listdir(SOURCES_PATH))
    missing = []
    for fname in EXPECTED_FILES:
        status = "OK " if fname in present else "FALTANDO"
        if status != "OK ":
            missing.append(fname)
            _erros_landing.append(fname)
        print(f"  [{status}]  {fname}")

    print()
    if missing:
        print(f"  {len(missing)} arquivo(s) faltando — execute o upload antes de continuar.")
    else:
        print(f"  Todos os {len(EXPECTED_FILES)} arquivos presentes. Prosseguindo com organização.")
except Exception as e:
    _erros_landing.append(str(e))
    print(f"Erro ao acessar {SOURCES_PATH}: {e}")
    print("Certifique-se de ter feito o upload dos arquivos.")

# COMMAND ----------

# DBTITLE 1,Processamento e Conversão
_erros_conversao = []
_total_registros = 0

_now       = datetime.now()
_ano       = _now.strftime("%Y")
_mes       = _now.strftime("%m")
_timestamp = _now.strftime("%Y%m%d%H%M%S")

print(f"Convertendo arquivos brutos para Parquet... [{_timestamp}]\n")

for sistema, config in SOURCE_MAP.items():
    name  = config["name"]
    fname = config["file"]
    fmt   = config["format"]
    opts  = config["options"]

    src_path = f"{SOURCES_PATH}/{fname}"
    dst_dir  = f"{LANDING_PATH}/{name}/{_ano}/{_mes}"
    dst_file = f"{dst_dir}/{name}_{_timestamp}.parquet"

    try:
        print(f"  [{sistema}] Lendo {fname} ({fmt})...")

        if fmt == "csv":
            sep = opts.get("sep", ",")
            with open(src_path, newline="", encoding="utf-8-sig") as _f:
                rows = list(csv.DictReader(_f, delimiter=sep))
            df = spark.createDataFrame(rows)
        elif fmt == "json":
            with open(src_path, "r", encoding="utf-8") as _f:
                if opts.get("multiLine", False):
                    data = json.load(_f)
                    rows = data if isinstance(data, list) else [data]
                else:
                    rows = [json.loads(line) for line in _f if line.strip()]
            df = spark.createDataFrame(rows)
        elif fmt == "excel":
            wb = openpyxl.load_workbook(src_path, read_only=True, data_only=True)
            ws = wb.active
            headers = [cell.value for cell in next(ws.iter_rows(max_row=1))]
            rows = [
                {headers[i]: cell for i, cell in enumerate(row)}
                for row in ws.iter_rows(min_row=2, values_only=True)
            ]
            wb.close()
            df = spark.createDataFrame(rows)
        else:
            raise ValueError(f"Formato desconhecido: {fmt}")

        count = df.count()
        _total_registros += count

        print(f"  [{sistema}] Gravando {count:,} registros → {dst_file}")
        tmp_dir = f"{dst_dir}/_tmp_{_timestamp}"
        dbutils.fs.mkdirs(dst_dir)
        df.coalesce(1).write.mode("overwrite").parquet(tmp_dir)
        part = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.endswith(".parquet")][0]
        dbutils.fs.mv(part, dst_file)
        dbutils.fs.rm(tmp_dir, recurse=True)

        print(f"  [OK] {fname} → systems/{sistema}/{_ano}/{_mes}/ ({count:,} registros)\n")

    except Exception as e:
        _erros_conversao.append(f"{fname}: {str(e)}")
        print(f"  [ERRO] {fname}: {e}\n")

print(f"\nConversão concluída. Total de registros processados: {_total_registros:,}")

# COMMAND ----------

# DBTITLE 1,Registro de Monitoramento
_duracao_landing = round(time.time() - _inicio_landing, 2)
_todos_erros     = _erros_landing + _erros_conversao
_status_landing  = 'FALHA' if _todos_erros else 'SUCESSO'
_msg_erro        = ' | '.join(_todos_erros) if _todos_erros else ''

log_table_execution(
    tabela    = f'{CATALOG}.monitoring.landing_sources',
    duracao_segundos = _duracao_landing,
    status    = _status_landing,
    linhas    = _total_registros,
    erro      = _msg_erro,
)

print(f"\n[Landing] Status: {_status_landing} | {_duracao_landing:.1f}s | {_total_registros:,} registros")
