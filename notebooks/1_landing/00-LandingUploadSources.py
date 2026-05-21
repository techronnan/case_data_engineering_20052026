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
# MAGIC | Origem Fonte de Dados de Entrada | DBFS `{SOURCES_PATH}/` (arquivos raw: CSV, JSON, XLSX, NDJSON, pipe-delimited) |
# MAGIC | Destino Fonte de Dados de Saída | DBFS `/FileStore/case/landing/{sistema}/` (Parquet otimizado) |
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
# MAGIC ## Como Fazer o Upload das Fontes
# MAGIC
# MAGIC **Opção 1 — Via CLI (recomendado):**
# MAGIC ```bash
# MAGIC databricks fs mkdirs dbfs:/FileStore/case/sources
# MAGIC databricks fs cp sources/ dbfs:/FileStore/case/sources/ --recursive --profile AZDO
# MAGIC ```
# MAGIC
# MAGIC **Opção 2 — Via UI do Databricks:**
# MAGIC 1. Menu lateral → **Catalog** → aba **Browse** → **DBFS** → `/FileStore/case/sources/`
# MAGIC 2. Botão **Upload** → selecionar todos os arquivos da pasta `sources/`

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
        "file": "erp_pedidos_cabecalho_2025.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ","}
    },
    "erp_itens": {
        "file": "erp_pedidos_itens_2025.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ","}
    },
    "legado": {
        "file": "legado_regioes_pipe.txt",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": "|"}
    },
    "vendedores": {
        "file": "vendedores.csv",
        "format": "csv",
        "options": {"header": True, "inferSchema": True, "sep": ","}
    },
    "atendimento": {
        "file": "atendimento_ocorrencias.ndjson",
        "format": "json",
        "options": {}
    },
    "logistica": {
        "file": "logistica_entregas.json",
        "format": "json",
        "options": {"multiLine": True}
    },
    "produtos": {
        "file": "cadastro_produtos_api_dump.json",
        "format": "json",
        "options": {"multiLine": True}
    },
    "crm": {
        "file": "crm_clientes_export.xlsx",
        "format": "excel",
        "options": {"header": True, "inferSchema": True}
    },
    "canais": {
        "file": "comercial_canais.xlsx",
        "format": "excel",
        "options": {"header": True, "inferSchema": True}
    },
}

# Caminho de destino para landing zone
LANDING_PATH = "dbfs:/FileStore/case/landing"

print(f"SOURCES_PATH  : {SOURCES_PATH}")
print(f"LANDING_PATH  : {LANDING_PATH}")
print(f"Arquivos esperados : {len(EXPECTED_FILES)}")
print(f"Sistemas mapeados  : {list(SOURCE_MAP.keys())}")

# COMMAND ----------

import time
_inicio_landing = time.time()
_erros_landing  = []

try:
    present = {f.name for f in dbutils.fs.ls(SOURCES_PATH)}
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

print("Convertendo arquivos brutos para Parquet...\n")

for sistema, config in SOURCE_MAP.items():
    fname = config["file"]
    fmt = config["format"]
    opts = config["options"]
    
    src_path = f"{SOURCES_PATH}/{fname}"
    dst_path = f"{LANDING_PATH}/{sistema}"
    
    try:
        # Leitura conforme formato
        print(f"  [{sistema}] Lendo {fname} ({fmt})...")
        
        if fmt == "csv":
            df = spark.read.format("csv").options(**opts).load(src_path)
        elif fmt == "json":
            df = spark.read.format("json").options(**opts).load(src_path)
        elif fmt == "excel":
            # Excel requer biblioteca com.crealytics.spark.excel
            df = spark.read.format("com.crealytics.spark.excel").options(**opts).load(src_path)
        else:
            raise ValueError(f"Formato desconhecido: {fmt}")
        
        # Contagem de registros
        count = df.count()
        _total_registros += count
        
        # Conversão para Parquet
        print(f"  [{sistema}] Convertendo {count:,} registros para Parquet...")
        df.write.mode("overwrite").parquet(dst_path)
        
        print(f"  [OK] {fname} → {sistema}/ ({count:,} registros)\n")
        
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
