# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Landing — Upload e Organização das Fontes
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Validar presença dos arquivos no DBFS e organizá-los em subdiretórios por sistema para o AutoLoader |
# MAGIC | Origem Fonte de Dados de Entrada | Pasta `sources/` do repositório |
# MAGIC | Destino Fonte de Dados de Saída | DBFS `{SOURCES_PATH}/{sistema}/` |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação do notebook e organização em subdiretórios para padrão AutoLoader. |
# MAGIC | 21/05/2026 | Ronnan           | Migrado para 0-Init (entry point único). Parametrização via EXPECTED_FILES e SOURCE_MAP. Monitoramento via log_table_execution. |

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

# Mapeamento: subdiretório → arquivo(s) de origem
SOURCE_MAP = {
    "erp":         ["erp_pedidos_cabecalho_2025.csv", "erp_pedidos_itens_2025.csv"],
    "legado":      ["legado_regioes_pipe.txt"],
    "vendedores":  ["vendedores.csv"],
    "atendimento": ["atendimento_ocorrencias.ndjson"],
    "logistica":   ["logistica_entregas.json"],
    "produtos":    ["cadastro_produtos_api_dump.json"],
    "crm":         ["crm_clientes_export.xlsx"],
    "canais":      ["comercial_canais.xlsx"],
}

print(f"SOURCES_PATH  : {SOURCES_PATH}")
print(f"Arquivos esperados : {len(EXPECTED_FILES)}")
print(f"Sistemas mapeados  : {list(SOURCE_MAP.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos Arquivos na Raiz

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

# MAGIC %md
# MAGIC ## Organização em Subdiretórios (AutoLoader)
# MAGIC
# MAGIC Cada sistema recebe seu próprio subdiretório para que o AutoLoader possa monitorar
# MAGIC chegadas de novos arquivos por fonte de forma independente.

# COMMAND ----------

_erros_organizacao = []

print("Organizando arquivos em subdiretórios...\n")
for container, files in SOURCE_MAP.items():
    subdir = f"{SOURCES_PATH}/{container}"
    try:
        dbutils.fs.mkdirs(subdir)
    except Exception:
        pass
    for fname in files:
        src = f"{SOURCES_PATH}/{fname}"
        dst = f"{subdir}/{fname}"
        try:
            dbutils.fs.cp(src, dst, recurse=False)
            print(f"  [OK] {fname}  →  {container}/")
        except Exception as e:
            _erros_organizacao.append(f"{fname}: {e}")
            print(f"  [ERRO] {fname}: {e}")

print("\nOrganização concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro de Monitoramento

# COMMAND ----------

_duracao_landing = round(time.time() - _inicio_landing, 2)
_todos_erros     = _erros_landing + _erros_organizacao
_status_landing  = 'FALHA' if _todos_erros else 'SUCESSO'
_msg_erro        = ' | '.join(_todos_erros) if _todos_erros else ''

log_table_execution(
    tabela    = f'{CATALOG}.default.landing_sources',
    duracao_segundos = _duracao_landing,
    status    = _status_landing,
    linhas    = len(EXPECTED_FILES),
    erro      = _msg_erro,
)

print(f"\n[Landing] Status: {_status_landing} | {_duracao_landing:.1f}s")
