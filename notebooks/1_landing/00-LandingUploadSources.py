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
# MAGIC | Destino Fonte de Dados de Saída | DBFS `/FileStore/case/sources/{sistema}/` |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 20/05/2026 | Ronnan           | Criação do notebook e organização em subdiretórios para padrão AutoLoader. |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

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
# MAGIC ## Validação dos Arquivos na Raiz

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

try:
    present = {f.name for f in dbutils.fs.ls(SOURCES_PATH)}
    missing = []
    for fname in EXPECTED_FILES:
        status = "OK " if fname in present else "FALTANDO"
        if status != "OK ":
            missing.append(fname)
        print(f"  [{status}]  {fname}")

    print()
    if missing:
        print(f"  {len(missing)} arquivo(s) faltando — execute o upload antes de continuar.")
    else:
        print("  Todos os arquivos presentes. Prosseguindo com organização.")
except Exception as e:
    print(f"Erro ao acessar {SOURCES_PATH}: {e}")
    print("Certifique-se de ter feito o upload dos arquivos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organização em Subdiretórios (AutoLoader)
# MAGIC
# MAGIC Cada sistema recebe seu próprio subdiretório para que o AutoLoader possa monitorar
# MAGIC chegadas de novos arquivos por fonte de forma independente.

# COMMAND ----------

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
            print(f"  [ERRO] {fname}: {e}")

print("\nOrganização concluída.")
