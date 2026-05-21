# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 6-MonitoringLogs — Controller de Execução do Pipeline
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Criar e manter a tabela `pipeline_controller`, registrar execuções e disponibilizar funções de consulta de saúde do pipeline |
# MAGIC | Tabela Controladora | `workspace.default.pipeline_controller` |
# MAGIC | Executado Via | `0-Init` — não executar diretamente |
# MAGIC
# MAGIC ## Schema da Tabela Controladora
# MAGIC
# MAGIC | Coluna | Tipo | Descrição |
# MAGIC |--------|------|-----------|
# MAGIC | `tabela_nome` | STRING | Nome completo da tabela (catalog.schema.nome) |
# MAGIC | `camada` | STRING | landing / bronze / silver / gold |
# MAGIC | `status_execucao` | STRING | SUCESSO / FALHA |
# MAGIC | `linhas_processadas` | BIGINT | Quantidade de linhas escritas |
# MAGIC | `data_execucao` | TIMESTAMP | Timestamp de início da execução |
# MAGIC | `ultima_atualizacao` | TIMESTAMP | Timestamp de conclusão |
# MAGIC | `duracao_segundos` | DOUBLE | Tempo total de execução |
# MAGIC | `mensagem_erro` | STRING | Stack trace / detalhe do erro (vazio em sucesso) |
# MAGIC | `pipeline_versao` | STRING | Versão do pipeline no momento da execução |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 21/05/2026 | Ronnan           | Criação do notebook de monitoramento centralizado. |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criação da Tabela Controladora (idempotente)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
        tabela_nome        STRING         COMMENT 'Nome completo catalog.schema.tabela',
        camada             STRING         COMMENT 'landing | bronze | silver | gold',
        status_execucao    STRING         COMMENT 'SUCESSO | FALHA',
        linhas_processadas BIGINT         COMMENT 'Quantidade de linhas escritas',
        data_execucao      TIMESTAMP      COMMENT 'Timestamp de início',
        ultima_atualizacao TIMESTAMP      COMMENT 'Timestamp de conclusão',
        duracao_segundos   DOUBLE         COMMENT 'Duração em segundos',
        mensagem_erro      STRING         COMMENT 'Detalhe do erro — vazio em sucesso',
        pipeline_versao    STRING         COMMENT 'Versão do pipeline'
    )
    USING DELTA
    COMMENT 'Tabela controladora de execuções do pipeline Medallion'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print(f"[MonitoringLogs] Tabela controladora pronta: {CONTROL_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funções de Consulta

# COMMAND ----------

def get_pipeline_summary(data_execucao: str = None) -> None:
    """Exibe resumo de execução do pipeline.

    Parâmetros:
        data_execucao : filtrar por data no formato 'yyyy-MM-dd' (None = mais recente)
    """
    if data_execucao:
        filtro = f"WHERE DATE(data_execucao) = '{data_execucao}'"
    else:
        filtro = """
            WHERE DATE(data_execucao) = (
                SELECT DATE(MAX(data_execucao)) FROM {CONTROL_TABLE}
            )
        """.replace("{CONTROL_TABLE}", CONTROL_TABLE)

    spark.sql(f"""
        SELECT
            camada,
            tabela_nome,
            status_execucao,
            linhas_processadas,
            ROUND(duracao_segundos, 1)     AS duracao_s,
            DATE_FORMAT(data_execucao, 'HH:mm:ss') AS inicio,
            DATE_FORMAT(ultima_atualizacao,'HH:mm:ss') AS fim,
            pipeline_versao
        FROM {CONTROL_TABLE}
        {filtro}
        ORDER BY camada, tabela_nome, data_execucao DESC
    """).display()


def get_last_errors(n: int = 10) -> None:
    """Exibe os últimos N erros registrados."""
    spark.sql(f"""
        SELECT
            data_execucao,
            camada,
            tabela_nome,
            mensagem_erro
        FROM {CONTROL_TABLE}
        WHERE status_execucao = 'FALHA'
          AND mensagem_erro IS NOT NULL
          AND mensagem_erro != ''
        ORDER BY data_execucao DESC
        LIMIT {n}
    """).display()


def get_table_history(tabela: str) -> None:
    """Exibe histórico de execuções de uma tabela específica."""
    spark.sql(f"""
        SELECT
            data_execucao,
            status_execucao,
            linhas_processadas,
            ROUND(duracao_segundos, 1) AS duracao_s,
            mensagem_erro,
            pipeline_versao
        FROM {CONTROL_TABLE}
        WHERE tabela_nome = '{tabela}'
        ORDER BY data_execucao DESC
        LIMIT 30
    """).display()


def get_pipeline_kpis() -> None:
    """Exibe KPIs consolidados da última execução completa."""
    spark.sql(f"""
        WITH ultima_exec AS (
            SELECT DATE(MAX(data_execucao)) AS ultima_data
            FROM {CONTROL_TABLE}
        )
        SELECT
            u.ultima_data                                       AS data_execucao,
            COUNT(*)                                            AS total_tabelas,
            SUM(CASE WHEN status_execucao = 'SUCESSO' THEN 1 ELSE 0 END) AS sucessos,
            SUM(CASE WHEN status_execucao = 'FALHA'   THEN 1 ELSE 0 END) AS falhas,
            SUM(linhas_processadas)                             AS total_linhas,
            ROUND(SUM(duracao_segundos), 1)                    AS tempo_total_s,
            ROUND(AVG(duracao_segundos), 1)                    AS tempo_medio_s
        FROM {CONTROL_TABLE} c
        CROSS JOIN ultima_exec u
        WHERE DATE(c.data_execucao) = u.ultima_data
        GROUP BY u.ultima_data
    """).display()

# COMMAND ----------

print("[MonitoringLogs] Funções disponíveis:")
print("  get_pipeline_summary(data_execucao=None) — resumo por data")
print("  get_last_errors(n=10)                    — últimos erros")
print("  get_table_history(tabela)                — histórico de uma tabela")
print("  get_pipeline_kpis()                      — KPIs consolidados")
print(f"  Tabela: {CONTROL_TABLE}")
