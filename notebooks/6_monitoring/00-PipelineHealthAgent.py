# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Pipeline Health Agent — Monitor de Saúde do Pipeline
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Agent de monitoramento de saúde do pipeline: executa verificações, exibe dashboard e emite alertas via condições programáticas |
# MAGIC | Fonte de Dados | `workspace.default.pipeline_controller` |
# MAGIC | Nível | Workspace — cobre todas as camadas (landing, bronze, silver, gold) |
# MAGIC | Uso | Executar após cada pipeline completo ou agendar como Job independente |
# MAGIC
# MAGIC ## Histórico
# MAGIC
# MAGIC | Data       | Desenvolvido Por | Motivo |
# MAGIC |:----------:|------------------|--------|
# MAGIC | 21/05/2026 | Ronnan           | Criação do agent de monitoramento de saúde do pipeline. |

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parâmetros

# COMMAND ----------

# Data de análise (None = mais recente disponível na controller)
DATA_ANALISE      = None           # ex: '2026-05-21' para filtrar data específica
THRESHOLD_FALHA   = 0              # alertar se falhas > 0
THRESHOLD_LINHAS  = 0             # alertar se total de linhas < este valor

print(f"[HealthAgent] Controller : {CONTROL_TABLE}")
print(f"[HealthAgent] Data ref.  : {DATA_ANALISE or 'mais recente'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificação de Disponibilidade da Tabela Controller

# COMMAND ----------

try:
    _controller_exists = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {CONTROL_TABLE}
    """).collect()[0][0]
    print(f"[OK] pipeline_controller acessível — {_controller_exists:,} registros encontrados.")
except Exception as _e:
    print(f"[ERRO] Não foi possível acessar {CONTROL_TABLE}: {_e}")
    print("Execute o pipeline ao menos uma vez para popular a tabela controller.")
    dbutils.notebook.exit("SEM_DADOS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. KPIs da Última Execução

# COMMAND ----------

spark.sql(f"""
    WITH ultima_exec AS (
        SELECT DATE(MAX(data_execucao)) AS ultima_data
        FROM {CONTROL_TABLE}
    )
    SELECT
        u.ultima_data                                                       AS data_execucao,
        COUNT(*)                                                            AS total_tabelas,
        SUM(CASE WHEN status_execucao = 'SUCESSO' THEN 1 ELSE 0 END)       AS tabelas_ok,
        SUM(CASE WHEN status_execucao = 'FALHA'   THEN 1 ELSE 0 END)       AS tabelas_falha,
        FORMAT_NUMBER(SUM(linhas_processadas), 0)                           AS total_linhas,
        ROUND(SUM(duracao_segundos) / 60.0, 1)                             AS tempo_total_min,
        ROUND(AVG(duracao_segundos), 1)                                     AS tempo_medio_s,
        MAX(pipeline_versao)                                                AS versao_pipeline
    FROM {CONTROL_TABLE} c
    CROSS JOIN ultima_exec u
    WHERE DATE(c.data_execucao) = u.ultima_data
    GROUP BY u.ultima_data
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Status por Camada

# COMMAND ----------

spark.sql(f"""
    WITH ultima_exec AS (
        SELECT DATE(MAX(data_execucao)) AS ultima_data
        FROM {CONTROL_TABLE}
    )
    SELECT
        c.camada,
        COUNT(*)                                                       AS tabelas,
        SUM(CASE WHEN status_execucao = 'SUCESSO' THEN 1 ELSE 0 END)  AS ok,
        SUM(CASE WHEN status_execucao = 'FALHA'   THEN 1 ELSE 0 END)  AS falha,
        FORMAT_NUMBER(SUM(linhas_processadas), 0)                      AS linhas,
        ROUND(SUM(duracao_segundos), 1)                                AS duracao_total_s
    FROM {CONTROL_TABLE} c
    CROSS JOIN ultima_exec u
    WHERE DATE(c.data_execucao) = u.ultima_data
    GROUP BY c.camada
    ORDER BY CASE c.camada
        WHEN 'landing' THEN 1
        WHEN 'bronze'  THEN 2
        WHEN 'silver'  THEN 3
        WHEN 'gold'    THEN 4
        ELSE 5 END
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Detalhe de Cada Tabela (Última Execução)

# COMMAND ----------

spark.sql(f"""
    WITH ultima_exec AS (
        SELECT DATE(MAX(data_execucao)) AS ultima_data
        FROM {CONTROL_TABLE}
    ),
    ranked AS (
        SELECT
            c.*,
            ROW_NUMBER() OVER (PARTITION BY c.tabela_nome ORDER BY c.data_execucao DESC) AS rn
        FROM {CONTROL_TABLE} c
        CROSS JOIN ultima_exec u
        WHERE DATE(c.data_execucao) = u.ultima_data
    )
    SELECT
        camada,
        tabela_nome,
        status_execucao,
        FORMAT_NUMBER(linhas_processadas, 0)               AS linhas,
        ROUND(duracao_segundos, 1)                         AS duracao_s,
        DATE_FORMAT(data_execucao, 'HH:mm:ss')            AS inicio,
        DATE_FORMAT(ultima_atualizacao, 'HH:mm:ss')       AS fim,
        CASE WHEN mensagem_erro != '' THEN mensagem_erro ELSE '' END AS erro
    FROM ranked
    WHERE rn = 1
    ORDER BY
        CASE camada
            WHEN 'landing' THEN 1
            WHEN 'bronze'  THEN 2
            WHEN 'silver'  THEN 3
            WHEN 'gold'    THEN 4
            ELSE 5 END,
        tabela_nome
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Histórico de Execuções (últimos 7 dias)

# COMMAND ----------

spark.sql(f"""
    SELECT
        DATE(data_execucao)                                                 AS data,
        COUNT(DISTINCT tabela_nome)                                         AS tabelas_executadas,
        SUM(CASE WHEN status_execucao = 'SUCESSO' THEN 1 ELSE 0 END)       AS sucessos,
        SUM(CASE WHEN status_execucao = 'FALHA'   THEN 1 ELSE 0 END)       AS falhas,
        FORMAT_NUMBER(SUM(linhas_processadas), 0)                           AS linhas_total,
        ROUND(SUM(duracao_segundos) / 60.0, 1)                             AS tempo_total_min
    FROM {CONTROL_TABLE}
    WHERE data_execucao >= CURRENT_DATE() - INTERVAL 7 DAYS
    GROUP BY DATE(data_execucao)
    ORDER BY data DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Alertas Automáticos

# COMMAND ----------

_alertas = []

# Verificar falhas na última execução
_falhas = spark.sql(f"""
    WITH ultima_exec AS (
        SELECT DATE(MAX(data_execucao)) AS ultima_data FROM {CONTROL_TABLE}
    )
    SELECT tabela_nome, camada, mensagem_erro
    FROM {CONTROL_TABLE} c CROSS JOIN ultima_exec u
    WHERE DATE(c.data_execucao) = u.ultima_data
      AND c.status_execucao = 'FALHA'
""").collect()

for row in _falhas:
    _alertas.append(f"[FALHA] {row.camada.upper()} | {row.tabela_nome} | {row.mensagem_erro[:200]}")

# Verificar tabelas sem execução hoje
_tabelas_esperadas = [
    f"{BRONZE}.erp_pedidos_cabecalho",    f"{BRONZE}.erp_pedidos_itens",
    f"{BRONZE}.legado_regioes",           f"{BRONZE}.vendedores",
    f"{BRONZE}.atendimento_ocorrencias",  f"{BRONZE}.logistica_entregas",
    f"{BRONZE}.cadastro_produtos",        f"{BRONZE}.crm_clientes",
    f"{BRONZE}.comercial_canais",
    f"{SILVER}.erp_pedidos_cabecalho",    f"{SILVER}.erp_pedidos_itens",
    f"{SILVER}.legado_regioes",           f"{SILVER}.vendedores",
    f"{SILVER}.atendimento_ocorrencias",  f"{SILVER}.logistica_entregas",
    f"{SILVER}.cadastro_produtos",        f"{SILVER}.crm_clientes",
    f"{SILVER}.comercial_canais",
    f"{GOLD}.dim_clientes",   f"{GOLD}.dim_produtos",
    f"{GOLD}.dim_regioes",    f"{GOLD}.dim_canais",
    f"{GOLD}.dim_vendedores", f"{GOLD}.dim_tempo",
    f"{GOLD}.fact_pedidos",   f"{GOLD}.fact_itens_pedido",
    f"{GOLD}.fact_entregas",  f"{GOLD}.fact_ocorrencias",
]

_tabelas_executadas = {
    row.tabela_nome for row in spark.sql(f"""
        SELECT DISTINCT tabela_nome FROM {CONTROL_TABLE}
        WHERE DATE(data_execucao) = (SELECT DATE(MAX(data_execucao)) FROM {CONTROL_TABLE})
    """).collect()
}

for _t in _tabelas_esperadas:
    if _t not in _tabelas_executadas:
        _alertas.append(f"[SEM EXECUCAO] {_t} não encontrada na última execução")

# Exibir resultado
print("=" * 65)
if _alertas:
    print(f"  ALERTAS DETECTADOS ({len(_alertas)})")
    print("=" * 65)
    for _a in _alertas:
        print(f"  {_a}")
else:
    print("  PIPELINE SAUDAVEL — Nenhum alerta detectado.")
    print(f"  {len(_tabelas_esperadas)} tabelas verificadas com sucesso.")
print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tabelas com Maior Latência

# COMMAND ----------

spark.sql(f"""
    WITH ultima_exec AS (
        SELECT DATE(MAX(data_execucao)) AS ultima_data FROM {CONTROL_TABLE}
    )
    SELECT
        camada,
        tabela_nome,
        ROUND(duracao_segundos, 1)   AS duracao_s,
        FORMAT_NUMBER(linhas_processadas, 0) AS linhas,
        ROUND(linhas_processadas / NULLIF(duracao_segundos, 0), 0) AS linhas_por_segundo
    FROM {CONTROL_TABLE} c CROSS JOIN ultima_exec u
    WHERE DATE(c.data_execucao) = u.ultima_data
      AND status_execucao = 'SUCESSO'
    ORDER BY duracao_segundos DESC
    LIMIT 10
""").display()
