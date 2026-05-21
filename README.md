# Case Data Engineering — Medallion + Star Schema no Databricks

> Pipeline end-to-end que transforma **9 fontes brutas heterogêneas** (CSV, JSON, NDJSON, XLSX, TXT pipe-delimited) em um **modelo analítico Star Schema** pronto para consumo por ferramentas de BI.

---

## Resumo Executivo

A solução cobre três domínios de negócio — **Operações**, **Comercial** e **Atendimento ao Cliente** — e disponibiliza métricas de receita, ticket médio, taxa de cancelamento/atraso e volume de ocorrências diretamente no modelo Gold.

| Item | Detalhe |
|------|---------|
| Arquitetura | Medallion (Landing → Bronze → Silver → Gold) |
| Modelagem | Star Schema — 6 dimensões + 4 fatos |
| Plataforma | Databricks (serverless) + Delta Lake |
| Orquestração | Databricks Workflows (DAB `databricks.yml`) |
| Monitoramento | Tabela controladora `pipeline_controller` + notebook agent |
| Autor | Ronnan — ronnan_ok@hotmail.com |
| Versão | 1.0.0 |

---

## Arquitetura Geral

```
FONTES (9 arquivos)          MEDALLION                     CONSUMO
──────────────────    ───────────────────────────    ─────────────────
erp_pedidos_cab  ──►
erp_pedidos_itns ──►  BRONZE (Delta / AutoLoader)
legado_regioes   ──►   9 tabelas — dado bruto     ──►  SILVER (Delta)     ──►  GOLD
vendedores       ──►   + dsRefChave + metadata          9 tabelas              Star Schema
atend_ocorrencias──►                                    limpeza,               6 dim + 4 fato
logistica_entrega──►                                    normalização,          ──────────────
cadastro_produto ──►                                    dedup, flags           Power BI
crm_clientes     ──►                                                           Tableau
comercial_canais ──►
```

### Camadas

| Camada | Responsabilidade | Estratégia de Carga |
|--------|-----------------|---------------------|
| **Landing** | Validação e organização dos arquivos fonte por sistema | `dbutils.fs.cp` para subdiretórios |
| **Bronze** | Ingestão fiel (sem transformações), metadados de rastreabilidade, dedup por `dsRefChave` | AutoLoader (`cloudFiles`) + `MERGE INTO` |
| **Silver** | Limpeza, padronização de tipos/vocabulário, deduplicação, flags de qualidade | `process_data_load` (full) ou `MERGE INTO` (delta) |
| **Gold** | Modelo dimensional — surrogate keys, joins resolvidos, métricas pré-calculadas | `process_data_load` (full) ou `MERGE INTO` (delta) |
| **Monitor** | Controller de execução — log por tabela (status, linhas, erro, tempo) | INSERT INTO `pipeline_controller` |

---

## Modelo de Dados — Star Schema

```
                    dim_tempo
                       │
dim_clientes  ─┐        │
dim_vendedores─┤        │
dim_canais    ─┼──► fact_pedidos ◄──────────── fact_entregas
dim_regioes   ─┘     (order_key)               fact_ocorrencias
                         │
                         ▼
                 fact_itens_pedido ◄── dim_produtos
```

| Tabela | Granularidade | Chave PK |
|--------|--------------|----------|
| `dim_clientes` | 1 linha por cliente | `customer_key` |
| `dim_produtos` | 1 linha por produto ativo | `product_key` |
| `dim_regioes` | 1 linha por região | `region_key` |
| `dim_canais` | 1 linha por canal de venda | `channel_key` |
| `dim_vendedores` | 1 linha por vendedor | `seller_key` |
| `dim_tempo` | 1 linha por dia (2024–2027) | `date_key` (YYYYMMDD) |
| `fact_pedidos` | 1 linha por pedido | `order_key` |
| `fact_itens_pedido` | 1 linha por item de pedido | `item_key` |
| `fact_entregas` | 1 linha por entrega | `delivery_key` |
| `fact_ocorrencias` | 1 linha por ticket de atendimento | `ticket_key` |

---

## DAG do Pipeline (Databricks Workflows)

```
landing
  └─ bronze_erp_cabecalho   ─► silver_erp_cabecalho   ─► gold_fact_pedidos ─┐
  └─ bronze_erp_itens        ─► silver_erp_itens        ─► gold_fact_itens   │
  └─ bronze_legado_regioes   ─► silver_legado_regioes   ─► gold_dim_regioes ─┤
  └─ bronze_vendedores       ─► silver_vendedores       ─► gold_dim_vendedores
  └─ bronze_atend_ocorrencias─► silver_atend_ocorrencias─► gold_fact_ocorrencias
  └─ bronze_logistica_entregas► silver_logistica_entregas► gold_fact_entregas
  └─ bronze_cadastro_produtos ─► silver_cadastro_produtos─► gold_dim_produtos
  └─ bronze_crm_clientes     ─► silver_crm_clientes     ─► gold_dim_clientes
  └─ bronze_comercial_canais  ─► silver_comercial_canais ─► gold_dim_canais
  └─────────────────────────────────────────────────────── gold_dim_tempo
```

Bronze e Silver são paralelos entre si. Fatos aguardam todas as dimensões necessárias.

---

## Estrutura do Repositório

```
case_data_engineering_20052026/
├── databricks.yml                        # Databricks Asset Bundle — dev/prod
├── sources/                              # 9 arquivos fonte originais
│   ├── erp_pedidos_cabecalho_2025.csv
│   ├── erp_pedidos_itens_2025.csv
│   ├── legado_regioes_pipe.txt
│   ├── vendedores.csv
│   ├── atendimento_ocorrencias.ndjson
│   ├── logistica_entregas.json
│   ├── cadastro_produtos_api_dump.json
│   ├── crm_clientes_export.xlsx
│   └── comercial_canais.xlsx
└── notebooks/
    ├── 0_config/                         # Configuração centralizada
    │   ├── 0-Init.py                     # Entry point — carrega Libs, Variables, Functions, Monitoring
    │   ├── 1-Libs.py                     # Imports PySpark
    │   ├── 2-Variables.py                # Catalog, schemas, paths (DBFS/Volume), estratégias
    │   ├── 3-Functions.py                # Funções utilitárias + log_table_execution()
    │   ├── 4-Config.py                   # Alias retrocompatível para 0-Init
    │   ├── 5-CleanerRoutineVacuumOptimizeGlobal.py  # VACUUM + OPTIMIZE das 27 tabelas
    │   └── 6-MonitoringLogs.py           # Controller table + funções de monitoramento
    ├── 1_landing/
    │   └── 00-LandingUploadSources.py    # Validação e organização das fontes
    ├── 2_bronze/                         # 9 notebooks — AutoLoader (CSV/JSON) + openpyxl (XLSX)
    │   ├── 01-BronzeErpPedidosCabecalho.py
    │   ├── 02-BronzeErpPedidosItens.py
    │   ├── 03-BronzeLegadoRegioes.py
    │   ├── 04-BronzeVendedores.py
    │   ├── 05-BronzeAtendimentoOcorrencias.py
    │   ├── 06-BronzeLogisticaEntregas.py
    │   ├── 07-BronzeCadastroProdutos.py
    │   ├── 08-BronzeCrmClientes.py
    │   └── 09-BronzeComercialCanais.py
    ├── 3_silver/                         # 9 notebooks — limpeza, normalização, dedup
    │   ├── 01-SilverErpPedidosCabecalho.py
    │   ├── 02-SilverErpPedidosItens.py
    │   ├── 03-SilverLegadoRegioes.py
    │   ├── 04-SilverVendedores.py
    │   ├── 05-SilverAtendimentoOcorrencias.py
    │   ├── 06-SilverLogisticaEntregas.py
    │   ├── 07-SilverCadastroProdutos.py
    │   ├── 08-SilverCrmClientes.py
    │   └── 09-SilverComercialCanais.py
    ├── 4_gold/                           # 10 notebooks — 6 dim + 4 fato
    │   ├── 01-GoldDimClientes.py
    │   ├── 02-GoldDimProdutos.py
    │   ├── 03-GoldDimRegioes.py
    │   ├── 04-GoldDimCanais.py
    │   ├── 05-GoldDimVendedores.py
    │   ├── 06-GoldDimTempo.py
    │   ├── 07-GoldFactPedidos.py
    │   ├── 08-GoldFactItensPedido.py
    │   ├── 09-GoldFactEntregas.py
    │   └── 10-GoldFactOcorrencias.py
    ├── 5_workflows/
    │   └── 00-RunFullPipeline.py         # Execução sequencial (dev/teste)
    └── 6_monitoring/
        └── 00-PipelineHealthAgent.py     # Dashboard de saúde + alertas do pipeline
```

---

## Padrão de Notebook

Todo notebook do pipeline segue este padrão:

```python
# 1. Cabeçalho Markdown (tabela Visão Geral + Histórico)
# 2. %run ../0_config/0-Init          ← entry point único de config
# 3. Parâmetros parametrizáveis       ← nome_tabela, tipo_carga, chaves
# 4. Lógica de transformação          ← Spark SQL ou PySpark
# 5. Gravação via process_data_load() ou MERGE INTO  (sempre com log)
# 6. log_table_execution()            ← registra na pipeline_controller
```

---

## Principais Desafios de Dados

| Desafio | Tratamento |
|---------|-----------|
| Múltiplos formatos de data | `parse_date_multi_format()` — coalesce de 4 formatos |
| Status com variações de case/vocabulário | `normalize_status_pedido()` → enum canônico |
| `order_id` inconsistente entre fontes | `upper(trim(order_id))` em todas as Silver |
| Vendedores duplicados | Window dedup por `seller_id`, keep mais recente |
| JSON aninhado (produtos, entregas) | `col("nested.field")` + `concat_ws` para arrays |
| Decimal com vírgula (BR) | `regexp_replace(",", ".")` + cast DoubleType |
| XLSX sem suporte no AutoLoader | `openpyxl` + `spark.createDataFrame()` (sem pandas) |
| ~5% pedidos sem status | Marcados `INDEFINIDO`, mantidos no pipeline |

---

## Quickstart

### 1. Pré-requisitos

```bash
# Instalar Databricks CLI + autenticar
pip install databricks-cli
databricks configure --token
```

### 2. Upload das fontes

```bash
# Via CLI (recomendado)
databricks fs mkdirs dbfs:/FileStore/case/sources
databricks fs cp sources/ dbfs:/FileStore/case/sources/ --recursive
```

Ou via UI: **Catalog → DBFS → `/FileStore/case/sources/` → Upload**.

### 3. Deploy via Databricks Asset Bundle

```bash
# Validar o bundle
databricks bundle validate

# Deploy no ambiente dev
databricks bundle deploy --target dev

# Executar o pipeline completo
databricks bundle run pipeline_medallion_completo --target dev
```

### 4. Execução manual (dev/teste)

```
1_landing/00-LandingUploadSources        ← organiza arquivos
5_workflows/00-RunFullPipeline           ← executa Bronze → Silver → Gold
6_monitoring/00-PipelineHealthAgent      ← visualiza resultado e saúde
```

---

## Monitoramento — pipeline_controller

Toda execução de tabela é registrada automaticamente em `workspace.default.pipeline_controller`:

| Coluna | Descrição |
|--------|-----------|
| `tabela_nome` | Nome completo da tabela (`catalog.schema.tabela`) |
| `camada` | `landing` / `bronze` / `silver` / `gold` |
| `status_execucao` | `SUCESSO` / `FALHA` |
| `linhas_processadas` | Quantidade de linhas escritas |
| `data_execucao` | Timestamp de início |
| `ultima_atualizacao` | Timestamp de conclusão |
| `duracao_segundos` | Tempo de execução |
| `mensagem_erro` | Stack trace em caso de falha |
| `pipeline_versao` | Versão do pipeline (`1.0.0`) |

```sql
-- Última execução de cada tabela
SELECT camada, tabela_nome, status_execucao, linhas_processadas, duracao_segundos
FROM workspace.default.pipeline_controller
WHERE data_execucao = (SELECT MAX(data_execucao) FROM workspace.default.pipeline_controller)
ORDER BY camada, tabela_nome;
```

---

## Decisões Técnicas

| Decisão | Motivação |
|---------|-----------|
| **Medallion** | Rastreabilidade completa: Bronze preserva o dado original, reprocessamento sem perda |
| **Star Schema** | Consumidor é Analista de BI — modelo dimensional nativo de Power BI/Tableau |
| **PySpark + Spark SQL** | PySpark para lógica complexa (window dedup, flatten JSON); SQL no Gold para legibilidade |
| **Delta Lake** | ACID, time travel para auditoria, `MERGE INTO` para reprocessamento incremental |
| **AutoLoader** | Detecção automática de novos arquivos, evolução de schema (`addNewColumns`) |
| **dsRefChave** | Chave determinística de dedup (`>> || concat(PKs)`) — idempotência nas cargas |
| **openpyxl sem pandas** | Compatibilidade serverless — evita dependência pesada para 2 arquivos XLSX |
| **DBFS para checkpoints** | `/FileStore/case/_checkpoints/` persiste entre tasks em execução serverless |

---

## Evolução Sugerida

1. **Unity Catalog** — linhagem automática, controle de acesso por coluna, auditoria nativa
2. **SCD Tipo 2** — preservar histórico em `dim_clientes` e `dim_produtos`
3. **Great Expectations / Soda** — alertas proativos de anomalias no Silver
4. **Data Marts** — views materializadas por área (`mart_comercial`, `mart_operacoes`)
5. **Reprocessamento incremental** — watermark por `last_update` substituindo full-load no Bronze/Silver
6. **Lakehouse Monitoring** — profiling automático das tabelas Gold via Databricks Monitor

---

## Documentação Adicional

- [case_artifacts/Case - Data Engineer.pdf](case_artifacts/Case%20-%20Data%20Engineer.pdf) — enunciado original
