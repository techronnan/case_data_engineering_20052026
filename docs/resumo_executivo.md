# Resumo Executivo Técnico

**Case Técnico — Engenheiro de Dados**
Autor: Ronnan — ronnan_ok@hotmail.com

---

## O que foi construído

Pipeline end-to-end que transforma **9 fontes brutas heterogêneas** (CSV, JSON, NDJSON, XLSX, TXT) em um **modelo analítico Star Schema** pronto para consumo por ferramentas de BI, rodando em **Databricks Serverless** com orquestração via **Databricks Asset Bundles**.

**Escopo entregue:**

| Artefato | Qtd | Descrição |
|----------|-----|-----------|
| Notebooks Landing | 1 | Upload e organização dos arquivos fonte no UC Volume |
| Notebooks Bronze | 9 | Ingestão fiel via AutoLoader (streaming) + openpyxl (XLSX) |
| Notebooks Silver | 9 | Limpeza, normalização, deduplicação, flags de qualidade |
| Notebooks Gold | 10 | Star Schema — 6 dimensões + 4 fatos com surrogate keys |
| Config centralizados | 7 | Init, Libs, Variables, Functions, Setup, Monitoring, Cleaner |
| Asset Bundle | 1 | Pipeline orquestrado com DAG paralelo — targets `dev` e `prod` |

---

## Arquitetura da Solução

```
Fontes (9 arquivos)     LANDING          BRONZE            SILVER            GOLD
CSV / JSON / XLSX  →  UC Volume  →  Delta (bruto)  →  Delta (limpo)  →  Star Schema
                     /Volumes/                          normalizado        6 dim + 4 fato
                                    AutoLoader /                          ↓
                                    openpyxl                         Power BI / Tableau
```

**Camadas:**
- **Landing**: Volume Unity Catalog — repositório dos arquivos fonte organizados por sistema
- **Bronze**: Dado bruto sem transformações, com `dsRefChave` (dedup), `rastreamento_source` (lineage) e `data_processamento`
- **Silver**: Tipos corrigidos, status normalizados, UF padronizada, datas multi-formato parseadas, duplicatas removidas
- **Gold**: Surrogate keys, JOINs resolvidos, modelo dimensional pronto para BI

---

## Principais Decisões Técnicas

| Decisão | Motivação |
|---------|-----------|
| **Medallion 3 camadas** | Bronze preserva o original para reprocessamento sem perda; Silver isola limpeza; Gold isola modelagem |
| **Star Schema no Gold** | Modelo dimensional é o padrão nativo de Power BI/Tableau — analista acessa direto sem transformações |
| **AutoLoader com `availableNow=True`** | Micro-batch que processa apenas arquivos novos — idempotente e eficiente |
| **`dsRefChave = >> || PK`** | Chave determinística para MERGE INTO — garante idempotência em qualquer reprocessamento |
| **SparkSQL nas transformações Silver/Gold** | Mais legível que chains PySpark para JOINs complexos; SQL é a língua franca para revisão |
| **`spark.catalog.tableExists()`** | API Unity Catalog nativa — não depende de queries em system tables |
| **openpyxl sem pandas** | Serverless não suporta pandas por padrão; `openpyxl` + `spark.createDataFrame()` é suficiente |
| **Catálogos por ambiente** | `dev` e `prod` no mesmo workspace — isolamento total sem duplicar infra |

---

## Principais Desafios Encontrados

| Desafio | Tratamento adotado |
|---------|-------------------|
| Datas em 4+ formatos distintos | `parse_date_multi_format()` — `coalesce(to_date(f1), to_date(f2), ...)` |
| Status de pedidos com variações (`cancelado`, `CANCELADO`, `Cancelado`) | `normalize_status_pedido()` → enum canônico padronizado |
| `order_id` com case e espaços inconsistentes entre ERP e outros sistemas | `upper(trim(order_id))` em todos os notebooks Silver |
| JSON aninhado multinível (`product.pricing.list_price`) | Acesso por ponto no SparkSQL com flatten explícito |
| Duas fontes ERP no mesmo formato/sistema | Subdiretórios separados para evitar conflito de schema no AutoLoader |
| ~5% pedidos sem status definido | Mantidos no pipeline com flag `InStatusInvalido = true` e valor `INDEFINIDO` |
| Checkpoints efêmeros em serverless | Movidos de `/tmp/` para UC Volume — persiste entre tasks de job |
| XLSX fora do suporte AutoLoader | Leitura via `openpyxl` com iteração por linha e `spark.createDataFrame()` |

---

## Visão Geral do Modelo Final

```
                      dim_tempo (1 linha/dia, 2024–2027)
                          │ date_key
dim_clientes ──────────┐  │
dim_vendedores ─────── ┤  │
dim_canais ────────────┼──► fact_pedidos ◄── fact_entregas
dim_regioes ───────────┘     (order_key)     fact_ocorrencias
                                 │
                                 ▼
                        fact_itens_pedido ◄── dim_produtos
```

**Indicadores diretamente disponíveis no Gold:**
- Receita bruta, desconto e líquida por pedido — campos `gross_amount`, `discount_amount`, `net_amount`
- Ticket médio — `AVG(net_amount)` sobre `fact_pedidos`
- Taxa de cancelamento — `COUNT(CASE WHEN status = 'CANCELADO')` / `COUNT(*)`
- Taxa de atraso — `AVG(is_late)` sobre `fact_entregas`
- Volume de pedidos por região/canal/categoria/período — JOINs simples com dimensões

---

## Próximos Passos Recomendados

1. **SCD Tipo 2** em `dim_clientes` e `dim_vendedores` para preservar histórico de alterações cadastrais e permitir análises "como era em tal data"

2. **Camada `gold_agg`** com tabelas pré-agregadas por mês/canal/região — reduz latência de dashboards de centenas de milissegundos para dezenas

3. **Migração para Spark Declarative Pipelines** — substitui o fluxo MERGE manual por pipelines declarativas com expectativas de qualidade (ex: `expect_or_drop`) nativas

4. **Testes de qualidade formalizados** — Great Expectations ou Databricks DQ para regras de completude, unicidade e integridade referencial executadas antes da carga Gold

5. **Catálogo de dados via Unity Catalog** — `COMMENT ON TABLE/COLUMN` nas tabelas Gold para autodocumentação navegável pelo analista de BI
