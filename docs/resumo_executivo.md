# Resumo Executivo Técnico

**Case Técnico — Engenheiro de Dados**
Autor: Ronnan

---

## O que foi construído

Pipeline end-to-end que transforma **9 fontes brutas heterogêneas** (CSV, JSON, NDJSON, XLSX, TXT pipe-delimited) em um **modelo analítico Star Schema** pronto para consumo por ferramentas de BI, rodando em **Databricks Free Edition** com orquestração via **Declarative Automation Bundles**.

> **Nota sobre o ambiente:** o case referencia Databricks Community Edition, que foi descontinuado e substituído pelo **Databricks Free Edition**. A solução foi construída nesse ambiente, aproveitando recursos modernos como **Declarative Automation Bundles (DAB)** — a tecnologia atual de deployment declarativo da Databricks, que substitui o modelo manual de criação de jobs. O DAB permite versionar a infraestrutura junto com o código, gerenciar múltiplos ambientes (`dev`/`prod`) via YAML e fazer deploy/rollback com um único comando CLI.

**Escopo entregue:**

| Artefato | Qtd | Descrição |
|----------|-----|-----------|
| Notebooks Landing | 6 | Upload por formato de arquivo (CSV, TXT, JSON, NDJSON, XLSX, Parquet) |
| Notebooks Bronze | 9 | Ingestão fiel via AutoLoader (streaming) + openpyxl (XLSX) |
| Notebooks Silver | 9 | Limpeza, normalização, deduplicação, flags de qualidade |
| Notebooks Gold | 10 | Star Schema — 6 dimensões + 4 fatos com surrogate keys |
| Notebook Analytics | 1 | Queries analíticas respondendo às 5 perguntas de negócio do case |
| Config centralizados | 7 | Init, Libs, Variables, Functions, Setup, Monitoring, Cleaner |
| Declarative Automation Bundle | 1 | DAG completo com 35 tasks orquestradas — targets `dev` e `prod` |

---

## Arquitetura da Solução

```
Fontes (9 arquivos)     LANDING          BRONZE            SILVER            GOLD
CSV / JSON / XLSX  →  UC Volume  →  Delta (bruto)  →  Delta (limpo)  →  Star Schema
TXT / NDJSON           por formato    AutoLoader /       normalizado       6 dim + 4 fato
                       e sistema      openpyxl                                  ↓
                                                                          Analytics (BI)
```

**DAG do job — 5 ondas de execução:**

```
setup
 ├─ landing_csv   → bronze_erp_cabecalho / bronze_erp_itens / bronze_vendedores
 ├─ landing_txt   → bronze_legado_regioes
 ├─ landing_ndjson→ bronze_atend_ocorrencias
 ├─ landing_json  → bronze_logistica_entregas / bronze_cadastro_produtos
 └─ landing_xlsx  → bronze_crm_clientes / bronze_comercial_canais
      ↓ (cada bronze dispara seu silver)
 Silver (9 paralelas, cada uma aguarda apenas seu bronze)
      ↓
 Gold Onda 1 — Dims (6 paralelas): dim_clientes, dim_produtos, dim_regioes,
                                    dim_canais, dim_vendedores, dim_tempo
      ↓
 Gold Onda 2 — Facts (4 paralelas): fact_pedidos, fact_itens_pedido,
                                     fact_entregas, fact_ocorrencias
```

Cada Bronze sobe assim que o landing do seu formato conclui — sem esperar os outros formatos. Isso maximiza o paralelismo e reduz o tempo total de pipeline.

**Camadas:**
- **Landing**: Volume Unity Catalog — repositório dos arquivos fonte organizados por sistema e data
- **Bronze**: Dado bruto sem transformações, com `dsRefChave` (dedup), `rastreamento_source` (lineage) e `data_processamento`
- **Silver**: Tipos corrigidos, status normalizados, UF padronizada, datas multi-formato parseadas, duplicatas removidas
- **Gold**: Surrogate keys, JOINs resolvidos, modelo dimensional pronto para BI
- **Analytics**: Queries diretas nas tabelas Gold respondendo às 5 perguntas de negócio

---

## Principais Decisões Técnicas

| Decisão | Motivação |
|---------|-----------|
| **Medallion 4 camadas** | Bronze preserva o original para reprocessamento sem perda; Silver isola limpeza; Gold isola modelagem; Analytics isola consumo |
| **Star Schema no Gold** | Modelo dimensional é o padrão nativo de Power BI/Tableau — analista acessa direto sem transformações |
| **Gold lê apenas Silver** | Nenhuma tabela Gold lê outra Gold — dims e facts são todos derivados diretamente do Silver |
| **Surrogate keys inline via CTE** | `row_number() OVER (ORDER BY natural_key)` replicado nas facts que precisam de `order_key` — evita dependência Gold→Gold |
| **DAG em duas ondas no Gold** | Onda 1 (dims em paralelo) → Onda 2 (facts em paralelo) — garante que dims existam quando facts forem carregadas |
| **Landing por formato** | Cada Bronze sobe assim que seu formato fica pronto — sem esperar todos os outros formatos |
| **Declarative Automation Bundles** | Infraestrutura como código — job, dependências e ambientes versionados em YAML junto com o código |
| **AutoLoader com `availableNow=True`** | Micro-batch que processa apenas arquivos novos — idempotente e eficiente |
| **`dsRefChave = >> \|\| PK`** | Chave determinística para MERGE INTO — garante idempotência em qualquer reprocessamento |
| **SparkSQL nas transformações Silver/Gold** | Mais legível que chains PySpark para JOINs complexos; SQL é a língua franca para revisão |
| **openpyxl sem pandas** | Serverless não suporta pandas por padrão; `openpyxl` + `spark.createDataFrame()` é suficiente |
| **Catálogos por ambiente** | `dev` e `prod` no mesmo workspace — isolamento total sem duplicar infra |

---

## Principais Desafios Encontrados

| Desafio | Tratamento adotado |
|---------|-------------------|
| Datas em 4+ formatos distintos | `parse_date_multi_format()` — `coalesce(to_date(f1), to_date(f2), ...)` |
| Status `EM SEPARACAO` (espaço) vs `EM_SEPARACAO` (underscore) na mesma fonte | `regexp_replace(upper(trim(status)), '\s+', '_')` — normaliza espaços para underscore |
| ~5% pedidos com status ausente ou nulo | `CASE WHEN vazio/nulo → 'INDEFINIDO'` — mantidos no pipeline com valor canônico |
| `order_id` com case e espaços inconsistentes entre ERP e outros sistemas | `upper(trim(order_id))` em todos os notebooks Silver |
| JSON aninhado multinível (`product.pricing.list_price`) | Acesso por ponto no SparkSQL com flatten explícito |
| Duas fontes ERP no mesmo formato/sistema | Subdiretórios separados para evitar conflito de schema no AutoLoader |
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

| Tabela | Granularidade | Chave PK |
|--------|--------------|----------|
| `dim_clientes` | 1 linha por cliente | `customer_key` |
| `dim_produtos` | 1 linha por produto ativo | `product_key` |
| `dim_regioes` | 1 linha por região | `region_key` |
| `dim_canais` | 1 linha por canal | `channel_key` |
| `dim_vendedores` | 1 linha por vendedor | `seller_key` |
| `dim_tempo` | 1 linha por dia (2024–2027) | `date_key` (YYYYMMDD) |
| `fact_pedidos` | 1 linha por pedido | `order_key` |
| `fact_itens_pedido` | 1 linha por item de pedido | `item_key` |
| `fact_entregas` | 1 linha por entrega | `delivery_key` |
| `fact_ocorrencias` | 1 linha por ticket de atendimento | `ticket_key` |

**Indicadores diretamente disponíveis no Gold:**
- Receita bruta, desconto e líquida por pedido — `gross_amount`, `discount_amount`, `net_amount`
- Ticket médio — `AVG(net_amount)` sobre `fact_pedidos`
- Taxa de cancelamento — `COUNT(CASE WHEN status = 'CANCELADO') / COUNT(*)`
- Taxa de atraso — `AVG(CAST(is_late AS INT))` sobre `fact_entregas`
- Volume de pedidos por região/canal/categoria/período — JOINs simples com dimensões

---

## Boas Práticas de Modelagem e Arquitetura Medallion

### Regras de dependência entre camadas

A regra fundamental do Medallion é que cada camada **só lê da camada imediatamente anterior**:

```
Landing → Bronze → Silver → Gold → Analytics
```

Violações dessa regra criam acoplamento frágil: falhas se propagam em cascata, o reprocessamento deixa de ser idempotente e o DAG se torna um grafo arbitrário difícil de manter.

### Gold: o que é e o que não é permitido

| Situação | Permitido? | Motivação |
|----------|-----------|-----------|
| Fact lê Silver + Dims | Sim | Padrão Star Schema — fact resolve surrogate keys das dims |
| Dim lê Silver | Sim | Cada dim é derivada independentemente da sua fonte Silver |
| Fact lê outra Fact | **Não** | Cria dependência em runtime; falha em cascata; DAG quebrado |
| Dim lê outra Dim (snowflake) | **Não** | Anti-padrão Kimball; desnormalizar na própria dim usando Silver |

### Surrogate keys: consistência sem dependência Gold→Gold

O problema clássico: `fact_itens_pedido` precisa do `order_key` gerado em `fact_pedidos`. A solução é **reproduzir a surrogate key via CTE no próprio notebook**, usando a mesma fórmula e a mesma fonte Silver:

```sql
WITH order_keys AS (
    SELECT order_id,
           row_number() OVER (ORDER BY order_id) AS order_key
    FROM silver.erp_pedidos_cabecalho   -- mesma fonte, mesma fórmula
)
SELECT ok.order_key, ... FROM silver.erp_pedidos_itens si
LEFT JOIN order_keys ok ON si.order_id = ok.order_id
```

Isso funciona porque a fórmula é **determinística** — dado o mesmo conjunto de `order_id` no Silver, a surrogate key gerada é sempre a mesma.

---

## Próximos Passos Recomendados

1. **SCD Tipo 2** em `dim_clientes` e `dim_vendedores` — preserva histórico de alterações cadastrais para análises "como era em tal data"

2. **Camada `gold_agg`** com tabelas pré-agregadas por mês/canal/região — reduz latência de dashboards de centenas de milissegundos para dezenas

3. **Migração para Lakeflow Spark Declarative Pipelines** — substitui o fluxo MERGE manual por pipelines declarativas com expectativas de qualidade nativas (`expect_or_drop`)

4. **Testes de qualidade formalizados** — Great Expectations ou Databricks DQ para regras de completude, unicidade e integridade referencial executadas antes da carga Gold

5. **Catálogo de dados via Unity Catalog** — `COMMENT ON TABLE/COLUMN` nas tabelas Gold para autodocumentação navegável pelo analista de BI
