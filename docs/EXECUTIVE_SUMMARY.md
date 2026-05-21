# Resumo Executivo Técnico — Case Data Engineer

---

## O que foi construído

Pipeline de dados end-to-end em Databricks que transforma **9 fontes brutas heterogêneas** (CSV, JSON, NDJSON, XLSX, TXT) em um **modelo analítico dimensional (Star Schema)** pronto para consumo por Analistas de BI.

A solução cobre as áreas de **Operações**, **Comercial** e **Atendimento**, disponibilizando tabelas que permitem calcular receita, ticket médio, taxas de cancelamento/atraso e segmentar análises por região, canal, categoria e período.

---

## Arquitetura: Medallion em 3 Camadas

```
SOURCES  →  BRONZE (bruto)  →  SILVER (limpo)  →  GOLD (analítico)
  9 fontes     9 Delta Tables    9 Delta Tables    10 tabelas (4 dim + 4 fact + dim_tempo)
```

| Camada | Responsabilidade |
|--------|-----------------|
| Bronze | Ingestão fiel da fonte, sem transformações, + metadados de rastreabilidade |
| Silver | Limpeza, padronização de tipos/vocabulários, deduplicação, validações |
| Gold | Star schema: `dim_clientes`, `dim_produtos`, `dim_regioes`, `dim_canais`, `dim_vendedores`, `dim_tempo` + `fact_pedidos`, `fact_itens_pedido`, `fact_entregas`, `fact_ocorrencias` |

---

## Principais Decisões Técnicas

**1. Medallion Architecture**
Escolhida por fornecer rastreabilidade completa (Bronze preserva o dado original), isolamento de responsabilidades e facilidade de reprocessamento sem perda.

**2. Star Schema no Gold**
Preferido a uma flat wide table porque o consumidor é um Analista de BI — o modelo dimensional é o padrão de ferramentas como Power BI e Tableau, evita fanout em joins e permite análises cruzadas (pedidos × produtos × clientes) de forma intuitiva.

**3. PySpark + Spark SQL**
PySpark para lógica complexa (normalização de datas com múltiplos formatos, flatten de JSON aninhado, deduplicação com window functions). Spark SQL no Gold para legibilidade dos joins dimensionais.

**4. Delta Lake**
Garante ACID transactions (evita leituras parciais), suporta time travel para auditoria e permite reprocessamento incremental futuro via `MERGE INTO`.

---

## Principais Desafios Encontrados nos Dados

| Desafio | Impacto | Tratamento |
|---------|---------|------------|
| Múltiplos formatos de data em todas as fontes | Falha em cast direto | `coalesce(to_date(..., fmt1), to_date(..., fmt2), ...)` |
| Status com variações de case e vocabulário | Joins e filtros incorretos | Normalização via `upper()` + mapeamento para enum canônico |
| `order_id` inconsistente entre fontes | Perda de registros no join | `upper(trim(order_id))` em todas as tabelas Silver |
| Vendedores duplicados (V004, V008) | Contagem e métricas incorretas | Deduplicação com regra de negócio + log de quarentena |
| Regiões duplicadas/inconsistentes | Segmentação geográfica incorreta | Filtrar `active_flag=1`, normalizar código, descartar XX |
| JSON aninhado em produtos e entregas | Impossível usar em BI sem flatten | `col("nested.field")` + `concat_ws` para arrays |
| Valores numéricos com vírgula como decimal | Erro de cast para double | `regexp_replace(",", ".")` antes do cast |
| ~5% de pedidos sem status | Métricas de status imprecisas | Marcar como `INDEFINIDO`, manter no pipeline |

---

## Visão Geral do Modelo Final (Gold)

```
dim_tempo ────────────────────────────────────────────────────────┐
                                                                  │
dim_clientes ──┐                                                  │
dim_vendedores ┤                                                  │
dim_canais ────┼──► fact_pedidos ◄── order_key ──► fact_entregas │
dim_regioes ───┘         │                          fact_ocorrencias
                         │ order_key
                         ▼
                   fact_itens_pedido ◄── dim_produtos
```

**Métricas disponíveis diretamente no modelo:**
- Receita bruta, desconto, receita líquida (por período, região, canal, categoria)
- Ticket médio por cliente, canal, período
- Taxa de cancelamento (pedidos cancelados / total)
- Taxa de atraso (entregas com `delivered_at > promised_date`)
- Volume de ocorrências por tipo e severidade
- Evolução temporal de qualquer indicador (via `dim_tempo`)

---

## Próximos Passos Recomendados

1. **Governança:** Migrar para Unity Catalog quando disponível — habilita linhagem, controle de acesso por coluna e auditoria automática
2. **Orquestração:** Implementar Databricks Workflows para execução automática e monitoramento de falhas
3. **Qualidade contínua:** Adicionar Great Expectations ou Soda no Silver para alertas proativos de anomalias
4. **Histórico dimensional:** Implementar SCD Tipo 2 em `dim_clientes` e `dim_produtos` para preservar histórico de mudanças
5. **Marts por área:** Criar views materializadas por área de negócio (`mart_comercial`, `mart_operacoes`) com métricas pré-calculadas, reduzindo carga de trabalho do analista
6. **Reprocessamento incremental:** Substituir full-load por `MERGE INTO` no Bronze/Silver usando `last_update` como watermark
