# Case Data Engineering — Medallion + Star Schema no Databricks

> Este documento é a **especificação canônica** do projeto. Toda implementação (notebooks, YAML, configuração) deve refletir o que está aqui. Quando houver divergência, este doc é a referência — ou este doc deve ser atualizado para refletir a decisão tomada.

| Item | Detalhe |
|------|---------|
| Arquitetura | Medallion: Landing → Bronze → Silver → Gold |
| Modelagem | Star Schema Kimball — 6 dimensões + 4 fatos |
| Plataforma | Databricks Free Edition + Delta Lake + Unity Catalog |
| Orquestração | Declarative Automation Bundles — DAB (`databricks.yml`) |
| Ambientes | `dev` e `prod` — catálogos Unity Catalog independentes |
| Monitoramento | `{catalog}.monitoring.pipeline_controller` |
| Autor | Ronnan |

### Artefatos de documentação

| Artefato | Formato | Descrição |
|----------|---------|-----------|
| [docs/resumo_executivo.pptx](docs/resumo_executivo.pptx) | PowerPoint | Resumo executivo técnico — 6 slides (case item 6.7) |
| [docs/resumo_executivo.md](docs/resumo_executivo.md) | Markdown | Mesma cobertura do PPTX em formato texto |
| [docs/documentacao_tecnica.docx](docs/documentacao_tecnica.docx) | Word | Documentação técnica completa — premissas, decisões, qualidade, limitações (case item 6.6) |

---

## Sumário

1. [Arquitetura](#1-arquitetura)
2. [Contratos de Fonte](#2-contratos-de-fonte)
3. [Especificação do Pipeline — DAG](#3-especificação-do-pipeline--dag)
4. [Contratos de Dados — Schemas Gold](#4-contratos-de-dados--schemas-gold)
5. [Especificação de Transformação por Camada](#5-especificação-de-transformação-por-camada)
6. [Contratos entre Camadas](#6-contratos-entre-camadas)
7. [Referência de Configuração](#7-referência-de-configuração)
8. [Padrões e Convenções](#8-padrões-e-convenções)
9. [Critérios de Aceite por Camada](#9-critérios-de-aceite-por-camada)
10. [Protocolo de Verificação](#10-protocolo-de-verificação)
11. [Como Estender o Pipeline](#11-como-estender-o-pipeline)
12. [Guia Operacional](#12-guia-operacional)
13. [Estrutura do Repositório](#13-estrutura-do-repositório)
14. [Decisões Técnicas](#14-decisões-técnicas)
15. [Limitações e Roadmap](#15-limitações-e-roadmap)
16. [Changelog](#16-changelog)

---

## 1. Arquitetura

```
FONTES (9 arquivos)
 CSV · JSON · NDJSON · XLSX · TXT
        │
        ▼
  ┌─────────────┐
  │   LANDING   │  Cópia fiel para UC Volume. Organização por formato e sistema.
  │  6 notebooks│  Nenhuma transformação.
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │   BRONZE    │  Ingestão via AutoLoader (streaming availableNow).
  │  9 tabelas  │  Schema evolution. Metadados: dsRefChave, rastreamento_source.
  └──────┬──────┘  XLSX: openpyxl direto (sem AutoLoader).
         │
         ▼
  ┌─────────────┐
  │   SILVER    │  Limpeza, normalização de tipos e status, dedup, flags de qualidade.
  │  9 tabelas  │  MERGE INTO por chave natural. Dado confiável para consumo.
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐   ┌─────────────────────────────────┐
  │    GOLD     │   │  6 Dimensões + 4 Fatos           │
  │  10 tabelas │   │  Surrogate keys, joins resolvidos│
  └──────┬──────┘   │  Modelo pronto para BI           │
         │          └─────────────────────────────────┘
         ▼
  ┌─────────────┐
  │  ANALYTICS  │  Notebook analítico — responde às 5 perguntas de negócio do case
  └─────────────┘  diretamente nas tabelas Gold.
```

---

## 2. Contratos de Fonte

Estes são os 9 arquivos esperados em `sources/`. O pipeline falha se algum estiver ausente.

| Sistema | Arquivo | Formato | Separador | Notebook Landing |
|---------|---------|---------|-----------|-----------------|
| ERP | `erp_pedidos_cabecalho_2025.csv` | CSV | `;` | `01-LandingUploadSourcesCsv.py` |
| ERP | `erp_pedidos_itens_2025.csv` | CSV | `,` | `01-LandingUploadSourcesCsv.py` |
| Legado | `legado_regioes_pipe.txt` | TXT | `\|` | `02-LandingUploadSourcesTxt.py` |
| Vendedores | `vendedores.csv` | CSV | `;` | `01-LandingUploadSourcesCsv.py` |
| Atendimento | `atendimento_ocorrencias.ndjson` | NDJSON | — | `04-LandingUploadSourcesNdjson.py` |
| Logística | `logistica_entregas.json` | JSON multiline | — | `03-LandingUploadSourcesJson.py` |
| Produtos | `cadastro_produtos_api_dump.json` | JSON multiline | — | `03-LandingUploadSourcesJson.py` |
| CRM | `crm_clientes_export.xlsx` | XLSX | — | `05-LandingUploadSourcesXlsx.py` |
| Comercial | `comercial_canais.xlsx` | XLSX | — | `05-LandingUploadSourcesXlsx.py` |

**Destino no Volume:** `/Volumes/{catalog}/landing/storage_files/systems/{sistema}/{ano}/{mes}/{arquivo_YYYYMMDDHHMMSS}`

---

## 3. Especificação do Pipeline — DAG

### Visão geral das ondas

| Onda | Tasks | Paralelismo | Aguarda |
|------|-------|-------------|---------|
| 0 | `setup` | 1 | — |
| 1 | 6 landings | 6 paralelas | setup |
| 2 | 9 bronzes | por formato¹ | landing do seu formato |
| 3 | 9 silvers | 9 paralelas | seu bronze |
| 4 — Dims | 6 gold dims | 6 paralelas | silvers que lê² |
| 5 — Facts | 4 gold facts | 4 paralelas | dims + silvers que lê³ |

¹ Cada bronze sobe assim que o landing do seu formato conclui — sem esperar todos os outros.  
² Cada dimensão aguarda apenas os Silvers que ela efetivamente lê (não todo o Silver).  
³ Cada fato aguarda as dimensões e os Silvers que ele lê — nunca outro fato.

### Dependências detalhadas

```
setup
 ├─ landing_general
 ├─ landing_csv
 │    ├─► bronze_erp_cabecalho  ─► silver_erp_cabecalho
 │    ├─► bronze_erp_itens      ─► silver_erp_itens
 │    └─► bronze_vendedores     ─► silver_vendedores ─► gold_dim_vendedores*
 ├─ landing_txt
 │    └─► bronze_legado_regioes ─► silver_legado_regioes ─► gold_dim_regioes
 │                                                        └─► gold_dim_vendedores*
 ├─ landing_ndjson
 │    └─► bronze_atend_ocorrencias ─► silver_atend_ocorrencias ─► gold_fact_ocorrencias**
 ├─ landing_json
 │    ├─► bronze_logistica_entregas ─► silver_logistica_entregas ─► gold_fact_entregas**
 │    └─► bronze_cadastro_produtos  ─► silver_cadastro_produtos  ─► gold_dim_produtos
 └─ landing_xlsx
      ├─► bronze_crm_clientes      ─► silver_crm_clientes      ─► gold_dim_clientes
      └─► bronze_comercial_canais  ─► silver_comercial_canais  ─► gold_dim_canais
                                                               └─► gold_dim_vendedores*

setup ─────────────────────────────────────────────────────► gold_dim_tempo

* gold_dim_vendedores depende de: silver_vendedores + silver_legado_regioes + silver_comercial_canais
** gold_fact_* dependem de: gold_dim_tempo + silvers que lê + gold_dims que lê
   (ver tabela de dependências completa abaixo)
```

### Dependências completas das tasks Gold

| Task Gold | Depende de |
|-----------|-----------|
| `gold_dim_clientes` | `silver_crm_clientes` |
| `gold_dim_produtos` | `silver_cadastro_produtos` |
| `gold_dim_regioes` | `silver_legado_regioes` |
| `gold_dim_canais` | `silver_comercial_canais` |
| `gold_dim_vendedores` | `silver_vendedores` · `silver_legado_regioes` · `silver_comercial_canais` |
| `gold_dim_tempo` | `setup` |
| `gold_fact_pedidos` | `gold_dim_clientes` · `gold_dim_vendedores` · `gold_dim_tempo` · `silver_erp_cabecalho` |
| `gold_fact_itens_pedido` | `gold_dim_produtos` · `silver_erp_cabecalho` · `silver_erp_itens` |
| `gold_fact_entregas` | `gold_dim_tempo` · `silver_logistica_entregas` · `silver_erp_cabecalho` |
| `gold_fact_ocorrencias` | `gold_dim_tempo` · `silver_atend_ocorrencias` · `silver_erp_cabecalho` |

---

## 4. Contratos de Dados — Schemas Gold

Estes schemas são a especificação canônica do que cada tabela Gold produz. Toda mudança nos notebooks deve refletir aqui.

### dim_clientes

Fonte Silver: `crm_clientes` · Carga: `full`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `customer_key` | INT | PK surrogate | `row_number() OVER (ORDER BY customer_code)` |
| `customer_id` | STRING | NK | `customer_code` da fonte |
| `customer_name` | STRING | | |
| `segment` | STRING | | Segmento de mercado |
| `city` | STRING | | |
| `state` | STRING | | |
| `created_at` | TIMESTAMP | | Data de cadastro |
| `InRegistroAtivo` | INT | | Sempre `1` |
| `data_processamento` | TIMESTAMP | | Timestamp de carga |

### dim_produtos

Fonte Silver: `cadastro_produtos` · Carga: `full` · Filtro: `status = 'ATIVO'`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `product_key` | INT | PK surrogate | `row_number() OVER (ORDER BY product_code)` |
| `product_id` | STRING | NK | `product_code` da fonte |
| `product_name` | STRING | | |
| `category` | STRING | | Categoria do produto |
| `subcategory` | STRING | | |
| `family` | STRING | | |
| `list_price` | DOUBLE | | Preço de tabela |
| `currency` | STRING | | |
| `tags` | STRING | | Tags concatenadas |
| `status` | STRING | | Sempre `'ATIVO'` (filtrado) |
| `InRegistroAtivo` | INT | | Sempre `1` |
| `data_processamento` | TIMESTAMP | | |

### dim_regioes

Fonte Silver: `legado_regioes` · Carga: `full`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `region_key` | INT | PK surrogate | `row_number() OVER (ORDER BY regional_code)` |
| `regional_code` | STRING | NK | |
| `region_name` | STRING | | |
| `InRegistroAtivo` | INT | | Sempre `1` |
| `data_processamento` | TIMESTAMP | | |

### dim_canais

Fonte Silver: `comercial_canais` · Carga: `full`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `channel_key` | INT | PK surrogate | `row_number() OVER (ORDER BY channel_id)` |
| `channel_id` | STRING | NK | |
| `channel_name` | STRING | | |
| `channel_type` | STRING | | |
| `InRegistroAtivo` | INT | | Sempre `1` |
| `data_processamento` | TIMESTAMP | | |

### dim_vendedores

Fontes Silver: `vendedores` · `legado_regioes` · `comercial_canais` · Carga: `full`

> `region_key` e `channel_key` são gerados inline via CTE sobre Silver — não lê outras dims Gold.

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `seller_key` | INT | PK surrogate | `row_number() OVER (ORDER BY seller_id)` |
| `seller_id` | STRING | NK | |
| `seller_name` | STRING | | |
| `region_key` | INT | FK | Gerado inline via CTE do Silver `legado_regioes` |
| `channel_key` | INT | FK | Gerado inline via CTE do Silver `comercial_canais` |
| `hire_date` | DATE | | Data de contratação |
| `status` | STRING | | Status do vendedor |
| `InRegistroAtivo` | INT | | Sempre `1` |
| `data_processamento` | TIMESTAMP | | |

### dim_tempo

Gerada por sequência (sem Silver) · Cobertura: `2024-01-01` a `2027-12-31` · Carga: `full`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `date_key` | INT | PK | Formato `YYYYMMDD` — ex: `20250115` |
| `date` | DATE | | |
| `year` | INT | | |
| `quarter` | INT | | 1–4 |
| `month` | INT | | 1–12 |
| `month_name` | STRING | | Ex: `January` |
| `month_abbr` | STRING | | Ex: `Jan` |
| `week_of_year` | INT | | |
| `day_of_month` | INT | | 1–31 |
| `day_of_week` | INT | | 1=Domingo, 7=Sábado (padrão Spark) |
| `day_name` | STRING | | Ex: `Monday` |
| `is_weekend` | BOOLEAN | | `day_of_week IN (1, 7)` |
| `InRegistroAtivo` | INT | | Sempre `1` |
| `data_processamento` | TIMESTAMP | | |

### fact_pedidos

Fontes: Silver `erp_pedidos_cabecalho` · `dim_clientes` · `dim_vendedores` · `dim_tempo` · Carga: `delta`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `order_key` | INT | PK surrogate | `row_number() OVER (ORDER BY order_id)` |
| `order_id` | STRING | NK | |
| `order_date_key` | INT | FK dim_tempo | Data do pedido |
| `customer_key` | INT | FK dim_clientes | |
| `seller_key` | INT | FK dim_vendedores | |
| `channel_key` | INT | FK dim_canais | Herdado de `dim_vendedores` |
| `region_key` | INT | FK dim_regioes | Herdado de `dim_vendedores` |
| `status` | STRING | | Ver enum de status abaixo |
| `gross_amount` | DOUBLE | | Receita bruta |
| `discount_amount` | DOUBLE | | Desconto aplicado |
| `net_amount` | DOUBLE | | `gross_amount - discount_amount` |
| `payment_source` | STRING | | `$.source` do JSON `payment_details` |
| `payment_priority` | STRING | | `$.priority` do JSON `payment_details` |
| `due_date` | DATE | | Data prometida de entrega |
| `data_processamento` | TIMESTAMP | | |

### fact_itens_pedido

Fontes: Silver `erp_pedidos_itens` · Silver `erp_pedidos_cabecalho` (para `order_key`) · `dim_produtos` · Carga: `delta`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `item_key` | INT | PK surrogate | `row_number() OVER (ORDER BY order_id, item_seq)` |
| `order_key` | INT | FK fact_pedidos | Gerado inline via CTE do Silver `erp_pedidos_cabecalho` |
| `product_key` | INT | FK dim_produtos | |
| `order_id` | STRING | | |
| `item_seq` | INT | | Sequência do item no pedido |
| `quantity` | DOUBLE | | |
| `unit_price` | DOUBLE | | |
| `total_item` | DOUBLE | | `quantity × unit_price` |
| `item_status` | STRING | | Status do item |
| `data_processamento` | TIMESTAMP | | |

### fact_entregas

Fontes: Silver `logistica_entregas` · Silver `erp_pedidos_cabecalho` (para `order_key`) · `dim_tempo` · Carga: `delta`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `delivery_key` | INT | PK surrogate | `row_number() OVER (ORDER BY delivery_id)` |
| `order_key` | INT | FK fact_pedidos | Gerado inline via CTE do Silver `erp_pedidos_cabecalho` |
| `delivery_id` | STRING | NK | |
| `shipped_date_key` | INT | FK dim_tempo | Data de expedição |
| `delivered_date_key` | INT | FK dim_tempo | Data de entrega efetiva |
| `carrier_name` | STRING | | Transportadora |
| `carrier_mode` | STRING | | Modal (ex: RODOVIÁRIO, AÉREO) |
| `delivery_status` | STRING | | Status da entrega |
| `dest_state` | STRING | | UF destino (normalizada) |
| `dest_city` | STRING | | |
| `cost` | DOUBLE | | Custo da entrega |
| `delivery_days` | INT | | `datediff(delivered_at, shipped_at)` |
| `is_late` | BOOLEAN | | `delivery_days > 7` |
| `data_processamento` | TIMESTAMP | | |

### fact_ocorrencias

Fontes: Silver `atendimento_ocorrencias` · Silver `erp_pedidos_cabecalho` (para `order_key`) · `dim_tempo` · Carga: `delta`

| Coluna | Tipo | Chave | Descrição |
|--------|------|-------|-----------|
| `ticket_key` | INT | PK surrogate | `row_number() OVER (ORDER BY ticket_id)` |
| `order_key` | INT | FK fact_pedidos | Gerado inline via CTE do Silver |
| `created_date_key` | INT | FK dim_tempo | Data de abertura do ticket |
| `ticket_id` | STRING | NK | |
| `order_id` | STRING | | Referência ao pedido |
| `event_type` | STRING | | Tipo do evento (lowercase) |
| `severity` | STRING | | Severidade: `BAIXA` · `MEDIA` · `ALTA` · `CRITICA` |
| `status` | STRING | | Status do ticket |
| `has_event_type` | BOOLEAN | | Flag de qualidade |
| `has_severity` | BOOLEAN | | Flag de qualidade |
| `created_at` | TIMESTAMP | | |
| `data_processamento` | TIMESTAMP | | |

---

## 5. Especificação de Transformação por Camada

### Bronze — Regras

1. **Schema evolution**: novos campos da fonte são adicionados automaticamente (`addNewColumns`).
2. **`dsRefChave`**: `concat(lit('>>'), coalesce(PK1, lit('NULL')), ...)` — chave determinística de dedup.
3. **`rastreamento_source`**: `_metadata.file_path` do AutoLoader — rastreabilidade de arquivo.
4. **Sem transformação de negócio**: nenhum cast, normalização ou cálculo. Bronze é o dado bruto.
5. **XLSX**: leitura via `openpyxl.load_workbook`, carga `full` a cada execução.
6. **Upsert**: `MERGE INTO ... ON dsRefChave = dsRefChave` com `ORDER BY rastreamento_source` para keep-latest.

### Silver — Regras

| Regra | Especificação |
|-------|--------------|
| **IDs** | `upper(trim(campo_id))` em todo campo de chave natural |
| **Status (pedidos)** | `CASE WHEN vazio/nulo → 'INDEFINIDO' ELSE regexp_replace(upper(trim(status)), '\s+', '_')` |
| **Datas** | `parse_date_multi_format()` — testa 4 formatos em sequência: `yyyy-MM-dd`, `dd/MM/yyyy`, `dd-MM-yyyy`, `yyyyMMdd` |
| **Timestamps** | `parse_timestamp_multi_format()` — formatos ISO 8601 e variantes BR |
| **Decimal BR** | `cast(regexp_replace(campo, ',', '.') as double)` |
| **UF** | `normalize_uf_column(df, "dest_state")` — padroniza siglas de estados BR |
| **Flag `is_late`** | `datediff(delivered_at, shipped_at) > 7` |
| **Dedup** | `MERGE INTO ... ON chave_natural` — keep mais recente por `data_processamento` |

### Gold — Regras

| Regra | Especificação |
|-------|--------------|
| **Surrogate key** | `row_number() OVER (ORDER BY chave_natural)` — determinístico, recalculado a cada `full` |
| **Fonte única** | Cada tabela Gold lê apenas Silver — nunca outra tabela Gold |
| **`order_key` inline** | Facts que precisam de `order_key` geram via CTE: `SELECT order_id, row_number() OVER (ORDER BY order_id) AS order_key FROM silver.erp_pedidos_cabecalho` |
| **`InRegistroAtivo`** | Literal `1` em todas as dimensões |
| **Filtro em dim_produtos** | `WHERE status = 'ATIVO'` — apenas produtos ativos |
| **Dims: carga `full`** | Recriadas a cada execução — sem SCD |
| **Facts: carga `delta`** | MERGE INTO por chave natural da fonte |

### Enums e Valores Canônicos

**Status de Pedido** (`fact_pedidos.status` / `silver.erp_pedidos_cabecalho.status_order`):

| Valor | Descrição |
|-------|-----------|
| `FATURADO` | Pedido faturado |
| `EM_SEPARACAO` | Em processo de separação (espaço original normalizado para underscore) |
| `ENTREGUE` | Entregue ao cliente |
| `CANCELADO` | Pedido cancelado |
| `INDEFINIDO` | Status ausente ou não reconhecível na fonte |

**Severidade de Ocorrência** (`fact_ocorrencias.severity`):

| Valor | Prioridade |
|-------|-----------|
| `BAIXA` | 1 |
| `MEDIA` | 2 |
| `ALTA` | 3 |
| `CRITICA` | 4 |

---

## 6. Contratos entre Camadas

Define o que cada camada **garante** à camada seguinte. Qualquer violação é um bug.

### Bronze garante ao Silver
- Dado bruto preservado sem alteração de conteúdo.
- `dsRefChave` presente e não nulo em todas as linhas.
- `rastreamento_source` presente — rastreabilidade do arquivo de origem.
- Schema evoluiu automaticamente se a fonte adicionou colunas.

### Silver garante ao Gold
- Todos os IDs de chave estão em `UPPER CASE` sem espaços extras.
- Datas estão em tipo `DATE` ou `TIMESTAMP` — sem strings.
- Decimais estão em `DOUBLE` — sem vírgula BR.
- Status de pedido está em um dos 5 valores canônicos (`FATURADO`, `EM_SEPARACAO`, `ENTREGUE`, `CANCELADO`, `INDEFINIDO`).
- UFs estão normalizadas para sigla de 2 letras maiúsculas.
- `delivery_days` e `is_late` calculados e presentes em `logistica_entregas`.

### Gold garante ao Analytics
- Surrogate keys (`*_key`) são inteiros positivos gerados deterministicamente.
- FKs nas fatos referenciam surrogates que existem na dimensão correspondente (via LEFT JOIN — FK pode ser NULL se Silver não tiver correspondência).
- `InRegistroAtivo = 1` em todas as dimensões.
- Colunas de medida (`gross_amount`, `net_amount`, `cost`, etc.) são `DOUBLE`.
- `is_late` é `BOOLEAN` — nunca string.
- Percentuais no analytics sempre têm sufixo `_pct`.

---

## 7. Referência de Configuração

### Variáveis globais (`2-Variables.py`)

| Variável | Tipo | Valor exemplo | Descrição |
|----------|------|--------------|-----------|
| `CATALOG` | STRING | `dev` | Catálogo Unity Catalog — lido do widget `catalog` |
| `BRONZE_SCHEMA` | STRING | `bronze` | Fixo |
| `SILVER_SCHEMA` | STRING | `silver` | Fixo |
| `GOLD_SCHEMA` | STRING | `gold` | Fixo |
| `SOURCES_PATH` | STRING | `/Workspace/Repos/{user}/...` | Caminho dos arquivos fonte no Workspace |
| `LANDING_PATH` | STRING | `/Volumes/{catalog}/landing/storage_files/systems` | Destino do Landing |
| `CHECKPOINT_BASE` | STRING | `/Volumes/{catalog}/landing/storage_files/_checkpoints` | Checkpoints do AutoLoader |
| `SCHEMA_BASE` | STRING | `/Volumes/{catalog}/landing/storage_files/_cloudfiles_schema` | Schema inferred pelo AutoLoader |
| `CONTROL_TABLE` | STRING | `{catalog}.monitoring.pipeline_controller` | Tabela de monitoramento |
| `PIPELINE_VERSION` | STRING | `1.0.0` | |
| `STRATEGY_FULL` | STRING | `FULL` | Constante para carga full |
| `STRATEGY_DELTA` | STRING | `DELTA` | Constante para carga incremental |

**Aliases `var_*`** (compatibilidade nos notebooks de camada):
`var_environment = CATALOG`, `var_bronze_schema`, `var_silver_schema`, `var_gold_schema`, `var_bronze`, `var_silver`, `var_gold`.

### Tabelas por ambiente

```
{catalog}.bronze.{tabela}      → dado bruto
{catalog}.silver.{tabela}      → dado limpo
{catalog}.gold.{tabela}        → modelo dimensional
{catalog}.monitoring.pipeline_controller  → logs de execução
```

---

## 8. Padrões e Convenções

### Estrutura de todo notebook do pipeline

```
1. Cabeçalho Markdown — entidade, granularidade, tratamentos
2. %run ../0_config/0-Init
3. Variáveis locais: nome_catalogo, nome_tabela, tipo_carga, paths
4. Transformação (spark.sql + createOrReplaceTempView, ou AutoLoader stream)
5. Verificação de existência: spark.catalog.tableExists()
6. Gravação: process_data_load() ou MERGE INTO
7. log_table_execution() — sempre ao final
```

### Variáveis locais obrigatórias (Bronze, Silver, Gold)

```python
nome_catalogo           = var_environment
nome_tabela             = 'nome_da_tabela'          # snake_case
tipo_carga              = 'full' | 'delta'
nome_gravacao_tabela    = f'{nome_catalogo}.{var_*_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_*_schema}/{nome_tabela}'
```

### Convenções de nomenclatura

| Elemento | Convenção | Exemplo |
|----------|-----------|---------|
| Notebook | `NN-NomeCamadaEntidade.py` | `01-BronzeErpPedidosCabecalho.py` |
| Tabela Delta | `snake_case` | `erp_pedidos_cabecalho` |
| Variável local | `snake_case` | `nome_gravacao_tabela` |
| Variável global | `UPPER_SNAKE_CASE` | `CHECKPOINT_BASE` |
| Alias de compatibilidade | `var_` prefixo | `var_environment` |
| Função utilitária | `snake_case` verbo+substantivo | `parse_date_multi_format` |
| Flag booleana (coluna) | `In` + PascalCase | `InRegistroAtivo`, `InStatusInvalido` |
| Chave de dedup Bronze | sempre `dsRefChave` | — |
| Surrogate key | `{entidade}_key` | `customer_key`, `order_key` |
| Coluna percentual | sufixo `_pct` | `taxa_atraso_pct`, `desconto_pct` |

### Regras de código

- `%pip install` e imports: **apenas** em `1-Libs.py`.
- Funções utilitárias: **apenas** em `3-Functions.py`.
- `0-Init` é o único entry point — todo notebook abre com `%run ../0_config/0-Init`.
- Sem `try/except` genérico — erros propagam e quebram o job.
- Sem `.rdd` — Databricks Serverless usa Spark Connect (sem RDD API).
- Sem `.collect()` — use `dbutils.notebook.getContext()` para dados do contexto.
- Comentários apenas para o **porquê** — nunca para o **o quê**.

---

## 9. Critérios de Aceite por Camada

Define o que significa cada camada estar **concluída e correta**. Use como checklist após um run ou ao revisar uma mudança.

### Landing ✓
- [ ] 9 arquivos copiados para o Volume (`/Volumes/{catalog}/landing/storage_files/systems/`)
- [ ] Nenhum arquivo com 0 bytes
- [ ] Subdiretório por sistema criado (ex: `erp_cabecalho/`, `crm/`)
- [ ] Log com `SUCESSO` em `pipeline_controller` para todas as tasks Landing

### Bronze ✓
- [ ] 9 tabelas existem em `{catalog}.bronze.*`
- [ ] Todas têm colunas `dsRefChave` e `rastreamento_source`
- [ ] Nenhuma tabela com 0 linhas
- [ ] Schema evolution não quebrou execuções anteriores
- [ ] Log com `SUCESSO` em `pipeline_controller` para todas as tasks Bronze

### Silver ✓
- [ ] 9 tabelas existem em `{catalog}.silver.*`
- [ ] Colunas de data são tipo `DATE` ou `TIMESTAMP` — nenhuma como string
- [ ] `status_order` contém apenas: `FATURADO`, `EM_SEPARACAO`, `ENTREGUE`, `CANCELADO`, `INDEFINIDO`
- [ ] `is_late` é tipo `BOOLEAN`
- [ ] IDs de chave estão em UPPER CASE sem espaços
- [ ] Log com `SUCESSO` em `pipeline_controller` para todas as tasks Silver

### Gold — Dimensões ✓
- [ ] 6 tabelas `dim_*` existem em `{catalog}.gold.*`
- [ ] `dim_tempo` tem exatamente **1461 linhas** (2024-01-01 a 2027-12-31)
- [ ] Surrogate keys são únicos em cada dimensão
- [ ] `InRegistroAtivo = 1` em 100% das linhas de cada dimensão
- [ ] `dim_produtos` contém apenas registros com `status = 'ATIVO'`
- [ ] Log com `SUCESSO` para todas as tasks Gold Onda 1

### Gold — Fatos ✓
- [ ] 4 tabelas `fact_*` existem em `{catalog}.gold.*`
- [ ] `fact_pedidos`: linhas = linhas de `silver.erp_pedidos_cabecalho`
- [ ] FKs não-nulas > 90% em todas as facts (pedidos sem dados de dim ficam NULL por LEFT JOIN)
- [ ] `gross_amount`, `net_amount`, `cost` são tipo `DOUBLE` — sem NULL inesperado
- [ ] Log com `SUCESSO` para todas as tasks Gold Onda 2

---

## 10. Protocolo de Verificação

Execute estas queries após cada run para confirmar integridade. Todas devem retornar sem alertas.

### 1. Resumo do último run

```sql
SELECT camada, tabela_nome, status_execucao, linhas_processadas, duracao_segundos
FROM dev.monitoring.pipeline_controller
WHERE status_execucao = 'FALHA'
ORDER BY data_execucao DESC;
-- Esperado: 0 linhas
```

### 2. Contagem por camada

```sql
SELECT
  'bronze' AS camada, COUNT(*) AS tabelas,
  SUM(CASE WHEN status_execucao = 'SUCESSO' THEN 1 ELSE 0 END) AS ok
FROM dev.monitoring.pipeline_controller
WHERE camada = 'bronze'
UNION ALL
SELECT 'silver', COUNT(*),
  SUM(CASE WHEN status_execucao = 'SUCESSO' THEN 1 ELSE 0 END)
FROM dev.monitoring.pipeline_controller WHERE camada = 'silver'
UNION ALL
SELECT 'gold', COUNT(*),
  SUM(CASE WHEN status_execucao = 'SUCESSO' THEN 1 ELSE 0 END)
FROM dev.monitoring.pipeline_controller WHERE camada = 'gold';
-- Esperado: bronze=9/9, silver=9/9, gold=10/10
```

### 3. Validação de status canônico

```sql
SELECT status_order, COUNT(*) AS qtd
FROM dev.silver.erp_pedidos_cabecalho
WHERE status_order NOT IN ('FATURADO','EM_SEPARACAO','ENTREGUE','CANCELADO','INDEFINIDO')
GROUP BY status_order;
-- Esperado: 0 linhas
```

### 4. dim_tempo — cobertura de datas

```sql
SELECT COUNT(*) AS total_dias FROM dev.gold.dim_tempo;
-- Esperado: 1461
```

### 5. Integridade de surrogate keys

```sql
-- Surrogate keys únicos em fact_pedidos
SELECT COUNT(*) AS duplicatas FROM (
  SELECT order_key, COUNT(*) AS n FROM dev.gold.fact_pedidos
  GROUP BY order_key HAVING n > 1
);
-- Esperado: 0
```

### 6. Cobertura de FKs em fact_pedidos

```sql
SELECT
  round(SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS customer_null_pct,
  round(SUM(CASE WHEN seller_key   IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS seller_null_pct,
  round(SUM(CASE WHEN order_date_key IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS date_null_pct
FROM dev.gold.fact_pedidos;
-- Alerta se qualquer valor > 10%
```

---

## 11. Como Estender o Pipeline

### Adicionar uma nova fonte de dados

Siga esta sequência. Não pule etapas — cada uma garante que a próxima funcione.

**1. Arquivo fonte**
- Adicionar o arquivo em `sources/`
- Registrar em `SOURCE_MAP` em `notebooks/0_config/2-Variables.py`
- Adicionar o nome ao array `EXPECTED_FILES`

**2. Landing**
- Reusar o notebook de landing do mesmo formato se já existir
- Se for formato novo: criar `notebooks/1_landing/0N-LandingUploadSources{Formato}.py` seguindo o padrão existente
- Adicionar task no YAML dependendo de `setup`

**3. Bronze**
- Criar `notebooks/2_bronze/NN-Bronze{Sistema}.py`
- Usar `initialize_bronze_context()` com `container_source`, `nome_arquivo`, `file_name_saida`
- Para XLSX: usar padrão openpyxl (ver `08-BronzeCrmClientes.py`)
- Para demais: usar AutoLoader com `cloudFiles.format` correto
- Adicionar task no YAML dependendo do landing do seu formato

**4. Silver**
- Criar `notebooks/3_silver/NN-Silver{Sistema}.py`
- Aplicar regras: `upper(trim())` nos IDs, `parse_date_multi_format()` nas datas, `regexp_replace(',','.')` nos decimais
- MERGE INTO por chave natural com `UPDATE SET *`
- Adicionar task no YAML dependendo do seu Bronze

**5. Gold (se necessário)**
- **Nova dimensão**: criar `notebooks/4_gold/NN-GoldDim{Entidade}.py`, carga `full`, `row_number()` como surrogate key
- **Novo fato**: criar `notebooks/4_gold/NN-GoldFact{Entidade}.py`, carga `delta`, gerar FKs via LEFT JOIN ou CTE inline
- **Regra crítica**: fact nunca lê outro fact — gerar `order_key` via CTE do Silver
- Adicionar tasks no YAML na onda correta (dims antes, facts depois)

**6. Atualizar a spec**
- `README.md` seção 2 (Contratos de Fonte): nova linha na tabela
- `README.md` seção 3 (DAG): nova task e dependências
- `README.md` seção 4 (Schemas Gold): schema da nova tabela, se Gold
- `README.md` seção 5 (Transformação): regras aplicadas na Silver
- `README.md` seção 9 (Critérios de Aceite): critério para a nova camada, se aplicável
- `CLAUDE.md` seção Estado Atual: atualizar contagem de tabelas

---

## 12. Guia Operacional

### Pré-requisitos

```bash
# Databricks CLI >= 0.292.0
databricks --version

# Autenticar com profile AZDO
databricks auth login --host https://<seu-workspace>.cloud.databricks.com --profile AZDO
```

### Deploy e execução

```bash
# Validar bundle
databricks bundle validate -t dev --profile AZDO

# Deploy dev
databricks bundle deploy -t dev --profile AZDO

# Rodar pipeline
databricks bundle run pipeline_medallion_completo -t dev --profile AZDO

# Verificar status do run
databricks jobs get-run <RUN_ID> --profile AZDO --output JSON

# Deploy + run em prod
databricks bundle deploy -t prod --profile AZDO
databricks bundle run pipeline_medallion_completo -t prod --profile AZDO
```

### Setup inicial (primeira vez por ambiente)

A task `setup` cria automaticamente:
- Catálogo (`dev` ou `prod`)
- Schemas: `bronze`, `silver`, `gold`, `monitoring`
- Volume: `{catalog}.landing.storage_files`

Execuções seguintes ignoram o `IF NOT EXISTS`.

### Monitoramento

```sql
-- Última execução de cada tabela
SELECT camada, tabela_nome, status_execucao, linhas_processadas, duracao_segundos, ultima_atualizacao
FROM dev.monitoring.pipeline_controller
ORDER BY camada, tabela_nome;

-- Tasks com falha na última execução
SELECT tabela_nome, mensagem_erro, data_execucao
FROM dev.monitoring.pipeline_controller
WHERE status_execucao = 'FALHA'
ORDER BY data_execucao DESC;
```

### Tabela `pipeline_controller`

| Coluna | Descrição |
|--------|-----------|
| `tabela_nome` | Nome completo `catalog.schema.tabela` |
| `camada` | `landing` · `bronze` · `silver` · `gold` |
| `status_execucao` | `SUCESSO` · `FALHA` |
| `linhas_processadas` | Linhas escritas na execução |
| `data_execucao` | Timestamp de início |
| `ultima_atualizacao` | Timestamp de conclusão |
| `duracao_segundos` | Tempo de execução |
| `mensagem_erro` | Stack trace em caso de falha |
| `pipeline_versao` | Valor de `PIPELINE_VERSION` |

---

## 13. Estrutura do Repositório

```
case_data_engineering_20052026/
├── databricks.yml                              # Declarative Automation Bundle — targets dev e prod
├── resources/jobs/
│   └── pipeline_medallion_completo.job.yml     # DAG completo do pipeline
├── docs/                                       # Documentação de entrega
│   ├── resumo_executivo.md                     # Resumo executivo técnico em Markdown
│   ├── resumo_executivo.pptx                   # Resumo executivo — 6 slides (case item 6.7)
│   └── documentacao_tecnica.docx              # Documentação técnica completa (case item 6.6)
├── case_artifacts/
│   └── Case - Data Engineer.pdf                # Enunciado original do case
├── sources/                                    # 9 arquivos fonte originais
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
    ├── 0_config/                               # Configuração centralizada
    │   ├── 0-Init.py                           # Entry point único
    │   ├── 1-Libs.py                           # pip install + imports
    │   ├── 2-Variables.py                      # Variáveis globais e SOURCE_MAP
    │   ├── 3-Functions.py                      # Funções utilitárias
    │   ├── 5-CleanerRoutineVacuumOptimizeGlobal.py
    │   ├── 6-MonitoringLogs.py                 # pipeline_controller + log_table_execution()
    │   └── 7-SetupCatalog.py                   # Cria catalog, schemas, volume
    ├── 1_landing/                              # 6 notebooks — por formato
    │   ├── 00-LandingUploadSources.py          # Parquet (geral)
    │   ├── 01-LandingUploadSourcesCsv.py
    │   ├── 02-LandingUploadSourcesTxt.py
    │   ├── 03-LandingUploadSourcesJson.py
    │   ├── 04-LandingUploadSourcesNdjson.py
    │   └── 05-LandingUploadSourcesXlsx.py
    ├── 2_bronze/                               # 9 notebooks — ingestão fiel
    │   ├── 01-BronzeErpPedidosCabecalho.py
    │   ├── 02-BronzeErpPedidosItens.py
    │   ├── 03-BronzeLegadoRegioes.py
    │   ├── 04-BronzeVendedores.py
    │   ├── 05-BronzeAtendimentoOcorrencias.py
    │   ├── 06-BronzeLogisticaEntregas.py
    │   ├── 07-BronzeCadastroProdutos.py
    │   ├── 08-BronzeCrmClientes.py             # XLSX via openpyxl
    │   └── 09-BronzeComercialCanais.py         # XLSX via openpyxl
    ├── 3_silver/                               # 9 notebooks — limpeza e normalização
    │   ├── 01-SilverErpPedidosCabecalho.py
    │   ├── 02-SilverErpPedidosItens.py
    │   ├── 03-SilverLegadoRegioes.py
    │   ├── 04-SilverVendedores.py
    │   ├── 05-SilverAtendimentoOcorrencias.py
    │   ├── 06-SilverLogisticaEntregas.py
    │   ├── 07-SilverCadastroProdutos.py
    │   ├── 08-SilverCrmClientes.py
    │   └── 09-SilverComercialCanais.py
    ├── 4_gold/                                 # 10 notebooks — 6 dims + 4 fatos
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
    ├── 5_analytics/
    │   └── 00-AnaliseNegocio.py               # Responde às 5 perguntas do case
    └── 6_monitoring/
        └── 00-PipelineHealthAgent.py
```

---

## 14. Decisões Técnicas

| # | Decisão | Motivação |
|---|---------|-----------|
| 1 | Medallion | Rastreabilidade — Bronze preserva o dado original; reprocessamento sem perda de origem |
| 2 | Star Schema Kimball | Consumidor analítico — modelo nativo de Power BI/Tableau; joins simples |
| 3 | Unity Catalog por ambiente | Isolamento total dev/prod sem duplicar workspace; `{catalog}.schema.tabela` |
| 4 | Delta Lake | ACID, time travel, `MERGE INTO` para idempotência |
| 5 | AutoLoader (`availableNow`) | Schema evolution automática; processamento de novos arquivos sem reprocessar os já lidos |
| 6 | `dsRefChave` | Chave determinística de dedup — idempotência nas cargas Bronze independente de ordem |
| 7 | openpyxl sem pandas | Compatibilidade serverless — pandas tem overhead; openpyxl é leve para 2 arquivos XLSX |
| 8 | Spark Connect (serverless) | Sem `.rdd`, sem `sparkContext` — constraints da plataforma |
| 9 | SparkSQL no Silver/Gold | SQL mais legível que chains PySpark para JOINs complexos e CTEs |
| 10 | Gold lê apenas Silver | Facts não leem outros facts — evita dependência circular e garante paralelismo no Gold |
| 11 | Surrogate keys inline via CTE | `order_key` gerado independentemente em cada fact usando a mesma fórmula determinística sobre o Silver — sem dependência de `fact_pedidos` |
| 12 | DAG em duas ondas no Gold | Dims em paralelo (Onda 1) → Facts em paralelo (Onda 2) — cada task aguarda apenas o que realmente lê |
| 13 | Landing por formato | Separa dependências de Bronze — cada Bronze sobe assim que seu formato está pronto, sem esperar os outros |
| 14 | Checkpoints no UC Volume | `/tmp/` é efêmero por task em jobs serverless — Volume persiste entre tasks e execuções |
| 15 | `spark.catalog.tableExists()` | API Unity Catalog nativa — sem queries em `system.information_schema` |

---

## 15. Limitações e Roadmap

### Limitações conhecidas

| # | Limitação | Detalhe |
|---|-----------|---------|
| L1 | Surrogate keys não-estáveis | `row_number()` é recalculado a cada carga `full` — não são incrementais; histórico de FK pode se romper se o Silver mudar |
| L2 | XLSX sem AutoLoader | `crm_clientes` e `comercial_canais` são sempre `full` com `openpyxl` — sem rastreamento incremental |
| L3 | `dim_tempo` hard-coded | Cobertura `2024-01-01` a `2027-12-31` — dados fora desse range ficam com `date_key = NULL` |
| L4 | FK NULL em pedidos sem data | ~5% pedidos com `order_date = NULL` não resolvem `order_date_key` |
| L5 | Sem SCD Tipo 2 | Dimensões são sobrescritas a cada `full` — histórico de alterações cadastrais não é preservado |
| L6 | Sem suite de testes | Validações são feitas inline durante a transformação; sem framework formal de DQ |

### Roadmap de evolução

| # | Evolução | Impacto esperado |
|---|----------|-----------------|
| E1 | SCD Tipo 2 em `dim_clientes` e `dim_vendedores` | Preserva histórico de alterações para análises temporais |
| E2 | Lakeflow Spark Declarative Pipelines (ex-DLT) | Pipelines declarativas com expectativas de qualidade nativas e linhagem automática |
| E3 | Camada `gold_agg` | Tabelas pré-calculadas por mês/canal/região — reduz latência de dashboards |
| E4 | Alertas automáticos | Extensão do `pipeline_controller` com webhook ou Databricks SQL Alerts |
| E5 | Testes de DQ formais | Great Expectations ou Databricks DQ — completude, unicidade, integridade referencial |
| E6 | `COMMENT ON TABLE/COLUMN` | Autodocumentação das tabelas Gold no Unity Catalog |
| E7 | `dim_tempo` dinâmica | Geração baseada no range de datas real dos dados — sem hard-code |

---

## 16. Changelog

Registro de decisões e mudanças significativas no projeto. Atualizar sempre que uma seção da spec mudar.

| Data | Mudança | Impacto |
|------|---------|---------|
| 2026-05-25 | Adicionadas seções 9–11 e 16 ao README (spec-driven: critérios de aceite, verificação, extensão, changelog) | Spec mais completa seguindo princípios SDD e Harness Engineering |
| 2026-05-25 | Criado `CLAUDE.md` com instruções para agentes AI | Contexto persistente entre sessões; evita re-derivação de decisões |
| 2026-05-25 | Nomes de colunas percentuais padronizados para sufixo `_pct` no notebook analytics | Consistência de nomenclatura |
| 2026-05-25 | Silver `erp_pedidos_cabecalho`: status normalizado — `regexp_replace('\s+','_')` + `INDEFINIDO` para vazio | Eliminado `EM SEPARACAO` duplicado; status canônico garantido |
| 2026-05-23 | README reescrito como spec canônica (schemas, contratos, DAG, enums) | Documentação alinhada com estado real do código |
| 2026-05-23 | Segurança: workspace URL removida do `databricks.yml`; `.claude/` removido do tracking | Repositório seguro para publicação pública |
| 2026-05-23 | Anti-pattern Gold→Gold removido: `dim_vendedores` lê Silver; facts geram `order_key` inline via CTE | Paralelismo real no Gold; sem dependência circular |
| 2026-05-23 | DAG ativado com dependências reais por formato: Landing (6) → Bronze (9 por formato) → Silver (9) → Gold Onda 1 (6 dims) → Gold Onda 2 (4 facts) | Pipeline end-to-end funcional com 35 tasks |
| 2026-05-23 | Notebook `5_analytics/00-AnaliseNegocio.py` criado | Responde às 5 perguntas de negócio do case diretamente nas tabelas Gold |
