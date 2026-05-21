# Solução de Engenharia de Dados — Case Técnico

> **Plataforma:** Databricks Community Edition + Delta Lake  
> **Tecnologias:** PySpark, Spark SQL, Delta Tables, Python  
> **Paradigma de modelagem:** Medallion Architecture + Star Schema  
> **Prazo:** 4 dias corridos

---

## 1. Entendimento do Problema

A empresa possui dados distribuídos em múltiplas fontes brutas com **formatos, padrões e qualidade variados**. Não existe ainda uma base analítica consolidada e confiável. O objetivo é construir um pipeline completo que transforme essas fontes em tabelas prontas para consumo por Analistas de BI, respondendo questões de negócio sobre performance comercial, operações e atendimento.

### Consumidor Final

Analistas de BI constroem dashboards para as lideranças de **Operações**, **Comercial** e **Atendimento**. Isso exige:

- Tabelas com nomes de colunas claros e autoexplicativos
- Granularidade bem definida (sem ambiguidade)
- Relacionamentos explícitos entre entidades e transações
- Métricas pré-calculadas ou facilmente calculáveis (receita, ticket médio, taxas)

### Perguntas de Negócio Que a Solução Deve Responder

| Área | Perguntas |
|------|-----------|
| Comercial | Receita bruta/líquida por período, canal, região, categoria? Ticket médio? |
| Operações | Taxa de cancelamento? Taxa de atraso? Gargalos por transportadora/região? |
| Atendimento | Volume de ocorrências por tipo? Tickets abertos vs fechados? Regiões problemáticas? |
| Tendência | Evolução mensal de pedidos, receita e cancelamentos? |

---

## 2. Mapeamento das Fontes de Dados

### 2.1 Inventário de Fontes

| Arquivo | Formato | Separador | Entidade | Linhas (aprox.) |
|---------|---------|-----------|----------|-----------------|
| `erp_pedidos_cabecalho_2025.csv` | CSV | `;` (semicolon) | Cabeçalho de pedidos (transacional) | ~400 |
| `erp_pedidos_itens_2025.csv` | CSV | `,` (comma) | Itens de pedidos (transacional) | ~900 |
| `legado_regioes_pipe.txt` | TXT | `|` (pipe) | Regiões / hierarquia geográfica (dim) | ~9 |
| `vendedores.csv` | CSV | `;` (semicolon) | Vendedores (dim) | ~42 |
| `atendimento_ocorrencias.ndjson` | NDJSON | — | Tickets de atendimento (transacional) | ~400 |
| `logistica_entregas.json` | JSON Array | — | Entregas (transacional) | ~400 |
| `cadastro_produtos_api_dump.json` | JSON Array | — | Catálogo de produtos (dim) | ~70 |
| `crm_clientes_export.xlsx` | Excel | — | Cadastro de clientes (dim) | ~200 |
| `comercial_canais.xlsx` | Excel | — | Canais de venda (dim) | ~10 |

### 2.2 Relacionamentos Entre Fontes

```
crm_clientes ─────────────────────────────────────────────┐
                                                           │ customer_code
legado_regioes ──── vendedores ──── erp_pedidos_cabecalho ─┤ order_id
                    canal_id  |      seller_id             │
comercial_canais ───────────┘       order_id               │
                                         │                 │
                              erp_pedidos_itens ──── cadastro_produtos
                                         │ order_id
                              logistica_entregas (order_ref)
                                         │ order_id
                              atendimento_ocorrencias (order_id)
```

---

## 3. Análise de Qualidade dos Dados

Esta seção cataloga os principais problemas encontrados durante a exploração das fontes brutas.

### 3.1 `erp_pedidos_cabecalho_2025.csv`

| Problema | Exemplo | Tratamento |
|---------|---------|------------|
| Múltiplos formatos de data | `2025-07-31`, `2025/04/13`, `02/09/2025` | Normalizar via `to_date()` com múltiplos padrões |
| Status com case inconsistente | `Faturado`, `faturado`, `FATURADO` | `upper()` e mapeamento para vocabulário canônico |
| Status com nomes diferentes | `EM_SEPARACAO` vs `em separacao` | Normalizar para enum fixo |
| `order_id` com case misto | `O00001` vs `o00021` | `upper()` |
| Valor numérico com vírgula | `781,16` | `regexp_replace(',', '.')` antes do cast |
| Status nulo/vazio | 5 registros sem status | Marcar como `INDEFINIDO`, registrar em log |
| Campo `payment_details` como string JSON | `{"priority": "low", "source": "APP"}` | Extrair campos com `from_json()` no Silver |

**Vocabulário canônico de status proposto:**

| Valor bruto | Valor canônico |
|-------------|----------------|
| faturado, Faturado | FATURADO |
| em separacao, EM_SEPARACAO | EM_SEPARACAO |
| cancelado, Cancelado | CANCELADO |
| entregue, Entregue | ENTREGUE |
| (vazio/null) | INDEFINIDO |

### 3.2 `erp_pedidos_itens_2025.csv`

| Problema | Exemplo | Tratamento |
|---------|---------|------------|
| `order_id` com case misto | `O00002` vs `o00002` | `upper()` |
| `product_code` com case misto | `P0065` vs `p0065` | `upper()` |
| `unit_price` com vírgula | `"1274,78"` | Remover aspas, substituir vírgula |
| Quantidade negativa | `quantity = -1.0` | Flag `is_return = true`, manter no histórico |
| `item_status` inconsistente | `Ativo`, `ativo`, `cancelado` | `upper()` + mapeamento |
| `total_item` calculado incorretamente | Divergência entre `qty * unit_price` e `total_item` | Validar e registrar discrepâncias |

### 3.3 `legado_regioes_pipe.txt`

| Problema | Exemplo | Tratamento |
|---------|---------|------------|
| Código duplicado — mesmo `regional_code` | `SE` aparece 2x, `S` e `sul` | Deduplica por `active_flag=1` e maior especificidade |
| Código inconsistente | `S` vs `sul` | Normalizar para código padrão de 2 letras maiúsculas |
| Estado escrito por extenso | `Sta Catarina`, `sao paulo` | Normalizar para sigla UF padrão |
| Região `XX` inativa sem dados | `XX\|\|\|Sem gestor\|0` | Excluir do Silver (apenas regiões ativas e válidas) |

**Estratégia de deduplicação:** manter registros com `active_flag = 1` e código normalizado, priorizando o registro com nome mais específico.

### 3.4 `vendedores.csv`

| Problema | Exemplo | Tratamento |
|---------|---------|------------|
| `regional_code` inconsistente | `sul` vs `S` | Normalizar via mapeamento de regiões |
| Múltiplos formatos de data | `08/11/2024`, `2024-06-27`, `29/02/2024` | Normalizar; 29/02/2024 é válido (2024 = bissexto) |
| `canal_id` ausente | V007, V011, V021 | Manter null, não imputar |
| `canal_id` com case misto | `CH01`, `ch07` | `upper()` |
| `status` inconsistente | `Ativo`, `ativo`, `inativo` | `upper()` |
| **Duplicata real detectada** | V004 aparece 2x (com canal_id diferente: `CH02` e `CH99`) | Registrar conflito, manter o mais recente ou pelo `hire_date` |
| **Duplicata real detectada** | V008 aparece 2x (`Vendedor 8` e `Vendedor 8 duplicado`) | Manter primeiro registro, logar duplicata |

### 3.5 `atendimento_ocorrencias.ndjson`

| Problema | Exemplo | Tratamento |
|---------|---------|------------|
| Múltiplos formatos de data | `2025-01-04 05:00:00`, `2026/02/12`, `27/02/2025 16:00` | Normalizar com parser flexível |
| `event_type` nulo | 2 registros | Flag `has_event_type = false` |
| `severity` nula | ~30% dos registros | Manter null, flag explícita |
| `status` nulo | Alguns registros | Manter null |
| `status` com case misto | `Open`, `open`, `closed` | `upper()` |
| `severity` com case misto | `High`, `high`, `medium` | `upper()` |
| `event_type` com case misto | `Delay`, `delay`, `refund`, `troca` | `lower()` + mapeamento |
| `order_id` com case misto | `o00275` | `upper()` |

### 3.6 `logistica_entregas.json`

| Problema | Exemplo | Tratamento |
|---------|---------|------------|
| Estrutura aninhada | `carrier: {name, mode}`, `timestamps: {...}`, `destination: {...}` | Flatten com `col("carrier.name")` |
| `carrier.name` nulo | ~20% dos registros | Manter null |
| `delivery_status` nulo | ~25% dos registros | Manter null, flag |
| `carrier.mode` nulo | Alguns registros | Manter null |
| Estado escrito por extenso | `"S. Catarina"` | Normalizar para sigla UF |
| Formatos de timestamp mistos | `21/01/2026 00:00`, `2025-03-23T00:00:00` | Normalizar |
| `shipped_at == delivered_at` em D00001 | Entrega instantânea suspeita | Flag `delivery_time_days = 0` |

### 3.7 `cadastro_produtos_api_dump.json`

| Problema | Exemplo | Tratamento |
|---------|---------|------------|
| Estrutura aninhada multinível | `product: {...}`, `pricing: {...}`, `attributes: {tags: [...]}` | Flatten; tags como string concatenada ou tabela separada |
| `status` com case misto | `Ativo`, `ativo` | `upper()` |
| `tags` como array | `["b2b", "legacy", "cloud"]` | Concat com `|` para compatibilidade BI: `"b2b|legacy|cloud"` |

---

## 4. Arquitetura da Solução

### 4.1 Medallion Architecture

A solução adota a arquitetura Medallion em três camadas, padrão consolidado em ambientes Databricks com Delta Lake. Cada camada tem uma responsabilidade clara e imutável.

```
╔══════════════════════════════════════════════════════════════════════╗
║                    SOURCES (Arquivos Brutos)                        ║
║  CSV · JSON · NDJSON · XLSX · TXT  ─── pasta sources/              ║
╚══════════════════╦═══════════════════════════════════════════════════╝
                   │ Ingestão sem transformação
                   ▼
╔══════════════════════════════════════════════════════════════════════╗
║                    BRONZE (Raw Ingested)                            ║
║  Delta Tables · Schema-on-read · Dados brutos com metadados         ║
║  + _source_file + _ingested_at                                      ║
╚══════════════════╦═══════════════════════════════════════════════════╝
                   │ Limpeza · Padronização · Validação
                   ▼
╔══════════════════════════════════════════════════════════════════════╗
║                    SILVER (Cleansed & Conformed)                    ║
║  Delta Tables · Tipos corretos · Nulos tratados · Deduplicado       ║
║  Vocabulários padronizados · Chaves normalizadas                    ║
╚══════════════════╦═══════════════════════════════════════════════════╝
                   │ Modelagem dimensional · Enriquecimento
                   ▼
╔══════════════════════════════════════════════════════════════════════╗
║                    GOLD (Analytical Model)                          ║
║  Star Schema · dim_* + fact_* · Pronto para BI                     ║
║  Métricas pré-calculadas · Joins resolvidos · Nomes claros          ║
╚══════════════════════════════════════════════════════════════════════╝
                   │
                   ▼
            [ Analista de BI ]
         Dashboards · Relatórios · Análises
```

### 4.2 Por que Medallion?

| Decisão | Justificativa |
|---------|--------------|
| **Bronze imutável** | Preserva a fonte original para auditoria e reprocessamento sem perda |
| **Silver separado do Bronze** | Isola a lógica de qualidade: se a regra mudar, reprocessa só o Silver |
| **Gold orientado ao consumidor** | Analista de BI não precisa saber da complexidade das fontes |
| **Delta Lake** | ACID transactions, time travel (auditoria), schema evolution, Z-ordering para performance |
| **PySpark** | Escala horizontal, API rica para transformações complexas, integração nativa com Databricks |

### 4.3 Modelo Analítico Final (Star Schema)

O modelo Gold segue um **Star Schema** com dimensões conformed e fatos independentes entre si.

```
                        ┌─────────────────┐
                        │   dim_tempo     │
                        │─────────────────│
                        │ date_key (PK)   │
                        │ year            │
                        │ quarter         │
                        │ month           │
                        │ month_name      │
                        │ week            │
                        │ day_of_week     │
                        └────────┬────────┘
                                 │
        ┌────────────┐           │           ┌──────────────┐
        │dim_clientes│           │           │ dim_produtos │
        │────────────│           │           │──────────────│
        │customer_key│           │           │ product_key  │
        │customer_id │           │           │ product_id   │
        │name        │           │           │ name         │
        │segment     │           │           │ category     │
        │city        │           │           │ subcategory  │
        │state       │           │           │ family       │
        │region_key──┼──┐        │        ┌──┼─product_key  │
        └────────────┘  │        │        │  │ list_price   │
                        │        │        │  │ tags         │
        ┌────────────┐  │        │        │  └──────────────┘
        │ dim_regioes│  │        │        │
        │────────────│◄─┘  ┌────▼────┐   │  ┌──────────────┐
        │ region_key │     │fact_    │   │  │  dim_canais  │
        │ regional_cd│     │pedidos  │   │  │──────────────│
        │ name       │     │─────────│   │  │ channel_key  │
        │ state      │     │order_key│   │  │ channel_id   │
        │ manager    │     │order_id │   │  │ channel_name │
        └────────────┘     │date_key─┼───┘  │ channel_type │
                           │cust_key │      └──────┬───────┘
        ┌────────────┐     │seller_key│            │
        │dim_vendedor│     │channel_key─────────────┘
        │────────────│◄────│region_key│
        │ seller_key │     │status    │
        │ seller_id  │     │gross_amt │
        │ name       │     │discount  │
        │ channel_key│     │net_amt   │
        │ region_key │     │src_system│
        │ hire_date  │     └────┬─────┘
        │ status     │          │ order_key
        └────────────┘          │
                         ┌──────┴────────┐      ┌─────────────────┐
                         │fact_itens     │      │ fact_entregas   │
                         │───────────────│      │─────────────────│
                         │item_key       │      │ delivery_key    │
                         │order_key (FK) │      │ order_key (FK)  │
                         │product_key(FK)│      │ date_ship_key   │
                         │item_seq       │      │ date_del_key    │
                         │quantity       │      │ carrier_name    │
                         │unit_price     │      │ carrier_mode    │
                         │total_item     │      │ delivery_status │
                         │is_return      │      │ dest_state      │
                         │item_status    │      │ dest_city       │
                         └───────────────┘      │ cost            │
                                                │ delivery_days   │
                         ┌─────────────────┐    │ is_late         │
                         │ fact_ocorrencias│    └─────────────────┘
                         │─────────────────│
                         │ ticket_key      │
                         │ order_key (FK)  │
                         │ date_key (FK)   │
                         │ ticket_id       │
                         │ event_type      │
                         │ severity        │
                         │ status          │
                         └─────────────────┘
```

### 4.4 Granularidade das Tabelas Gold

| Tabela | Granularidade | Chave primária |
|--------|--------------|----------------|
| `dim_clientes` | 1 linha por cliente | `customer_key` |
| `dim_produtos` | 1 linha por produto | `product_key` |
| `dim_regioes` | 1 linha por região | `region_key` |
| `dim_canais` | 1 linha por canal | `channel_key` |
| `dim_vendedores` | 1 linha por vendedor | `seller_key` |
| `dim_tempo` | 1 linha por dia | `date_key` |
| `fact_pedidos` | 1 linha por pedido | `order_key` |
| `fact_itens_pedido` | 1 linha por item de pedido | `item_key` |
| `fact_entregas` | 1 linha por entrega | `delivery_key` |
| `fact_ocorrencias` | 1 linha por ticket | `ticket_key` |

---

## 5. Etapas da Implementação

### Etapa 0 — Setup do Ambiente

- Configurar catálogo/databases: `bronze`, `silver`, `gold`
- Fazer upload das fontes brutas para DBFS (`/FileStore/case/sources/`)
- Validar versão do Databricks Runtime (recomendado: 13.x LTS com Delta 2.x)

### Etapa 1 — Bronze: Ingestão Bruta

**Objetivo:** Ler cada fonte e salvar como Delta Table com schema mínimo. Zero transformações de negócio. Adicionar metadados de ingestão.

**Metadados adicionados em todas as tabelas bronze:**
```python
df = df.withColumn("_source_file", lit(source_path)) \
       .withColumn("_ingested_at", current_timestamp())
```

| Notebook | Fonte | Formato PySpark |
|----------|-------|-----------------|
| `01_bronze_erp_pedidos_cabecalho` | erp_pedidos_cabecalho_2025.csv | `spark.read.csv(..., sep=";", header=True)` |
| `02_bronze_erp_pedidos_itens` | erp_pedidos_itens_2025.csv | `spark.read.csv(..., sep=",", header=True)` |
| `03_bronze_legado_regioes` | legado_regioes_pipe.txt | `spark.read.csv(..., sep="|", header=True)` |
| `04_bronze_vendedores` | vendedores.csv | `spark.read.csv(..., sep=";", header=True)` |
| `05_bronze_ocorrencias` | atendimento_ocorrencias.ndjson | `spark.read.json(..., multiLine=False)` |
| `06_bronze_entregas` | logistica_entregas.json | `spark.read.json(..., multiLine=True)` |
| `07_bronze_produtos` | cadastro_produtos_api_dump.json | `spark.read.json(..., multiLine=True)` |
| `08_bronze_clientes` | crm_clientes_export.xlsx | `spark.read.format("com.crealytics.spark.excel")` |
| `09_bronze_canais` | comercial_canais.xlsx | `spark.read.format("com.crealytics.spark.excel")` |

> **Limitação Community Edition:** A biblioteca `spark-excel` pode não estar disponível por padrão. Alternativa: converter os XLSX para CSV localmente antes do upload, ou usar `pandas.read_excel()` + `spark.createDataFrame()`.

### Etapa 2 — Silver: Limpeza e Padronização

**Objetivo:** Corrigir problemas de qualidade, padronizar vocabulários, normalizar tipos e remover duplicatas. Cada tabela Silver tem um log de qualidade registrado.

**Transformações-chave por entidade:**

#### Silver: Pedidos Cabeçalho
```python
# Normalização de datas com múltiplos formatos
from pyspark.sql.functions import coalesce, to_date, col, upper, regexp_replace, trim

def parse_date_multi(col_name):
    return coalesce(
        to_date(col(col_name), "yyyy-MM-dd"),
        to_date(col(col_name), "yyyy/MM/dd"),
        to_date(col(col_name), "dd/MM/yyyy")
    )

# Normalização de status
status_map = {
    "FATURADO": "FATURADO",
    "EM_SEPARACAO": "EM_SEPARACAO",
    "EM SEPARACAO": "EM_SEPARACAO",
    "CANCELADO": "CANCELADO",
    "ENTREGUE": "ENTREGUE"
}

# Correção de valores numéricos com vírgula
df = df.withColumn("gross_amount",
    regexp_replace("gross_amount", ",", ".").cast("double"))
```

#### Silver: Regiões
```python
# Normalizar "sul" -> "S", garantir unicidade por regional_code
# Priorizar active_flag=1, descartar XX
df_regioes = df_regioes \
    .filter(col("active_flag") == "1") \
    .filter(col("regional_code") != "XX") \
    .withColumn("regional_code", upper(trim(col("regional_code")))) \
    .dropDuplicates(["regional_code"])
```

#### Silver: Produtos
```python
# Flatten do JSON aninhado
from pyspark.sql.functions import concat_ws

df_produtos = df_raw \
    .select(
        col("product.product_id"),
        col("product.name"),
        col("product.category"),
        col("product.subcategory"),
        upper(col("product.status")).alias("status"),
        col("pricing.list_price"),
        col("pricing.currency"),
        col("attributes.family"),
        concat_ws("|", col("attributes.tags")).alias("tags"),
        col("updated_at")
    )
```

#### Silver: Entregas
```python
# Flatten do JSON aninhado + calcular dias de entrega
from pyspark.sql.functions import datediff, when

df_entregas = df_raw \
    .select(
        col("delivery_id"),
        upper(col("order_ref")).alias("order_id"),
        col("carrier.name").alias("carrier_name"),
        col("carrier.mode").alias("carrier_mode"),
        col("delivery_status"),
        parse_timestamp("timestamps.shipped_at").alias("shipped_at"),
        parse_timestamp("timestamps.delivered_at").alias("delivered_at"),
        col("destination.state").alias("dest_state"),
        col("destination.city").alias("dest_city"),
        col("cost")
    ) \
    .withColumn("delivery_days",
        datediff(col("delivered_at"), col("shipped_at"))) \
    .withColumn("dest_state", normalize_uf(col("dest_state")))
```

### Etapa 3 — Gold: Modelagem Analítica

**Objetivo:** Construir o Star Schema a partir do Silver. Dimensões com surrogate keys, fatos com foreign keys.

#### Geração de surrogate keys
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

df_dim = df_silver \
    .withColumn("customer_key", row_number().over(
        Window.orderBy("customer_id")))
```

#### `dim_tempo` (gerada sinteticamente)
```python
from pyspark.sql.functions import sequence, explode, to_date, expr

# Gerar sequência de datas cobrindo o período do dataset
dates = spark.sql("""
    SELECT explode(sequence(
        to_date('2025-01-01'),
        to_date('2026-12-31'),
        interval 1 day
    )) as date
""")
dim_tempo = dates.select(
    date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
    year("date").alias("year"),
    quarter("date").alias("quarter"),
    month("date").alias("month"),
    date_format("date", "MMMM").alias("month_name"),
    weekofyear("date").alias("week"),
    dayofweek("date").alias("day_of_week"),
    col("date")
)
```

#### `fact_pedidos` (join de todas as dimensões)
```python
fact_pedidos = silver_pedidos \
    .join(dim_clientes, "customer_id") \
    .join(dim_vendedores, "seller_id") \
    .join(dim_regioes, silver_pedidos["region_code"] == dim_regioes["regional_code"]) \
    .join(dim_canais, "channel_id") \
    .join(dim_tempo, silver_pedidos["order_date"] == dim_tempo["date"]) \
    .select(
        monotonically_increasing_id().alias("order_key"),
        col("order_id"),
        col("date_key"),
        col("customer_key"),
        col("seller_key"),
        col("channel_key"),
        col("region_key"),
        col("status"),
        col("gross_amount"),
        col("discount_amount"),
        col("net_amount"),
        col("payment_source"),
        col("payment_priority")
    )
```

---

## 6. Estrutura do Repositório

```
case_data_engineering_20052026/
├── README.md                          # Instruções de execução e visão geral
├── docs/
│   ├── SOLUTION_DESIGN.md             # Este documento (arquitetura e decisões)
│   └── EXECUTIVE_SUMMARY.md           # Resumo executivo para apresentação
├── case_artifacts/
│   ├── Case - Data Engineer.pdf
│   └── Case - Data Sources/           # Fontes brutas originais (não modificadas)
│       ├── erp_pedidos_cabecalho_2025.csv
│       ├── erp_pedidos_itens_2025.csv
│       ├── legado_regioes_pipe.txt
│       ├── vendedores.csv
│       ├── atendimento_ocorrencias.ndjson
│       ├── logistica_entregas.json
│       ├── cadastro_produtos_api_dump.json
│       ├── crm_clientes_export.xlsx
│       └── comercial_canais.xlsx
└── notebooks/
    ├── 00_setup/
    │   └── 00_setup_environment.ipynb
    ├── 01_bronze/
    │   ├── 01_bronze_erp_pedidos_cabecalho.ipynb
    │   ├── 02_bronze_erp_pedidos_itens.ipynb
    │   ├── 03_bronze_legado_regioes.ipynb
    │   ├── 04_bronze_vendedores.ipynb
    │   ├── 05_bronze_ocorrencias.ipynb
    │   ├── 06_bronze_entregas.ipynb
    │   ├── 07_bronze_produtos.ipynb
    │   ├── 08_bronze_clientes.ipynb
    │   └── 09_bronze_canais.ipynb
    ├── 02_silver/
    │   ├── 01_silver_pedidos_cabecalho.ipynb
    │   ├── 02_silver_pedidos_itens.ipynb
    │   ├── 03_silver_regioes.ipynb
    │   ├── 04_silver_vendedores.ipynb
    │   ├── 05_silver_ocorrencias.ipynb
    │   ├── 06_silver_entregas.ipynb
    │   ├── 07_silver_produtos.ipynb
    │   ├── 08_silver_clientes.ipynb
    │   └── 09_silver_canais.ipynb
    └── 03_gold/
        ├── 01_gold_dim_clientes.ipynb
        ├── 02_gold_dim_produtos.ipynb
        ├── 03_gold_dim_regioes.ipynb
        ├── 04_gold_dim_canais.ipynb
        ├── 05_gold_dim_vendedores.ipynb
        ├── 06_gold_dim_tempo.ipynb
        ├── 07_gold_fact_pedidos.ipynb
        ├── 08_gold_fact_itens_pedido.ipynb
        ├── 09_gold_fact_entregas.ipynb
        └── 10_gold_fact_ocorrencias.ipynb
```

---

## 7. Decisões Técnicas e Justificativas

### 7.1 Por que Star Schema e não Data Vault ou Flat Wide Table?

| Abordagem | Prós | Contras | Decisão |
|-----------|------|---------|---------|
| **Star Schema** | Simples para BI, queries rápidas, intuitivo | Menos flexível para mudanças de schema | **Escolhida** — alinha com o consumidor (Analista de BI com Tableau/PowerBI) |
| Data Vault | Auditável, flexível, histórico | Complexo, difícil para BI direto, muitos joins | Não justificado para o tamanho e maturidade deste projeto |
| Flat Wide Table | Simples | Sem reusabilidade, alta redundância, difícil manutenção | Inadequado para análises cruzadas |

### 7.2 Por que separar `fact_pedidos` de `fact_itens_pedido`?

- **`fact_pedidos`** responde perguntas de nível pedido: receita por cliente, taxa de cancelamento, ticket médio
- **`fact_itens_pedido`** responde perguntas de nível produto: quais produtos mais vendidos, mix de receita por categoria
- Juntar em uma tabela única geraria **fanout** (multiplicação de métricas do cabeçalho) ao agregar por produto

### 7.3 Por que incluir `fact_entregas` e `fact_ocorrencias` separadas?

- Cada fato tem granularidade e métricas próprias (dias de entrega, custo logístico vs. volume/tipo de ocorrência)
- Permitem análises independentes sem forçar joins desnecessários
- Ambos se conectam ao fato central `fact_pedidos` via `order_key`

### 7.4 Por que normalizar `order_id` para uppercase no Silver (não no Bronze)?

- Bronze deve preservar o dado bruto para rastreabilidade
- A normalização no Silver garante que joins entre `fact_pedidos`, `fact_itens_pedido`, `fact_entregas` e `fact_ocorrencias` funcionem sem perda de registros

### 7.5 Tratamento de registros com status nulo

Pedidos sem `status` (cerca de 5 registros) recebem o valor `INDEFINIDO` e são mantidos no pipeline. **Não descartamos** pois ainda possuem dados financeiros válidos que impactam métricas de receita. O analista de BI pode filtrá-los se necessário.

### 7.6 Tratamento de itens com quantidade negativa

Itens com `quantity < 0` representam devoluções/estornos. São mantidos no dataset com flag `is_return = TRUE`. Ao calcular receita, a lógica de negócio deve decidir se inclui ou exclui devoluções — documentamos no Gold via coluna separada.

---

## 8. Validações Aplicadas no Silver

| Validação | Ação em caso de falha |
|-----------|----------------------|
| `order_id` existe na tabela de cabeçalho (itens orfãos) | Logar e manter em tabela de quarentena |
| `product_code` existe no catálogo de produtos | Logar, manter item (produto pode ter sido descontinuado) |
| `customer_code` existe no CRM | Logar e manter pedido |
| `seller_id` existe em vendedores | Logar e manter pedido |
| `regional_code` existe em regiões ativas | Logar e manter com região NULL |
| `net_amount <= gross_amount` | Logar como anomalia financeira |
| `total_item ≈ quantity * unit_price` (tolerância 0.01) | Logar discrepância, usar `total_item` da fonte como autoritativo |
| Datas de pedido antes de 2020 ou no futuro (> hoje) | Logar como dado suspeito, manter com flag |

---

## 9. Limitações da Solução

1. **Databricks Community Edition** não suporta Unity Catalog, workflows agendados (Jobs) ou Delta Sharing — a solução usa Hive Metastore e execução manual dos notebooks
2. **Arquivos XLSX** requerem biblioteca adicional (`spark-excel`) ou conversão prévia para CSV — limitação de ambiente, não de design
3. **Ausência de chave de cliente em entregas** — `logistica_entregas` referencia apenas `order_id`; para análises de cliente × logística, é necessário o join passando por `fact_pedidos`
4. **Registros sem canal de venda** (~5 vendedores sem `canal_id`) — não é possível segmentar esses pedidos por canal sem enriquecimento externo
5. **Dados simulados** — como este é um case com dados sintéticos, algumas inconsistências podem ser artefatos do gerador, não problemas reais de qualidade

---

## 10. Sugestões de Evolução

| Evolução | Benefício |
|---------|----------|
| Migrar para Unity Catalog (quando disponível) | Governança, linhagem de dados e controle de acesso granular |
| Adicionar camada de Data Quality com Great Expectations ou Soda | Monitoramento contínuo e alertas de qualidade |
| Implementar SCD Tipo 2 em dimensões de cliente e produto | Preservar histórico de mudanças (ex: cliente mudou de segmento) |
| Orquestrar com Databricks Workflows ou Apache Airflow | Execução automática, monitoramento e retry |
| Adicionar Z-ordering em `fact_pedidos` por `order_date` | Melhora de performance em queries filtradas por data |
| Criar camada de `mart_` por área de negócio | Views/tabelas pré-agregadas por área (Comercial, Operações, Atendimento) |

---

## 11. Critérios de Avaliação vs. Solução

| Critério avaliado | Como esta solução atende |
|------------------|--------------------------|
| Estruturar solução de engenharia de dados | Medallion em 3 camadas com responsabilidades claras |
| Domínio de Python/PySpark e Databricks | PySpark para todas as transformações, Spark SQL para legibilidade no Gold |
| Qualidade e clareza das transformações | Silver com tratamentos documentados e logs de qualidade |
| Modelagem analítica orientada a BI | Star Schema com dimensões conformed e fatos independentes |
| Investigação e tratamento de qualidade | Seção 3 cataloga todos os problemas encontrados e estratégias adotadas |
| Alinhamento com necessidades do negócio | Modelo permite calcular todas as métricas listadas no case diretamente |
| Organização do repositório | Estrutura de pastas clara com notebooks numerados e docs separados |
| Clareza da documentação | Este documento cobre arquitetura, decisões e limitações |
