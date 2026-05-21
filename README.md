# Case Data Engineering — Medallion + Star Schema no Databricks

> Pipeline end-to-end que transforma **9 fontes brutas heterogêneas** (CSV, JSON, NDJSON, XLSX, TXT pipe-delimited) em um **modelo analítico Star Schema** pronto para consumo por ferramentas de BI.

| Item | Detalhe |
|------|---------|
| Arquitetura | Medallion (Landing → Bronze → Silver → Gold) |
| Modelagem | Star Schema — 6 dimensões + 4 fatos |
| Plataforma | Databricks Serverless + Delta Lake + Unity Catalog |
| Orquestração | Databricks Asset Bundles (`databricks.yml`) |
| Ambientes | `dev` e `prod` — catálogos independentes no mesmo workspace |
| Monitoramento | `{catalog}.monitoring.pipeline_controller` |
| Autor | Ronnan — ronnan_ok@hotmail.com |
| Versão | 1.0.0 |

---

## Arquitetura

```
FONTES (9 arquivos)           MEDALLION                          CONSUMO
───────────────────    ────────────────────────────────    ─────────────────
erp_pedidos_cab   ──►
erp_pedidos_itens ──►  BRONZE (Delta / AutoLoader)
legado_regioes    ──►   9 tabelas — dado bruto         ──►  SILVER (Delta)  ──►  GOLD
vendedores        ──►   + dsRefChave + metadados             9 tabelas            Star Schema
atend_ocorrencias ──►                                        limpeza,             6 dim + 4 fato
logistica_entrega ──►                                        normalização,        ─────────────
cadastro_produtos ──►                                        dedup, flags         Power BI
crm_clientes      ──►                                                             Tableau
comercial_canais  ──►
```

---

## Estrutura do Repositório

```
case_data_engineering_20052026/
├── databricks.yml                              # Asset Bundle — targets dev e prod
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
    ├── 0_config/                               # Configuração centralizada — não executar diretamente
    │   ├── 0-Init.py                           # Entry point: carrega Libs → Variables → Functions
    │   ├── 1-Libs.py                           # pip install + imports PySpark centralizados
    │   ├── 2-Variables.py                      # Catalog dinâmico, schemas, paths, aliases
    │   ├── 3-Functions.py                      # Funções utilitárias de parse, carga e monitoramento
    │   ├── 5-CleanerRoutineVacuumOptimizeGlobal.py  # VACUUM + OPTIMIZE — descoberta dinâmica via information_schema
    │   ├── 6-MonitoringLogs.py                 # Criação da pipeline_controller + funções de log
    │   └── 7-SetupCatalog.py                   # Setup único: cria catalog, schemas e volume
    ├── 1_landing/
    │   └── 00-LandingUploadSources.py          # Upload e organização dos arquivos fonte no Volume
    ├── 2_bronze/                               # 9 notebooks — ingestão fiel, sem transformações
    │   ├── 01-BronzeErpPedidosCabecalho.py
    │   ├── 02-BronzeErpPedidosItens.py
    │   ├── 03-BronzeLegadoRegioes.py
    │   ├── 04-BronzeVendedores.py
    │   ├── 05-BronzeAtendimentoOcorrencias.py
    │   ├── 06-BronzeLogisticaEntregas.py
    │   ├── 07-BronzeCadastroProdutos.py
    │   ├── 08-BronzeCrmClientes.py             # XLSX via openpyxl (sem AutoLoader)
    │   └── 09-BronzeComercialCanais.py         # XLSX via openpyxl (sem AutoLoader)
    ├── 3_silver/                               # 9 notebooks — limpeza, tipos, dedup, flags
    │   ├── 01-SilverErpPedidosCabecalho.py
    │   ├── 02-SilverErpPedidosItens.py
    │   ├── 03-SilverLegadoRegioes.py
    │   ├── 04-SilverVendedores.py
    │   ├── 05-SilverAtendimentoOcorrencias.py
    │   ├── 06-SilverLogisticaEntregas.py
    │   ├── 07-SilverCadastroProdutos.py
    │   ├── 08-SilverCrmClientes.py
    │   └── 09-SilverComercialCanais.py
    ├── 4_gold/                                 # 10 notebooks — 6 dimensões + 4 fatos
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
    └── 6_monitoring/
        └── 00-PipelineHealthAgent.py           # Dashboard de saúde e alertas do pipeline
```

---

## Ambientes dev e prod

O bundle define dois targets no mesmo workspace. Cada target usa um catálogo Unity Catalog independente.

| Target | Catálogo | Job |
|--------|----------|-----|
| `dev` | `dev` | `[dev] pipeline_medallion_completo` |
| `prod` | `prod` | `[prod] pipeline_medallion_completo` |

O parâmetro `catalog` é passado automaticamente pelo job para todos os notebooks via `dbutils.widgets`. As tabelas seguem o padrão `{catalog}.{schema}.{tabela}` — ex: `dev.bronze.erp_pedidos_cabecalho`.

Comandos de deploy:

```bash
databricks bundle deploy -t dev
databricks bundle deploy -t prod
```

---

## DAG do Pipeline

```
setup
  └─ landing
       ├─ bronze_erp_cabecalho
       ├─ bronze_erp_itens
       ├─ bronze_legado_regioes
       ├─ bronze_vendedores
       ├─ bronze_atend_ocorrencias
       ├─ bronze_logistica_entregas
       ├─ bronze_cadastro_produtos
       ├─ bronze_crm_clientes
       └─ bronze_comercial_canais
            └─ (todas as bronze concluídas)
                 ├─ silver_erp_cabecalho
                 ├─ silver_erp_itens
                 ├─ silver_legado_regioes
                 ├─ silver_vendedores
                 ├─ silver_atend_ocorrencias
                 ├─ silver_logistica_entregas
                 ├─ silver_cadastro_produtos
                 ├─ silver_crm_clientes
                 └─ silver_comercial_canais
                      └─ (todas as silver concluídas)
                           ├─ gold_dim_clientes
                           ├─ gold_dim_produtos
                           ├─ gold_dim_regioes
                           ├─ gold_dim_canais
                           ├─ gold_dim_vendedores
                           ├─ gold_dim_tempo
                           ├─ gold_fact_pedidos
                           ├─ gold_fact_itens_pedido
                           ├─ gold_fact_entregas
                           └─ gold_fact_ocorrencias
```

Bronze e Silver são inteiramente paralelos entre si. Gold aguarda todo o Silver concluído.

---

## Padrões por Camada

### Config (`0_config/`)

- **`0-Init`** é o único entry point — todo notebook do pipeline abre com `%run ../0_config/0-Init`.
- **`1-Libs`** contém `%pip install` como primeira célula e todos os imports PySpark. Nenhum notebook fora dessa pasta faz import ou pip install.
- **`2-Variables`** lê o catálogo via widget (`dbutils.widgets.get("catalog")`) e define todas as variáveis globais de paths, schemas e aliases.
- **`3-Functions`** centraliza toda a lógica reutilizável. Nenhuma função utilitária fica nos notebooks de camada.
- **`7-SetupCatalog`** roda apenas como primeira task do job — cria catalog, schemas e volume se não existirem. Não faz parte do `0-Init`.

### Landing (`1_landing/`)

Responsabilidade: copiar os arquivos fonte do repositório para o UC Volume do ambiente.

| Padrão | Detalhe |
|--------|---------|
| Destino | `/Volumes/{catalog}/landing/storage_files/sources/{sistema}/arquivo` |
| Organização | Um subdiretório por sistema (`erp_cabecalho/`, `erp_itens/`, `crm/`, etc.) |
| Sem transformação | Apenas cópia — nenhum parse, cast ou renomeação |
| Idempotente | Sobrescreve se o arquivo já existir |

### Bronze (`2_bronze/`)

Responsabilidade: ingestão fiel do dado bruto. Nenhuma transformação de negócio.

| Padrão | Detalhe |
|--------|---------|
| Formato de entrada | CSV, JSON, NDJSON, TXT, XLSX |
| CSV/JSON/TXT | AutoLoader (`cloudFiles`) com `availableNow=True` |
| XLSX | `openpyxl.load_workbook` + `spark.createDataFrame()` — sem pandas |
| Chave de dedup | `dsRefChave` — `concat(lit('>>'), coalesce(PKs, lit('NULL')))` |
| Colunas de metadados | `rastreamento_source` (caminho do arquivo), `data_processamento` (timestamp) |
| Estratégia de carga | `full` para XLSX; streaming `foreachBatch` + upsert para demais |
| Schema evolution | `cloudFiles.schemaEvolutionMode: addNewColumns` |
| Checkpoints | `/Volumes/{catalog}/landing/storage_files/_checkpoints/{tabela}` |
| Função de gravação | `process_data_load()` ou `upsert_delta_live()` |
| Monitoramento | `log_table_execution()` chamado após `awaitTermination()` |

Variáveis locais obrigatórias em cada notebook Bronze:

```python
nome_catalogo        = var_environment       # catálogo dinâmico
nome_tabela          = 'nome_da_tabela'
tipo_carga           = 'full' | 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'
nome_gravacao_tabela    = f'{nome_catalogo}.{var_bronze_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_bronze_schema}/{nome_tabela}'
```

### Silver (`3_silver/`)

Responsabilidade: limpeza, normalização de tipos, deduplicação e flags de qualidade.

| Padrão | Detalhe |
|--------|---------|
| Fonte | `spark.table(f'{var_environment}.{var_bronze_schema}.{nome_tabela}')` |
| Normalização de datas | `parse_date_multi_format()` — coalesce de 4 formatos |
| Normalização de status | `normalize_status_pedido()` → enum canônico |
| Normalização de IDs | `upper(trim(col("order_id")))` em todos os campos de chave |
| Decimal BR | `normalize_decimal_value()` — `regexp_replace(",", ".")` + cast DoubleType |
| Deduplicação | Window `row_number()` por chave natural, `ORDER BY data_processamento DESC` |
| Flags | Colunas booleanas prefixadas com `In` — ex: `InRegistroAtivo`, `InStatusInvalido` |
| Estratégia de carga | `delta` (MERGE INTO por `dsRefChave`) |
| Mantém `dsRefChave` | Sim — herdado do Bronze |

Variáveis locais obrigatórias em cada notebook Silver:

```python
nome_catalogo        = var_environment
nome_tabela          = 'nome_da_tabela'
tipo_carga           = 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'
nome_gravacao_tabela    = f'{nome_catalogo}.{var_silver_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_silver_schema}/{nome_tabela}'
```

### Gold (`4_gold/`)

Responsabilidade: modelo dimensional — surrogate keys, joins resolvidos, métricas pré-calculadas.

| Padrão | Detalhe |
|--------|---------|
| Fonte | Silver via `spark.table()` |
| Surrogate key | `monotonically_increasing_id()` ou `row_number()` sobre Window com `ORDER BY chave_natural` |
| Nomenclatura das PKs | `{entidade}_key` — ex: `customer_key`, `product_key` |
| FKs nas fatos | Nome idêntico à PK da dimensão correspondente |
| Tabelas de dimensão | Prefixo `dim_` — carga `full` |
| Tabelas de fato | Prefixo `fact_` — carga `delta` (MERGE INTO) |
| `InRegistroAtivo` | Presente em todas as dimensões (booleano) |
| Métricas pré-calculadas | Apenas nas fatos — ex: `total_pedido`, `dias_para_entrega` |
| Estratégia de carga | `full` para dimensões; `delta` para fatos |

Variáveis locais obrigatórias em cada notebook Gold:

```python
nome_catalogo        = var_environment
nome_tabela          = 'dim_xxx' | 'fact_xxx'
tipo_carga           = 'full' | 'delta'
chave_clusterby      = ['dsRefChave']
chave_upsert         = 'dsRefChave'
nome_gravacao_tabela    = f'{nome_catalogo}.{var_gold_schema}.{nome_tabela}'
caminho_gravacao_tabela = f'/delta/{var_gold_schema}/{nome_tabela}'
```

---

## Padrões de Código

### Estrutura de todo notebook do pipeline

```
1. Cabeçalho Markdown — tabela Visão Geral + Histórico
2. %run ../0_config/0-Init
3. Declaração de variáveis locais (nome_tabela, tipo_carga, chaves, paths)
4. Leitura da fonte
5. Transformações
6. Gravação via process_data_load() ou MERGE INTO
7. log_table_execution() — sempre ao final
```

### Convenções de nomenclatura

| Elemento | Convenção | Exemplo |
|----------|-----------|---------|
| Arquivos de notebook | `NN-NomeCamadaEntidade.py` | `01-BronzeErpPedidosCabecalho.py` |
| Tabelas Delta | `snake_case` | `erp_pedidos_cabecalho` |
| Variáveis locais | `snake_case` | `nome_gravacao_tabela` |
| Variáveis globais (config) | `UPPER_SNAKE_CASE` | `CATALOG`, `BRONZE`, `CHECKPOINT_BASE` |
| Funções | `snake_case` 3 palavras | `parse_date_multi_format`, `process_data_load` |
| Colunas de flag | `InNomeFlag` (PascalCase com prefixo `In`) | `InRegistroAtivo`, `InStatusInvalido` |
| Chave de dedup | sempre `dsRefChave` | — |
| Surrogate keys | `{entidade}_key` | `customer_key`, `seller_key` |

### Regras de escrita

- Nenhum import fora de `1-Libs.py`. Nenhum `%pip install` fora de `1-Libs.py`.
- Nenhuma função utilitária definida dentro de notebook de camada — toda lógica reutilizável vai para `3-Functions.py`.
- Sem `try/except` genérico — erros devem propagar e quebrar o job.
- Sem comentários que expliquem *o que* o código faz — apenas comentários que explicam *por que* (restrições não óbvias, workarounds).
- `dsRefChave` deve ser a última coluna adicionada no `withColumn`, sempre com `concat(lit('>>'), coalesce(...))`.
- Timestamps em Python usam `datetime.now()` — `current_timestamp()` é uma Column Spark e não pode ser usado em `Row()`.
- Sem `.rdd` — Databricks Serverless usa Spark Connect, que não suporta RDD API. Use `.isEmpty()` direto no DataFrame.

---

## Modelo de Dados — Star Schema

```
                    dim_tempo
                       │
dim_clientes  ─┐        │
dim_vendedores─┤        │
dim_canais    ─┼──► fact_pedidos ◄──────── fact_entregas
dim_regioes   ─┘     (order_key)           fact_ocorrencias
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

## Monitoramento

Toda execução de tabela é registrada em `{catalog}.monitoring.pipeline_controller`:

| Coluna | Descrição |
|--------|-----------|
| `tabela_nome` | Nome completo (`catalog.schema.tabela`) |
| `camada` | `landing` / `bronze` / `silver` / `gold` |
| `status_execucao` | `SUCESSO` / `FALHA` |
| `linhas_processadas` | Linhas escritas |
| `data_execucao` | Timestamp de início |
| `ultima_atualizacao` | Timestamp de conclusão |
| `duracao_segundos` | Tempo de execução |
| `mensagem_erro` | Stack trace em caso de falha |
| `pipeline_versao` | Versão do pipeline |

```sql
-- Última execução de cada tabela
SELECT camada, tabela_nome, status_execucao, linhas_processadas, duracao_segundos
FROM dev.monitoring.pipeline_controller
ORDER BY camada, tabela_nome;
```

---

## Quickstart

### Pré-requisitos

```bash
# Databricks CLI >= 0.292.0
databricks --version

# Autenticar
databricks auth login --host https://dbc-9940a479-70fc.cloud.databricks.com
```

### Deploy e execução

```bash
# Validar o bundle
databricks bundle validate

# Deploy no ambiente dev (cria job [dev] pipeline_medallion_completo)
databricks bundle deploy -t dev

# Executar o pipeline completo em dev
databricks bundle run pipeline_medallion_completo -t dev

# Deploy e execução em prod
databricks bundle deploy -t prod
databricks bundle run pipeline_medallion_completo -t prod
```

Na primeira execução a task `setup` cria automaticamente o catálogo, os schemas (`bronze`, `silver`, `gold`, `monitoring`) e o volume (`{catalog}.default.sources`). As execuções seguintes ignoram o `IF NOT EXISTS`.

### Execução manual (dev/teste)

```
0_config/7-SetupCatalog              ← infraestrutura (uma vez por ambiente)
1_landing/00-LandingUploadSources    ← organiza arquivos no Volume
6_monitoring/00-PipelineHealthAgent  ← visualiza saúde do pipeline
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
| Decimal com vírgula (BR) | `normalize_decimal_value()` — `regexp_replace(",", ".")` + cast DoubleType |
| XLSX sem suporte no AutoLoader | `openpyxl` + `spark.createDataFrame()` — sem pandas |
| ~5% pedidos sem status | Marcados `INDEFINIDO`, mantidos no pipeline com `InStatusInvalido = true` |
| ERP com dois arquivos no mesmo diretório | Subdiretórios separados (`erp_cabecalho/`, `erp_itens/`) para evitar conflito de schema no AutoLoader |

---

## Decisões Técnicas

| Decisão | Motivação |
|---------|-----------|
| Medallion | Rastreabilidade completa — Bronze preserva o dado original, reprocessamento sem perda |
| Star Schema | Consumidor é Analista de BI — modelo dimensional nativo de Power BI/Tableau |
| Unity Catalog com catálogos por ambiente | Isolamento total dev/prod sem duplicar workspace |
| Delta Lake | ACID, time travel para auditoria, `MERGE INTO` para idempotência |
| AutoLoader | Detecção automática de novos arquivos, evolução de schema sem intervenção |
| `dsRefChave` | Chave determinística de dedup — garante idempotência nas cargas |
| openpyxl sem pandas | Compatibilidade serverless — evita dependência pesada para 2 arquivos XLSX |
| Spark Connect (serverless) | Sem `.rdd`, sem `sparkContext` — API DataFrame e SQL apenas |
| Checkpoints no UC Volume | `/Volumes/{catalog}/landing/storage_files/_checkpoints/` persiste entre tasks serverless |
