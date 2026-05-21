# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Workflow — Pipeline Completo End-to-End
# MAGIC
# MAGIC ## Visão Geral
# MAGIC
# MAGIC | Detalhe | Informação |
# MAGIC |---------|------------|
# MAGIC | Criado Originalmente Por | Ronnan |
# MAGIC | Finalidade | Orquestrar execução completa: Bronze → Silver → Gold |
# MAGIC | Dependência | Arquivos de fonte já presentes em DBFS (executar `1_landing/00-LandingUploadSources` antes) |

# COMMAND ----------

# MAGIC %run ../0_config/4-Config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Ingestão das Fontes

# COMMAND ----------

# MAGIC %run ../2_bronze/01-BronzeErpPedidosCabecalho

# COMMAND ----------

# MAGIC %run ../2_bronze/02-BronzeErpPedidosItens

# COMMAND ----------

# MAGIC %run ../2_bronze/03-BronzeLegadoRegioes

# COMMAND ----------

# MAGIC %run ../2_bronze/04-BronzeVendedores

# COMMAND ----------

# MAGIC %run ../2_bronze/05-BronzeAtendimentoOcorrencias

# COMMAND ----------

# MAGIC %run ../2_bronze/06-BronzeLogisticaEntregas

# COMMAND ----------

# MAGIC %run ../2_bronze/07-BronzeCadastroProdutos

# COMMAND ----------

# MAGIC %run ../2_bronze/08-BronzeCrmClientes

# COMMAND ----------

# MAGIC %run ../2_bronze/09-BronzeComercialCanais

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Limpeza e Padronização

# COMMAND ----------

# MAGIC %run ../3_silver/01-SilverErpPedidosCabecalho

# COMMAND ----------

# MAGIC %run ../3_silver/02-SilverErpPedidosItens

# COMMAND ----------

# MAGIC %run ../3_silver/03-SilverLegadoRegioes

# COMMAND ----------

# MAGIC %run ../3_silver/04-SilverVendedores

# COMMAND ----------

# MAGIC %run ../3_silver/05-SilverAtendimentoOcorrencias

# COMMAND ----------

# MAGIC %run ../3_silver/06-SilverLogisticaEntregas

# COMMAND ----------

# MAGIC %run ../3_silver/07-SilverCadastroProdutos

# COMMAND ----------

# MAGIC %run ../3_silver/08-SilverCrmClientes

# COMMAND ----------

# MAGIC %run ../3_silver/09-SilverComercialCanais

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Modelagem Dimensional (Dimensões primeiro, depois Fatos)

# COMMAND ----------

# MAGIC %run ../4_gold/01-GoldDimClientes

# COMMAND ----------

# MAGIC %run ../4_gold/02-GoldDimProdutos

# COMMAND ----------

# MAGIC %run ../4_gold/03-GoldDimRegioes

# COMMAND ----------

# MAGIC %run ../4_gold/04-GoldDimCanais

# COMMAND ----------

# MAGIC %run ../4_gold/05-GoldDimVendedores

# COMMAND ----------

# MAGIC %run ../4_gold/06-GoldDimTempo

# COMMAND ----------

# MAGIC %run ../4_gold/07-GoldFactPedidos

# COMMAND ----------

# MAGIC %run ../4_gold/08-GoldFactItensPedido

# COMMAND ----------

# MAGIC %run ../4_gold/09-GoldFactEntregas

# COMMAND ----------

# MAGIC %run ../4_gold/10-GoldFactOcorrencias

# COMMAND ----------

print("=" * 55)
print("  PIPELINE CONCLUIDO COM SUCESSO")
print(f"  {PIPELINE_NAME} v{PIPELINE_VERSION}")
print("=" * 55)
