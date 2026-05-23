# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Análise de Negócio — Respostas às Perguntas do Case
# MAGIC
# MAGIC Notebook analítico que responde, via queries diretas nas tabelas Gold, às 5 perguntas
# MAGIC de negócio propostas no case:
# MAGIC
# MAGIC 1. Como o negócio performou no período analisado?
# MAGIC 2. Quais regiões, canais e categorias apresentam melhor e pior desempenho?
# MAGIC 3. Onde estão os principais gargalos operacionais?
# MAGIC 4. Existem sinais de perda de receita ou ineficiência?
# MAGIC 5. Quais ações práticas poderiam ser priorizadas pela liderança?

# COMMAND ----------

# MAGIC %run ../0_config/0-Init

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — Registra views das tabelas Gold

# COMMAND ----------

spark.table(f'{var_environment}.{var_gold_schema}.fact_pedidos').createOrReplaceTempView('fact_pedidos')
spark.table(f'{var_environment}.{var_gold_schema}.fact_itens_pedido').createOrReplaceTempView('fact_itens_pedido')
spark.table(f'{var_environment}.{var_gold_schema}.fact_entregas').createOrReplaceTempView('fact_entregas')
spark.table(f'{var_environment}.{var_gold_schema}.fact_ocorrencias').createOrReplaceTempView('fact_ocorrencias')
spark.table(f'{var_environment}.{var_gold_schema}.dim_clientes').createOrReplaceTempView('dim_clientes')
spark.table(f'{var_environment}.{var_gold_schema}.dim_produtos').createOrReplaceTempView('dim_produtos')
spark.table(f'{var_environment}.{var_gold_schema}.dim_regioes').createOrReplaceTempView('dim_regioes')
spark.table(f'{var_environment}.{var_gold_schema}.dim_canais').createOrReplaceTempView('dim_canais')
spark.table(f'{var_environment}.{var_gold_schema}.dim_vendedores').createOrReplaceTempView('dim_vendedores')
spark.table(f'{var_environment}.{var_gold_schema}.dim_tempo').createOrReplaceTempView('dim_tempo')

print("✓ Views registradas | Gold pronto para análise")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Como o negócio performou no período analisado?
# MAGIC ### 1.1 KPIs Gerais

# COMMAND ----------

display(spark.sql("""
    SELECT
        COUNT(DISTINCT order_id)                                        AS total_pedidos,
        round(SUM(gross_amount), 2)                                     AS receita_bruta,
        round(SUM(discount_amount), 2)                                  AS total_descontos,
        round(SUM(net_amount), 2)                                       AS receita_liquida,
        round(AVG(net_amount), 2)                                       AS ticket_medio,
        round(SUM(discount_amount) / NULLIF(SUM(gross_amount), 0) * 100, 1) AS pct_desconto,
        round(COUNT(CASE WHEN status = 'CANCELADO' THEN 1 END)
              / COUNT(*) * 100, 1)                                      AS taxa_cancelamento_pct
    FROM fact_pedidos
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Distribuição de Status dos Pedidos

# COMMAND ----------

display(spark.sql("""
    SELECT
        status,
        COUNT(*)                                              AS qtd_pedidos,
        round(SUM(net_amount), 2)                            AS receita_liquida,
        round(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1)    AS pct_total
    FROM fact_pedidos
    GROUP BY status
    ORDER BY qtd_pedidos DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Evolução Mensal — Receita e Volume de Pedidos

# COMMAND ----------

display(spark.sql("""
    SELECT
        t.year,
        t.month,
        t.month_abbr,
        COUNT(DISTINCT fp.order_id)     AS total_pedidos,
        round(SUM(fp.net_amount), 2)    AS receita_liquida,
        round(AVG(fp.net_amount), 2)    AS ticket_medio,
        COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END) AS pedidos_cancelados
    FROM fact_pedidos fp
    JOIN dim_tempo t ON fp.order_date_key = t.date_key
    GROUP BY t.year, t.month, t.month_abbr
    ORDER BY t.year, t.month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Quais regiões, canais e categorias apresentam melhor e pior desempenho?
# MAGIC ### 2.1 Desempenho por Região

# COMMAND ----------

display(spark.sql("""
    SELECT
        coalesce(r.region_name, 'Sem Região')        AS regiao,
        COUNT(DISTINCT fp.order_id)                  AS total_pedidos,
        round(SUM(fp.net_amount), 2)                 AS receita_liquida,
        round(AVG(fp.net_amount), 2)                 AS ticket_medio,
        round(COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END)
              / COUNT(*) * 100, 1)                   AS taxa_cancelamento_pct
    FROM fact_pedidos fp
    LEFT JOIN dim_regioes r ON fp.region_key = r.region_key
    GROUP BY r.region_name
    ORDER BY receita_liquida DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Desempenho por Canal de Venda

# COMMAND ----------

display(spark.sql("""
    SELECT
        coalesce(c.channel_name, 'Sem Canal')        AS canal,
        c.channel_type                               AS tipo_canal,
        COUNT(DISTINCT fp.order_id)                  AS total_pedidos,
        round(SUM(fp.net_amount), 2)                 AS receita_liquida,
        round(AVG(fp.net_amount), 2)                 AS ticket_medio,
        round(COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END)
              / COUNT(*) * 100, 1)                   AS taxa_cancelamento_pct
    FROM fact_pedidos fp
    LEFT JOIN dim_canais c ON fp.channel_key = c.channel_key
    GROUP BY c.channel_name, c.channel_type
    ORDER BY receita_liquida DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Desempenho por Categoria de Produto

# COMMAND ----------

display(spark.sql("""
    SELECT
        coalesce(p.category, 'Sem Categoria')   AS categoria,
        COUNT(DISTINCT fi.order_id)             AS total_pedidos,
        SUM(fi.quantity)                        AS total_itens_vendidos,
        round(SUM(fi.total_item), 2)            AS receita_bruta_itens,
        round(AVG(fi.unit_price), 2)            AS preco_medio_unitario,
        round(SUM(fi.total_item)
              / NULLIF(SUM(fi.quantity), 0), 2) AS ticket_por_item
    FROM fact_itens_pedido fi
    LEFT JOIN dim_produtos p ON fi.product_key = p.product_key
    GROUP BY p.category
    ORDER BY receita_bruta_itens DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Top 10 Produtos por Receita

# COMMAND ----------

display(spark.sql("""
    SELECT
        p.product_name,
        p.category,
        p.subcategory,
        SUM(fi.quantity)            AS total_itens_vendidos,
        round(SUM(fi.total_item), 2) AS receita_total
    FROM fact_itens_pedido fi
    JOIN dim_produtos p ON fi.product_key = p.product_key
    GROUP BY p.product_name, p.category, p.subcategory
    ORDER BY receita_total DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Desempenho por Segmento de Cliente

# COMMAND ----------

display(spark.sql("""
    SELECT
        coalesce(cli.segment, 'Sem Segmento')    AS segmento,
        COUNT(DISTINCT fp.order_id)              AS total_pedidos,
        round(SUM(fp.net_amount), 2)             AS receita_liquida,
        round(AVG(fp.net_amount), 2)             AS ticket_medio
    FROM fact_pedidos fp
    LEFT JOIN dim_clientes cli ON fp.customer_key = cli.customer_key
    GROUP BY cli.segment
    ORDER BY receita_liquida DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Onde estão os principais gargalos operacionais?
# MAGIC ### 3.1 Entregas — Taxa de Atraso e Status

# COMMAND ----------

display(spark.sql("""
    SELECT
        COUNT(*)                                                        AS total_entregas,
        SUM(CAST(is_late AS INT))                                       AS entregas_atrasadas,
        round(SUM(CAST(is_late AS INT)) / COUNT(*) * 100, 1)           AS taxa_atraso_pct,
        round(AVG(delivery_days), 1)                                    AS prazo_medio_dias,
        round(AVG(CASE WHEN is_late THEN delivery_days END), 1)         AS prazo_medio_atrasados,
        round(AVG(CASE WHEN NOT is_late THEN delivery_days END), 1)     AS prazo_medio_no_prazo
    FROM fact_entregas
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Atraso por Transportadora

# COMMAND ----------

display(spark.sql("""
    SELECT
        carrier_name                                                  AS transportadora,
        carrier_mode                                                  AS modal,
        COUNT(*)                                                      AS total_entregas,
        SUM(CAST(is_late AS INT))                                     AS entregas_atrasadas,
        round(SUM(CAST(is_late AS INT)) / COUNT(*) * 100, 1)         AS taxa_atraso_pct,
        round(AVG(delivery_days), 1)                                  AS prazo_medio_dias,
        round(AVG(cost), 2)                                           AS custo_medio
    FROM fact_entregas
    GROUP BY carrier_name, carrier_mode
    ORDER BY taxa_atraso_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Atraso por Região de Destino

# COMMAND ----------

display(spark.sql("""
    SELECT
        dest_state                                                    AS estado_destino,
        COUNT(*)                                                      AS total_entregas,
        SUM(CAST(is_late AS INT))                                     AS entregas_atrasadas,
        round(SUM(CAST(is_late AS INT)) / COUNT(*) * 100, 1)         AS taxa_atraso_pct,
        round(AVG(delivery_days), 1)                                  AS prazo_medio_dias
    FROM fact_entregas
    GROUP BY dest_state
    ORDER BY taxa_atraso_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Ocorrências de Atendimento por Tipo e Severidade

# COMMAND ----------

display(spark.sql("""
    SELECT
        event_type                                                        AS tipo_ocorrencia,
        severity                                                          AS severidade,
        status                                                            AS status_ticket,
        COUNT(*)                                                          AS total_ocorrencias,
        round(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1)                 AS pct_total
    FROM fact_ocorrencias
    GROUP BY event_type, severity, status
    ORDER BY total_ocorrencias DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Pedidos com Mais Ocorrências (Top 10)

# COMMAND ----------

display(spark.sql("""
    SELECT
        fo.order_id,
        COUNT(*)                        AS total_tickets,
        COUNT(CASE WHEN fo.severity = 'ALTA' OR fo.severity = 'CRITICA' THEN 1 END) AS tickets_criticos
    FROM fact_ocorrencias fo
    GROUP BY fo.order_id
    ORDER BY total_tickets DESC
    LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Existem sinais de perda de receita ou ineficiência?
# MAGIC ### 4.1 Pedidos Cancelados — Receita Perdida por Período

# COMMAND ----------

display(spark.sql("""
    SELECT
        t.year,
        t.month_abbr,
        COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END)            AS pedidos_cancelados,
        round(SUM(CASE WHEN fp.status = 'CANCELADO'
                       THEN fp.gross_amount ELSE 0 END), 2)            AS receita_perdida,
        round(COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END)
              / COUNT(*) * 100, 1)                                      AS taxa_cancelamento_pct
    FROM fact_pedidos fp
    JOIN dim_tempo t ON fp.order_date_key = t.date_key
    GROUP BY t.year, t.month, t.month_abbr
    ORDER BY t.year, t.month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Análise de Descontos — Eficiência Comercial

# COMMAND ----------

display(spark.sql("""
    SELECT
        coalesce(c.channel_name, 'Sem Canal')               AS canal,
        COUNT(DISTINCT fp.order_id)                         AS total_pedidos,
        round(AVG(fp.gross_amount), 2)                      AS receita_bruta_media,
        round(AVG(fp.discount_amount), 2)                   AS desconto_medio,
        round(AVG(fp.net_amount), 2)                        AS receita_liquida_media,
        round(AVG(fp.discount_amount / NULLIF(fp.gross_amount, 0)) * 100, 1) AS pct_desconto_medio
    FROM fact_pedidos fp
    LEFT JOIN dim_canais c ON fp.channel_key = c.channel_key
    WHERE fp.status != 'CANCELADO'
    GROUP BY c.channel_name
    ORDER BY pct_desconto_medio DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Pedidos sem Entrega Associada

# COMMAND ----------

display(spark.sql("""
    SELECT
        COUNT(DISTINCT fp.order_id)                         AS total_pedidos,
        COUNT(DISTINCT fe.order_key)                        AS pedidos_com_entrega,
        COUNT(DISTINCT fp.order_id)
            - COUNT(DISTINCT fe.order_key)                  AS pedidos_sem_entrega,
        round((COUNT(DISTINCT fp.order_id)
            - COUNT(DISTINCT fe.order_key))
            / COUNT(DISTINCT fp.order_id) * 100, 1)        AS pct_sem_entrega
    FROM fact_pedidos fp
    LEFT JOIN fact_entregas fe ON fp.order_key = fe.order_key
    WHERE fp.status NOT IN ('CANCELADO', 'INDEFINIDO')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Custo de Entrega vs Receita do Pedido

# COMMAND ----------

display(spark.sql("""
    SELECT
        coalesce(r.region_name, 'Sem Região')               AS regiao,
        COUNT(DISTINCT fp.order_id)                         AS total_pedidos,
        round(SUM(fp.net_amount), 2)                        AS receita_liquida,
        round(SUM(fe.cost), 2)                              AS custo_total_entregas,
        round(SUM(fe.cost) / NULLIF(SUM(fp.net_amount), 0) * 100, 1) AS pct_custo_sobre_receita
    FROM fact_pedidos fp
    LEFT JOIN fact_entregas fe ON fp.order_key = fe.order_key
    LEFT JOIN dim_regioes r    ON fp.region_key = r.region_key
    GROUP BY r.region_name
    ORDER BY pct_custo_sobre_receita DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Quais ações práticas poderiam ser priorizadas pela liderança?
# MAGIC ### 5.1 Sumário Executivo — Principais Alertas

# COMMAND ----------

display(spark.sql("""
    WITH kpis AS (
        SELECT
            COUNT(DISTINCT order_id)                                                AS total_pedidos,
            round(SUM(net_amount), 2)                                               AS receita_liquida,
            round(AVG(net_amount), 2)                                               AS ticket_medio,
            round(COUNT(CASE WHEN status = 'CANCELADO' THEN 1 END)
                  / COUNT(*) * 100, 1)                                              AS taxa_cancelamento_pct,
            round(SUM(CASE WHEN status = 'CANCELADO' THEN gross_amount ELSE 0 END), 2) AS receita_perdida_cancelamentos
        FROM fact_pedidos
    ),
    atrasos AS (
        SELECT
            round(SUM(CAST(is_late AS INT)) / COUNT(*) * 100, 1) AS taxa_atraso_pct,
            COUNT(DISTINCT CASE WHEN is_late THEN order_key END)  AS pedidos_atrasados
        FROM fact_entregas
    ),
    ocorrencias AS (
        SELECT COUNT(*) AS total_ocorrencias
        FROM fact_ocorrencias
    )
    SELECT
        k.total_pedidos,
        k.receita_liquida,
        k.ticket_medio,
        k.taxa_cancelamento_pct,
        k.receita_perdida_cancelamentos,
        a.taxa_atraso_pct,
        a.pedidos_atrasados,
        o.total_ocorrencias
    FROM kpis k, atrasos a, ocorrencias o
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Região × Canal — Cruzamento de Desempenho

# COMMAND ----------

display(spark.sql("""
    SELECT
        coalesce(r.region_name, 'Sem Região')   AS regiao,
        coalesce(c.channel_name, 'Sem Canal')   AS canal,
        COUNT(DISTINCT fp.order_id)             AS total_pedidos,
        round(SUM(fp.net_amount), 2)            AS receita_liquida,
        round(AVG(fp.net_amount), 2)            AS ticket_medio,
        round(COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END)
              / COUNT(*) * 100, 1)              AS taxa_cancelamento_pct,
        round(SUM(CAST(fe.is_late AS INT))
              / NULLIF(COUNT(fe.order_key), 0) * 100, 1) AS taxa_atraso_pct
    FROM fact_pedidos fp
    LEFT JOIN dim_regioes  r  ON fp.region_key  = r.region_key
    LEFT JOIN dim_canais   c  ON fp.channel_key = c.channel_key
    LEFT JOIN fact_entregas fe ON fp.order_key  = fe.order_key
    GROUP BY r.region_name, c.channel_name
    ORDER BY receita_liquida DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Vendedores com Maior Taxa de Cancelamento

# COMMAND ----------

display(spark.sql("""
    SELECT
        v.seller_name                                                 AS vendedor,
        coalesce(r.region_name, 'Sem Região')                        AS regiao,
        coalesce(c.channel_name, 'Sem Canal')                        AS canal,
        COUNT(DISTINCT fp.order_id)                                  AS total_pedidos,
        COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END)         AS cancelados,
        round(COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END)
              / COUNT(*) * 100, 1)                                   AS taxa_cancelamento_pct,
        round(SUM(fp.net_amount), 2)                                 AS receita_liquida
    FROM fact_pedidos fp
    LEFT JOIN dim_vendedores v ON fp.seller_key  = v.seller_key
    LEFT JOIN dim_regioes   r  ON fp.region_key  = r.region_key
    LEFT JOIN dim_canais    c  ON fp.channel_key = c.channel_key
    GROUP BY v.seller_name, r.region_name, c.channel_name
    HAVING COUNT(DISTINCT fp.order_id) >= 5
    ORDER BY taxa_cancelamento_pct DESC
    LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Evolução Trimestral — Comparativo de Performance

# COMMAND ----------

display(spark.sql("""
    SELECT
        t.year                                                        AS ano,
        t.quarter                                                     AS trimestre,
        COUNT(DISTINCT fp.order_id)                                   AS total_pedidos,
        round(SUM(fp.net_amount), 2)                                  AS receita_liquida,
        round(AVG(fp.net_amount), 2)                                  AS ticket_medio,
        round(COUNT(CASE WHEN fp.status = 'CANCELADO' THEN 1 END)
              / COUNT(*) * 100, 1)                                    AS taxa_cancelamento_pct,
        round(SUM(CAST(fe.is_late AS INT))
              / NULLIF(COUNT(fe.order_key), 0) * 100, 1)             AS taxa_atraso_pct
    FROM fact_pedidos fp
    JOIN dim_tempo t ON fp.order_date_key = t.date_key
    LEFT JOIN fact_entregas fe ON fp.order_key = fe.order_key
    GROUP BY t.year, t.quarter
    ORDER BY t.year, t.quarter
"""))
