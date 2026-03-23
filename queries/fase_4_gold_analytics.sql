-- ============================================================
--  PROJETO: CRM ANALYTICS — THE LOOK
--  FASE 4: INTELIGÊNCIA — ANALYTICS (CAMADA GOLD)
--  Objetivo: Entregar as tabelas finais prontas para o BI.
--             Aqui matamos a charada do Churn, RFM e KPIs.
--
--  Inputs (saídas da Fase 3):
--    → silver_orders_full: base analítica unificada
--
--  Outputs (tabelas Gold):
--    → crm_analytics.dim_customers_gold   (Customer 360 + RFM)
--    → crm_analytics.fct_sales_performance (KPIs + Séries Temporais)
-- ============================================================


-- ============================================================
-- ATIVIDADE 7: dim_customers_gold
-- SEGMENTAÇÃO RFM + STATUS DO CICLO DE VIDA DO CLIENTE
-- ------------------------------------------------------------
-- RFM é o modelo mais usado em CRM para qualificar clientes:
--   R = Recência    → há quantos dias o cliente não compra?
--   F = Frequência  → quantas vezes ele comprou no total?
--   M = Monetário   → quanto ele gastou no total?
--
-- Status do ciclo de vida:
--   Novo       → apenas 1 pedido completo
--   Recorrente → 2+ pedidos, recência ≤ 90 dias
--   Churn      → sem compra há mais de 90 dias
--   Recuperado → estava em Churn, voltou a comprar
--
-- Decisão de negócio documentada:
--   Threshold de 90 dias para Churn é padrão de e-commerce.
--   Ajustar conforme ciclo de compra real do produto.
--   Apenas pedidos com status = 'Complete' entram no RFM —
--   cancelamentos e devoluções não representam receita real.
-- ============================================================

CREATE OR REPLACE TABLE `projeto-the-look-ecommerce.crm_analytics.dim_customers_gold` AS

WITH

-- ETAPA 1: Base limpa de usuários
users_clean AS (
    SELECT
        id                                               AS user_id,
        LOWER(TRIM(email))                               AS email,
        first_name,
        last_name,
        COALESCE(age, 0)                                 AS age,
        gender,
        UPPER(TRIM(country))                             AS country,
        UPPER(TRIM(state))                               AS state,
        city,
        CAST(created_at AS TIMESTAMP)                    AS user_created_at,
        EXTRACT(YEAR FROM CAST(created_at AS TIMESTAMP)) AS safra_cadastro,
        CASE
            WHEN age BETWEEN 18 AND 25 THEN '18–25'
            WHEN age BETWEEN 26 AND 35 THEN '26–35'
            WHEN age BETWEEN 36 AND 45 THEN '36–45'
            WHEN age BETWEEN 46 AND 60 THEN '46–60'
            WHEN age > 60             THEN '60+'
            ELSE 'Não informado'
        END                                              AS faixa_etaria
    FROM `bigquery-public-data.thelook_ecommerce.users`
    WHERE id IS NOT NULL
),

-- ETAPA 2: Apenas pedidos completos para o RFM
--           Cancelados e devolvidos distorcem as métricas
orders_complete AS (
    SELECT
        user_id,
        id                                               AS order_id,
        CAST(created_at AS TIMESTAMP)                    AS order_created_at
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE status = 'Complete'
      AND user_id IS NOT NULL
),

-- ETAPA 3: Receita real por usuário (somente itens não devolvidos)
receita_por_usuario AS (
    SELECT
        user_id,
        ROUND(SUM(sale_price), 2)                        AS receita_total,
        ROUND(AVG(sale_price), 2)                        AS ticket_medio_usuario
    FROM `bigquery-public-data.thelook_ecommerce.order_items`
    WHERE status NOT IN ('Returned', 'Cancelled')
      AND user_id IS NOT NULL
    GROUP BY 1
),

-- ETAPA 4: Cálculo RFM bruto por usuário
rfm_raw AS (
    SELECT
        user_id,

        -- RECÊNCIA: dias desde a última compra completa
        DATE_DIFF(
            CURRENT_DATE(),
            DATE(MAX(order_created_at)),
            DAY
        )                                                AS dias_inativo,

        -- DATA da última compra (útil para dashboards)
        MAX(order_created_at)                            AS ultima_compra_at,

        -- FREQUÊNCIA: total de pedidos completos
        COUNT(order_id)                                  AS total_pedidos,

        -- DATA da primeira compra (para análise de coorte)
        MIN(order_created_at)                            AS primeira_compra_at

    FROM orders_complete
    GROUP BY 1
),

-- ETAPA 5: Status do ciclo de vida + scores RFM
rfm_scored AS (
    SELECT
        r.user_id,
        r.dias_inativo,
        r.total_pedidos,
        r.ultima_compra_at,
        r.primeira_compra_at,
        rev.receita_total,
        rev.ticket_medio_usuario,

        -- -----------------------------------------------
        -- STATUS DO CICLO DE VIDA
        -- Lógica em cascata: a ordem importa
        -- -----------------------------------------------
        CASE
            -- Recuperado: estava inativo > 90 dias, voltou
            -- (tinha pedido antigo + pedido recente)
            WHEN r.dias_inativo <= 90
             AND r.total_pedidos > 1
             AND DATE_DIFF(
                    CURRENT_DATE(),
                    DATE(r.primeira_compra_at),
                    DAY
                 ) > 90                               THEN 'Recuperado'

            -- Churn: sem compra há mais de 90 dias
            WHEN r.dias_inativo > 90                  THEN 'Churn'

            -- Novo: apenas 1 pedido completo
            WHEN r.total_pedidos = 1                  THEN 'Novo'

            -- Recorrente: 2+ pedidos, ativo nos últimos 90 dias
            ELSE                                           'Recorrente'
        END                                              AS status_cliente,

        -- -----------------------------------------------
        -- SCORE DE RECÊNCIA (1 a 4)
        -- Quanto menor o dias_inativo, maior o score
        -- -----------------------------------------------
        CASE
            WHEN r.dias_inativo <= 30  THEN 4  -- Comprou no último mês
            WHEN r.dias_inativo <= 60  THEN 3  -- 1–2 meses atrás
            WHEN r.dias_inativo <= 90  THEN 2  -- 2–3 meses atrás
            ELSE                            1  -- Mais de 3 meses
        END                                              AS score_recencia,

        -- -----------------------------------------------
        -- SCORE DE FREQUÊNCIA (1 a 4)
        -- Baseado em quartis do número de pedidos
        -- -----------------------------------------------
        CASE
            WHEN r.total_pedidos >= 10 THEN 4  -- Heavy buyer
            WHEN r.total_pedidos >= 5  THEN 3  -- Comprador frequente
            WHEN r.total_pedidos >= 2  THEN 2  -- Comprador ocasional
            ELSE                            1  -- Compra única
        END                                              AS score_frequencia,

        -- -----------------------------------------------
        -- SCORE MONETÁRIO (1 a 4)
        -- Baseado em quartis de receita
        -- -----------------------------------------------
        CASE
            WHEN rev.receita_total >= 500 THEN 4  -- Alto valor
            WHEN rev.receita_total >= 200 THEN 3  -- Valor médio-alto
            WHEN rev.receita_total >= 50  THEN 2  -- Valor médio-baixo
            ELSE                               1  -- Baixo valor
        END                                              AS score_monetario

    FROM rfm_raw          AS r
    LEFT JOIN receita_por_usuario AS rev ON r.user_id = rev.user_id
),

-- ETAPA 6: Segmento RFM consolidado
--           Combinação dos scores → rótulo de segmento
rfm_segmented AS (
    SELECT
        *,

        -- Score RFM total (soma simples: mín 3, máx 12)
        score_recencia + score_frequencia + score_monetario AS score_rfm_total,

        -- Segmento de negócio baseado nos scores
        -- Prioridade: Recência tem peso maior (decisão de CRM)
        CASE
            WHEN score_recencia = 4
             AND score_frequencia >= 3
             AND score_monetario >= 3  THEN 'Champions'
                                            -- Compram frequente, gastam muito, recentes

            WHEN score_recencia >= 3
             AND score_frequencia >= 3  THEN 'Loyal Customers'
                                            -- Fiéis, mas talvez não os maiores gastadores

            WHEN score_recencia = 4
             AND score_frequencia <= 2  THEN 'Recent Customers'
                                            -- Compraram recentemente, mas pouco frequentes

            WHEN score_recencia >= 3
             AND score_monetario >= 3  THEN 'Big Spenders'
                                            -- Gastam muito, mas frequência irregular

            WHEN score_recencia = 2
             AND score_frequencia >= 2  THEN 'At Risk'
                                            -- Eram bons clientes, começando a sumir

            WHEN score_recencia = 1
             AND score_frequencia >= 3  THEN 'Cant Lose Them'
                                            -- Foram ótimos clientes, sumiram → prioridade de reativação

            WHEN score_recencia = 1
             AND score_frequencia = 1  THEN 'Lost'
                                            -- Baixo valor, sem retorno → menor prioridade

            ELSE                           'Needs Attention'
                                            -- Perfil misto → investigar
        END                                              AS segmento_rfm

    FROM rfm_scored
)

-- RESULTADO FINAL: dim_customers_gold
-- Visão 360° de cada cliente
SELECT
    -- Identificação
    u.user_id,
    u.email,
    u.first_name,
    u.last_name,

    -- Dados demográficos limpos
    u.age,
    u.faixa_etaria,
    u.gender,
    u.country,
    u.state,
    u.city,

    -- Dados de cadastro
    u.user_created_at,
    u.safra_cadastro,

    -- Métricas RFM
    r.dias_inativo,
    r.total_pedidos,
    r.receita_total,
    r.ticket_medio_usuario,
    r.primeira_compra_at,
    r.ultima_compra_at,

    -- Scores RFM individuais
    r.score_recencia,
    r.score_frequencia,
    r.score_monetario,
    r.score_rfm_total,

    -- Classificações de negócio
    r.status_cliente,
    r.segmento_rfm,

    -- [EXTRA] Dias como cliente (LTV window)
    DATE_DIFF(
        CURRENT_DATE(),
        DATE(u.user_created_at),
        DAY
    )                                                    AS dias_como_cliente,

    -- [EXTRA] LTV diário estimado (receita / dias como cliente)
    -- Proxy simples de LTV — não substitui modelos preditivos
    ROUND(
        SAFE_DIVIDE(
            r.receita_total,
            DATE_DIFF(CURRENT_DATE(), DATE(u.user_created_at), DAY)
        ), 4
    )                                                    AS ltv_diario_estimado

FROM users_clean       AS u
LEFT JOIN rfm_segmented AS r ON u.user_id = r.user_id
ORDER BY r.score_rfm_total DESC NULLS LAST;


-- ============================================================
-- ATIVIDADE 8: fct_sales_performance
-- PERFORMANCE LOGÍSTICA + TICKET MÉDIO POR PAÍS
-- ------------------------------------------------------------
-- Responde: onde estamos com pior SLA e qual é o ticket médio
--           por país? Esses são os dois KPIs operacionais
--           mais críticos para o time de Operações e Marketing.
-- ============================================================

CREATE OR REPLACE TABLE `projeto-the-look-ecommerce.crm_analytics.fct_sales_performance` AS

WITH

orders_joined AS (
    SELECT
        o.id                                             AS order_id,
        o.user_id,
        CAST(o.created_at AS TIMESTAMP)                  AS order_created_at,
        CAST(o.shipped_at AS TIMESTAMP)                  AS shipped_at,
        CAST(o.delivered_at AS TIMESTAMP)                AS delivered_at,
        o.status                                         AS order_status,
        UPPER(TRIM(u.country))                           AS country,
        oi.sale_price,
        CASE
            WHEN oi.status = 'Returned' THEN 0
            ELSE oi.sale_price
        END                                              AS receita_liquida,
        DATE_DIFF(
            DATE(CAST(o.shipped_at AS TIMESTAMP)),
            DATE(CAST(o.created_at AS TIMESTAMP)),
            DAY
        )                                                AS dias_para_envio,
        DATE_DIFF(
            DATE(CAST(o.delivered_at AS TIMESTAMP)),
            DATE(CAST(o.created_at AS TIMESTAMP)),
            DAY
        )                                                AS dias_para_entrega
    FROM `bigquery-public-data.thelook_ecommerce.orders`         AS o
    LEFT JOIN `bigquery-public-data.thelook_ecommerce.users`     AS u
        ON o.user_id = u.id
    LEFT JOIN `bigquery-public-data.thelook_ecommerce.order_items` AS oi
        ON o.id = oi.order_id
    WHERE o.user_id IS NOT NULL
      AND o.shipped_at IS NOT NULL
)

SELECT
    country,

    -- Volume
    COUNT(DISTINCT order_id)                             AS total_pedidos,
    COUNT(*)                                             AS total_itens,

    -- Receita
    ROUND(SUM(sale_price), 2)                            AS receita_bruta,
    ROUND(SUM(receita_liquida), 2)                       AS receita_liquida,
    ROUND(SUM(sale_price) - SUM(receita_liquida), 2)     AS perda_devolucoes,

    -- Ticket médio (por item — granularidade correta)
    ROUND(AVG(sale_price), 2)                            AS ticket_medio,

    -- SLA logístico
    ROUND(AVG(dias_para_envio), 1)                       AS avg_dias_envio,
    ROUND(AVG(dias_para_entrega), 1)                     AS avg_dias_entrega,
    ROUND(
        COUNTIF(dias_para_envio > 3) * 100.0 / COUNT(*), 1
    )                                                    AS pct_envio_atrasado

FROM orders_joined
WHERE country IS NOT NULL
GROUP BY 1
ORDER BY receita_bruta DESC;


-- ============================================================
-- ATIVIDADE 9: SÉRIES TEMPORAIS — YoY e MoM
-- ------------------------------------------------------------
-- YoY (Year over Year): crescimento vs mesmo mês do ano anterior
-- MoM (Month over Month): crescimento vs mês anterior
--
-- Técnica: LAG() com OVER (ORDER BY mes)
--   LAG(valor, 12) → pega o valor de 12 meses atrás (YoY)
--   LAG(valor, 1)  → pega o valor do mês anterior (MoM)
--
-- Apenas pedidos 'Complete' entram na série de receita.
-- ============================================================

WITH

receita_mensal AS (
    SELECT
        DATE_TRUNC(DATE(CAST(oi.created_at AS TIMESTAMP)), MONTH) AS mes,
        ROUND(SUM(oi.sale_price), 2)                               AS receita_bruta,
        ROUND(SUM(
            CASE WHEN oi.status NOT IN ('Returned','Cancelled')
                 THEN oi.sale_price ELSE 0 END
        ), 2)                                                      AS receita_liquida,
        COUNT(DISTINCT o.id)                                       AS pedidos,
        COUNT(DISTINCT o.user_id)                                  AS clientes_ativos,

        -- [EXTRA] Novos clientes no mês
        -- (primeiro pedido do usuário caindo neste mês)
        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC(DATE(CAST(o.created_at AS TIMESTAMP)), MONTH)
               = DATE_TRUNC(DATE(MIN(CAST(o.created_at AS TIMESTAMP)) OVER (PARTITION BY o.user_id)), MONTH)
            THEN o.user_id
        END)                                                       AS novos_clientes

    FROM `bigquery-public-data.thelook_ecommerce.order_items`    AS oi
    LEFT JOIN `bigquery-public-data.thelook_ecommerce.orders`    AS o
        ON oi.order_id = o.id
    WHERE o.status = 'Complete'
    GROUP BY 1
)

SELECT
    mes,
    receita_bruta,
    receita_liquida,
    pedidos,
    clientes_ativos,

    -- MoM: comparação com mês anterior
    LAG(receita_bruta, 1) OVER (ORDER BY mes)            AS receita_mes_anterior,
    ROUND(
        SAFE_DIVIDE(
            receita_bruta - LAG(receita_bruta, 1) OVER (ORDER BY mes),
            LAG(receita_bruta, 1) OVER (ORDER BY mes)
        ) * 100, 1
    )                                                    AS variacao_mom_pct,

    -- YoY: comparação com mesmo mês do ano anterior
    LAG(receita_bruta, 12) OVER (ORDER BY mes)           AS receita_ano_anterior,
    ROUND(
        SAFE_DIVIDE(
            receita_bruta - LAG(receita_bruta, 12) OVER (ORDER BY mes),
            LAG(receita_bruta, 12) OVER (ORDER BY mes)
        ) * 100, 1
    )                                                    AS variacao_yoy_pct,

    -- [EXTRA] Média móvel de 3 meses (suaviza sazonalidade)
    ROUND(AVG(receita_bruta) OVER (
        ORDER BY mes
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                                AS media_movel_3m

FROM receita_mensal
ORDER BY mes;


-- ============================================================
-- [EXTRA] ANÁLISE DE CHURN — QUANTIFICAÇÃO DA PERDA
-- ------------------------------------------------------------
-- Responde a pergunta do brief:
--   "Onde estamos perdendo dinheiro?"
--
-- Segmenta a base por status e mostra o impacto financeiro
-- de cada grupo — crítico para o relatório diagnóstico.
-- ============================================================

WITH

users_clean AS (
    SELECT id AS user_id, UPPER(TRIM(country)) AS country
    FROM `bigquery-public-data.thelook_ecommerce.users`
    WHERE id IS NOT NULL
),

rfm_base AS (
    SELECT
        user_id,
        DATE_DIFF(CURRENT_DATE(), DATE(MAX(CAST(created_at AS TIMESTAMP))), DAY) AS dias_inativo,
        COUNT(id) AS total_pedidos
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE status = 'Complete' AND user_id IS NOT NULL
    GROUP BY 1
),

receita_base AS (
    SELECT user_id, ROUND(SUM(sale_price), 2) AS receita_total
    FROM `bigquery-public-data.thelook_ecommerce.order_items`
    WHERE status NOT IN ('Returned','Cancelled') AND user_id IS NOT NULL
    GROUP BY 1
),

clientes_com_status AS (
    SELECT
        r.user_id,
        r.dias_inativo,
        r.total_pedidos,
        COALESCE(rev.receita_total, 0) AS receita_total,
        CASE
            WHEN r.dias_inativo > 90 THEN 'Churn'
            WHEN r.total_pedidos = 1 THEN 'Novo'
            ELSE 'Recorrente'
        END AS status_cliente
    FROM rfm_base AS r
    LEFT JOIN receita_base AS rev ON r.user_id = rev.user_id
)

SELECT
    status_cliente,
    COUNT(*)                                             AS total_clientes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)  AS pct_da_base,
    ROUND(SUM(receita_total), 2)                         AS receita_gerada,
    ROUND(AVG(receita_total), 2)                         AS receita_media_por_cliente,
    ROUND(AVG(dias_inativo), 0)                          AS avg_dias_inativo,
    ROUND(AVG(total_pedidos), 1)                         AS avg_pedidos

FROM clientes_com_status
GROUP BY 1
ORDER BY receita_gerada DESC;
