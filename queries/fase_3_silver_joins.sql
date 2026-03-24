-- ATIVIDADE 7: O GRANDE JOIN — PEDIDOS + USUÁRIOS + ITENS

-- 7a. Verificar orphans antes do join (boa prática de auditoria)
SELECT
    COUNT(*) AS pedidos_sem_usuario
FROM `bigquery-public-data.thelook_ecommerce.orders` o
LEFT JOIN `bigquery-public-data.thelook_ecommerce.users` u
    ON o.user_id = u.id
WHERE u.id IS NULL;

-- 7b. O Grande Join — base analítica unificada
WITH
users_clean AS (
    SELECT
        id AS user_id,
        LOWER(TRIM(email)) AS email,
        first_name,
        last_name,
        COALESCE(age, 0) AS age,
        gender,
        UPPER(TRIM(country)) AS country,
        UPPER(TRIM(state)) AS state,
        city,
        CAST(created_at AS TIMESTAMP) AS user_created_at,
        EXTRACT(YEAR FROM CAST(created_at AS TIMESTAMP)) AS safra_cadastro,
        CASE
            WHEN age BETWEEN 18 AND 25 THEN '18–25'
            WHEN age BETWEEN 26 AND 35 THEN '26–35'
            WHEN age BETWEEN 36 AND 45 THEN '36–45'
            WHEN age BETWEEN 46 AND 60 THEN '46–60'
            WHEN age > 60             THEN '60+'
            ELSE 'Não informado'
        END AS faixa_etaria
    FROM `bigquery-public-data.thelook_ecommerce.users`
    WHERE id IS NOT NULL
),

orders_clean AS (
    SELECT
        order_id,
        user_id,
        status AS order_status,
        num_of_item,
        CAST(created_at AS TIMESTAMP) AS order_created_at,
        CAST(shipped_at AS TIMESTAMP) AS shipped_at,
        CAST(delivered_at AS TIMESTAMP) AS delivered_at,
        CAST(returned_at AS TIMESTAMP) AS returned_at,
        DATE_DIFF(
            DATE(CAST(shipped_at AS TIMESTAMP)),
            DATE(CAST(created_at AS TIMESTAMP)),
            DAY
        ) AS dias_para_envio,
        DATE_DIFF(
            DATE(CAST(delivered_at AS TIMESTAMP)),
            DATE(CAST(created_at AS TIMESTAMP)),
            DAY
        ) AS dias_para_entrega,
        CASE
            WHEN DATE_DIFF(
                DATE(CAST(shipped_at AS TIMESTAMP)),
                DATE(CAST(created_at AS TIMESTAMP)),
                DAY
            ) > 3 THEN TRUE
            ELSE FALSE
        END  AS envio_atrasado
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE user_id IS NOT NULL
),
order_items_clean AS (
    SELECT
        id AS item_id,
        order_id,
        user_id,
        product_id,
        status AS item_status,
        ROUND(sale_price, 2) AS sale_price,
        CAST(created_at AS TIMESTAMP) AS item_created_at,
        CASE
            WHEN status = 'Returned' THEN 0
            ELSE ROUND(sale_price, 2)
        END AS receita_liquida,
        CASE
            WHEN status = 'Returned' THEN TRUE
            ELSE FALSE
        END AS foi_devolvido
    FROM `bigquery-public-data.thelook_ecommerce.order_items`
    WHERE order_id IS NOT NULL
      AND user_id  IS NOT NULL
),
silver_orders_full AS (
    SELECT
        oi.item_id,
        oi.order_id,
        oi.user_id,
        oi.product_id,
        u.email,
        u.country,
        u.state,
        u.city,
        u.age,
        u.gender,
        u.faixa_etaria,
        u.safra_cadastro,
        u.user_created_at,
        o.order_status,
        o.num_of_item,
        o.order_created_at,
        o.shipped_at,
        o.delivered_at,
        o.returned_at,
        o.dias_para_envio,
        o.dias_para_entrega,
        o.envio_atrasado,
        oi.item_status,
        oi.sale_price,
        oi.receita_liquida,
        oi.foi_devolvido

    FROM order_items_clean   AS oi
    LEFT JOIN orders_clean   AS o  ON oi.order_id = o.order_id
    LEFT JOIN users_clean    AS u  ON oi.user_id  = u.user_id
)
SELECT *
FROM silver_orders_full
ORDER BY order_created_at DESC;

-- 7c: VALIDAÇÃO DO JOIN

WITH

users_clean AS (
    SELECT id AS user_id, LOWER(TRIM(email)) AS email,
           UPPER(TRIM(country)) AS country, UPPER(TRIM(state)) AS state,
           COALESCE(age, 0) AS age, gender,
           CAST(created_at AS TIMESTAMP) AS user_created_at,
           EXTRACT(YEAR FROM CAST(created_at AS TIMESTAMP)) AS safra_cadastro,
           CASE
               WHEN age BETWEEN 18 AND 25 THEN '18–25'
               WHEN age BETWEEN 26 AND 35 THEN '26–35'
               WHEN age BETWEEN 36 AND 45 THEN '36–45'
               WHEN age BETWEEN 46 AND 60 THEN '46–60'
               WHEN age > 60             THEN '60+'
               ELSE 'Não informado'
           END AS faixa_etaria
    FROM `bigquery-public-data.thelook_ecommerce.users`
    WHERE id IS NOT NULL
),

orders_clean AS (
    SELECT id AS order_id, user_id, status AS order_status, num_of_item,
           CAST(created_at AS TIMESTAMP) AS order_created_at,
           CAST(shipped_at AS TIMESTAMP) AS shipped_at,
           CAST(delivered_at AS TIMESTAMP) AS delivered_at,
           CAST(returned_at AS TIMESTAMP) AS returned_at,
           DATE_DIFF(DATE(CAST(shipped_at AS TIMESTAMP)), DATE(CAST(created_at AS TIMESTAMP)), DAY) AS dias_para_envio,
           DATE_DIFF(DATE(CAST(delivered_at AS TIMESTAMP)), DATE(CAST(created_at AS TIMESTAMP)), DAY) AS dias_para_entrega,
           CASE WHEN DATE_DIFF(DATE(CAST(shipped_at AS TIMESTAMP)), DATE(CAST(created_at AS TIMESTAMP)), DAY) > 3
                THEN TRUE ELSE FALSE END AS envio_atrasado
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE user_id IS NOT NULL
),

order_items_clean AS (
    SELECT id AS item_id, order_id, user_id, product_id,
           status AS item_status, ROUND(sale_price, 2) AS sale_price,
           CAST(created_at AS TIMESTAMP) AS item_created_at,
           CASE WHEN status = 'Returned' THEN 0 ELSE ROUND(sale_price, 2) END AS receita_liquida,
           CASE WHEN status = 'Returned' THEN TRUE ELSE FALSE END AS foi_devolvido
    FROM `bigquery-public-data.thelook_ecommerce.order_items`
    WHERE order_id IS NOT NULL AND user_id IS NOT NULL
),

silver_orders_full AS (
    SELECT oi.*, o.order_status, o.order_created_at, o.shipped_at, o.delivered_at,
           o.returned_at, o.dias_para_envio, o.dias_para_entrega, o.envio_atrasado,
           u.country, u.age, u.gender, u.faixa_etaria, u.safra_cadastro
    FROM order_items_clean AS oi
    LEFT JOIN orders_clean AS o ON oi.order_id = o.order_id
    LEFT JOIN users_clean  AS u ON oi.user_id  = u.user_id
)

SELECT
    COUNT(*)  AS total_itens,
    COUNT(DISTINCT order_id) AS pedidos_unicos,
    COUNT(DISTINCT user_id)  AS usuarios_unicos,
    ROUND(SUM(sale_price), 2) AS receita_bruta_total,
    ROUND(SUM(receita_liquida), 2) AS receita_liquida_total,
    ROUND(SUM(sale_price) - SUM(receita_liquida), 2) AS perda_por_devolucoes,
    COUNTIF(country IS NULL) AS itens_sem_pais, 
    COUNTIF(envio_atrasado = TRUE) AS envios_atrasados,
    ROUND(AVG(dias_para_envio), 1) AS avg_dias_para_envio,
    ROUND(AVG(dias_para_entrega), 1) AS avg_dias_para_entrega
FROM silver_orders_full;

-- ATIVIDADE 8 [EXTRA] ANÁLISE DE SLA LOGÍSTICO POR PAÍS

WITH

orders_clean AS (
    SELECT order_id, user_id,
           CAST(created_at AS TIMESTAMP) AS order_created_at,
           CAST(shipped_at AS TIMESTAMP) AS shipped_at,
           CAST(delivered_at AS TIMESTAMP) AS delivered_at,
           DATE_DIFF(DATE(CAST(shipped_at AS TIMESTAMP)), DATE(CAST(created_at AS TIMESTAMP)), DAY) AS dias_para_envio,
           DATE_DIFF(DATE(CAST(delivered_at AS TIMESTAMP)), DATE(CAST(created_at AS TIMESTAMP)), DAY) AS dias_para_entrega,
           CASE WHEN DATE_DIFF(DATE(CAST(shipped_at AS TIMESTAMP)), DATE(CAST(created_at AS TIMESTAMP)), DAY) > 3
                THEN TRUE ELSE FALSE END AS envio_atrasado
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE user_id IS NOT NULL
      AND shipped_at IS NOT NULL
),

users_clean AS (
    SELECT id AS user_id, UPPER(TRIM(country)) AS country
    FROM `bigquery-public-data.thelook_ecommerce.users`
    WHERE id IS NOT NULL
),

order_items_clean AS (
    SELECT order_id, ROUND(sale_price, 2) AS sale_price,
           CASE WHEN status = 'Returned' THEN 0 ELSE ROUND(sale_price, 2) END AS receita_liquida
    FROM `bigquery-public-data.thelook_ecommerce.order_items`
    WHERE order_id IS NOT NULL
)

SELECT
    u.country,
    COUNT(DISTINCT o.order_id) AS total_pedidos,
    ROUND(AVG(o.dias_para_envio), 1) AS avg_dias_envio,
    ROUND(AVG(o.dias_para_entrega), 1) AS avg_dias_entrega,
    COUNTIF(o.envio_atrasado = TRUE) AS envios_atrasados,
    ROUND(
        COUNTIF(o.envio_atrasado = TRUE) * 100.0 / COUNT(*), 1
    )  AS pct_atraso,
    ROUND(AVG(oi.sale_price), 2) AS ticket_medio,
    ROUND(SUM(oi.receita_liquida), 2) AS receita_liquida_total
FROM orders_clean AS o
LEFT JOIN users_clean AS u  ON o.user_id   = u.user_id
LEFT JOIN order_items_clean AS oi ON o.order_id  = oi.order_id
WHERE u.country IS NOT NULL
GROUP BY 1
ORDER BY total_pedidos DESC
LIMIT 20;
