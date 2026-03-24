-- ATIVIDADE 4: PADRONIZAÇÃO DE TEXTO

-- 4a. Verificar variações de país ANTES da limpeza
SELECT
    country AS pais_original,
    UPPER(TRIM(country)) AS pais_padronizado,
    COUNT(*) AS ocorrencias
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;

-- ATIVIDADE 5: TRATAMENTO DE NULOS E DATAS

-- 5a. Preview da limpeza de nulos e datas
SELECT
    id,
    COALESCE(age, 0) AS age_limpa,
    EXTRACT(YEAR FROM CAST(created_at AS TIMESTAMP)) AS ano_cadastro,
    CAST(created_at AS TIMESTAMP) AS data_cadastro_ts
FROM `bigquery-public-data.thelook_ecommerce.users`
LIMIT 10;

-- ATIVIDADE 6: SILVER — VISÃO LIMPA DE USUÁRIOS (CTE MODULAR)

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
        postal_code,
        CAST(created_at AS TIMESTAMP) AS created_at,
        EXTRACT(YEAR FROM CAST(created_at AS TIMESTAMP)) AS safra_cadastro,
        CASE
            WHEN age BETWEEN 18 AND 25 THEN '18–25'
            WHEN age BETWEEN 26 AND 35 THEN '26–35'
            WHEN age BETWEEN 36 AND 45 THEN '36–45'
            WHEN age BETWEEN 46 AND 60 THEN '46–60'
            WHEN age > 60 THEN '60+'
            ELSE 'Não informado'
        END AS faixa_etaria
    FROM `bigquery-public-data.thelook_ecommerce.users`
    WHERE id IS NOT NULL
),
orders_clean AS (
    SELECT
        order_id,
        user_id,
        status,
        CAST(created_at AS TIMESTAMP) AS created_at,
        CAST(shipped_at AS TIMESTAMP) AS shipped_at,
        CAST(delivered_at AS TIMESTAMP) AS delivered_at,
        CAST(returned_at AS TIMESTAMP) AS returned_at,
        num_of_item
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE user_id IS NOT NULL
),
validacao_limpeza AS (
    SELECT
        COUNT(*) AS total_registros,
        COUNTIF(age IS NULL) AS nulos_age_antes,
        COUNTIF(TRIM(country) != UPPER(TRIM(country)))  AS pais_nao_padronizado_antes,
        COUNTIF(age_limpa = 0) AS substituidos_por_zero,
        COUNTIF(faixa_etaria = 'Não informado') AS sem_faixa_etaria
    FROM (
        SELECT
            age,
            COALESCE(age, 0) AS age_limpa,
            country,
            CASE
                WHEN age BETWEEN 18 AND 25 THEN '18–25'
                WHEN age BETWEEN 26 AND 35 THEN '26–35'
                WHEN age BETWEEN 36 AND 45 THEN '36–45'
                WHEN age BETWEEN 46 AND 60 THEN '46–60'
                WHEN age > 60 THEN '60+'
                ELSE 'Não informado'
            END AS faixa_etaria
        FROM `bigquery-public-data.thelook_ecommerce.users`
    )
)
SELECT *
FROM users_clean
ORDER BY created_at;


-- ============================================================
-- [EXTRA] SILVER — VISÃO LIMPA DE ITENS DE PEDIDO
-- ------------------------------------------------------------
-- order_items é a fonte correta para cálculo de receita
-- (conforme validado na Fase 1 - Atividade 1).
-- ============================================================

WITH order_items_clean AS (
    SELECT
        id                                              AS item_id,
        order_id,
        user_id,
        product_id,
        status,

        -- Valor de venda (fonte correta de receita)
        ROUND(sale_price, 2)                            AS sale_price,

        -- Datas tipadas
        CAST(created_at AS TIMESTAMP)                   AS created_at,
        CAST(shipped_at AS TIMESTAMP)                   AS shipped_at,
        CAST(delivered_at AS TIMESTAMP)                 AS delivered_at,
        CAST(returned_at AS TIMESTAMP)                  AS returned_at,

        -- [EXTRA] Flag de devolução — útil para calcular receita líquida
        CASE
            WHEN status = 'Returned' THEN TRUE
            ELSE FALSE
        END                                             AS foi_devolvido,

        -- [EXTRA] Receita líquida (exclui devoluções)
        CASE
            WHEN status = 'Returned' THEN 0
            ELSE ROUND(sale_price, 2)
        END                                             AS receita_liquida

    FROM `bigquery-public-data.thelook_ecommerce.order_items`
    WHERE order_id IS NOT NULL
      AND user_id  IS NOT NULL
)

SELECT *
FROM order_items_clean
ORDER BY created_at;
