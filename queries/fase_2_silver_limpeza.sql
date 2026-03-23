-- ============================================================
--  PROJETO: CRM ANALYTICS — THE LOOK
--  FASE 2: LIMPEZA — ETL (CAMADA SILVER)
--  Objetivo: Transformar os dados brutos em dados confiáveis.
--             A camada Silver é a base de todas as métricas.
--
--  Arquitetura Medalha (Medallion Architecture):
--    Bronze → dados brutos, como vieram da fonte
--    Silver → dados limpos e padronizados  ← estamos aqui
--    Gold   → dados agregados e prontos para BI (Fase 4)
-- ============================================================


-- ============================================================
-- ATIVIDADE 4: PADRONIZAÇÃO DE TEXTO
-- ------------------------------------------------------------
-- Problema identificado na Fase 1:
--   - Países com grafias diferentes: "USA", "US", "United States"
--   - Emails com maiúsculas e espaços extras
--
-- Solução aplicada:
--   - UPPER(TRIM()) para país → padrão para geotargeting
--   - LOWER(TRIM()) para email → chave de identificação única
-- ============================================================

-- 4a. Verificar variações de país ANTES da limpeza
--     (use para comparar com o resultado pós-limpeza)
SELECT
    country            AS pais_original,
    UPPER(TRIM(country)) AS pais_padronizado,
    COUNT(*)           AS ocorrencias
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;


-- ============================================================
-- ATIVIDADE 5: TRATAMENTO DE NULOS E DATAS
-- ------------------------------------------------------------
-- Problema identificado na Fase 1:
--   - Idades nulas quebram análises demográficas
--   - Datas precisam de tipo correto para cálculos de coorte
--
-- Decisões de negócio documentadas:
--   - Idade nula → substituída por 0 (sinalizador, não média)
--     Motivo: usar média mascararia o problema; 0 deixa visível
--   - Data de criação → cast para TIMESTAMP garante fuso correto
-- ============================================================

-- 5a. Preview da limpeza de nulos e datas
SELECT
    id,
    COALESCE(age, 0)                    AS age_limpa,

    -- Extrai o ANO de cadastro para análise de coorte (vintage)
    EXTRACT(YEAR FROM CAST(created_at AS TIMESTAMP)) AS ano_cadastro,
    CAST(created_at AS TIMESTAMP)       AS data_cadastro_ts
FROM `bigquery-public-data.thelook_ecommerce.users`
LIMIT 10;


-- ============================================================
-- ATIVIDADE 6: SILVER — VISÃO LIMPA DE USUÁRIOS (CTE MODULAR)
-- ------------------------------------------------------------
-- Este é o entregável principal da Fase 2.
-- Usa CTEs para separar cada etapa de limpeza, tornando
-- o código legível e fácil de manter (um requisito explícito
-- do brief de governança).
--
-- Esta query pode ser salva como VIEW no BigQuery:
--   CREATE OR REPLACE VIEW crm_analytics.silver_users AS ...
-- ============================================================

WITH

-- ETAPA 1: Limpeza de texto e nulos
users_clean AS (
    SELECT
        id                                              AS user_id,

        -- Identificação
        LOWER(TRIM(email))                              AS email,
        first_name,
        last_name,

        -- Dados demográficos limpos
        COALESCE(age, 0)                                AS age,
        gender,

        -- Endereço padronizado
        UPPER(TRIM(country))                            AS country,
        UPPER(TRIM(state))                              AS state,
        city,
        postal_code,

        -- Datas tipadas corretamente
        CAST(created_at AS TIMESTAMP)                   AS created_at,

        -- [EXTRA] Coluna derivada: safra (vintage) do cliente
        -- Usada para análise de coorte: "clientes de 2022 compram mais?"
        EXTRACT(YEAR FROM CAST(created_at AS TIMESTAMP)) AS safra_cadastro,

        -- [EXTRA] Faixa etária para segmentação demográfica
        CASE
            WHEN age BETWEEN 18 AND 25 THEN '18–25'
            WHEN age BETWEEN 26 AND 35 THEN '26–35'
            WHEN age BETWEEN 36 AND 45 THEN '36–45'
            WHEN age BETWEEN 46 AND 60 THEN '46–60'
            WHEN age > 60             THEN '60+'
            ELSE 'Não informado'
        END                                             AS faixa_etaria

    FROM `bigquery-public-data.thelook_ecommerce.users`
    -- Remove registros sem user_id (sem eles não há análise possível)
    WHERE id IS NOT NULL
),

-- ETAPA 2: Limpeza de pedidos (apenas colunas necessárias)
--           Filtra status irrelevantes para análise de receita
orders_clean AS (
    SELECT
        id                                              AS order_id,
        user_id,
        status,
        CAST(created_at AS TIMESTAMP)                   AS created_at,
        CAST(shipped_at AS TIMESTAMP)                   AS shipped_at,
        CAST(delivered_at AS TIMESTAMP)                 AS delivered_at,
        CAST(returned_at AS TIMESTAMP)                  AS returned_at,
        num_of_item
    FROM `bigquery-public-data.thelook_ecommerce.orders`
    WHERE user_id IS NOT NULL
),

-- ETAPA 3: [EXTRA] Tabela antes/depois — valida a limpeza
--           Útil para o relatório diagnóstico e apresentação
validacao_limpeza AS (
    SELECT
        COUNT(*)                                        AS total_registros,

        -- Antes
        COUNTIF(age IS NULL)                            AS nulos_age_antes,
        COUNTIF(TRIM(country) != UPPER(TRIM(country)))  AS pais_nao_padronizado_antes,

        -- Depois (resultado esperado: 0 em ambos)
        COUNTIF(age_limpa = 0)                          AS substituidos_por_zero,

        -- Cobertura de faixas etárias
        COUNTIF(faixa_etaria = 'Não informado')         AS sem_faixa_etaria

    FROM (
        SELECT
            age,
            COALESCE(age, 0)   AS age_limpa,
            country,
            CASE
                WHEN age BETWEEN 18 AND 25 THEN '18–25'
                WHEN age BETWEEN 26 AND 35 THEN '26–35'
                WHEN age BETWEEN 36 AND 45 THEN '36–45'
                WHEN age BETWEEN 46 AND 60 THEN '46–60'
                WHEN age > 60             THEN '60+'
                ELSE 'Não informado'
            END AS faixa_etaria
        FROM `bigquery-public-data.thelook_ecommerce.users`
    )
)

-- RESULTADO FINAL: Silver de usuários
-- Pronto para ser consumido pela Fase 4 (Gold/Analytics)
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
