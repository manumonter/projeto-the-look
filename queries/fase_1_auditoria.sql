-- ============================================================
--  PROJETO: CRM ANALYTICS — THE LOOK
--  FASE 1: AUDITORIA (DISCOVERY)
--  Objetivo: Entender a estrutura dos dados antes de construir
--             qualquer métrica. Não construímos sobre areia.
--  Dataset:  bigquery-public-data.thelook_ecommerce
-- ============================================================


-- ============================================================
-- ATIVIDADE 1: VALIDAÇÃO DE GRANULARIDADE
-- ------------------------------------------------------------
-- Pergunta: qual tabela representa "1 linha por pedido"
-- e qual representa "1 linha por item dentro do pedido"?
--
-- Por que isso importa?
--   Se somarmos o valor de vendas usando a tabela errada,
--   vamos contar a receita múltiplas vezes (um pedido com
--   3 itens apareceria 3x). Isso é o erro clássico de
--   contagem duplicada de receita.
-- ============================================================

-- 1a. Verificar granularidade da tabela `orders`
--     Esperado: cada order_id aparece exatamente 1 vez
--     → esta é a tabela de fatos de pedidos (1 linha por pedido)
SELECT
    order_id,
    COUNT(*) AS frequencia
FROM `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- 1b. Verificar granularidade da tabela `order_items`
--     Esperado: um mesmo order_id aparece múltiplas vezes
--     → esta é a tabela de itens (1 linha por produto no pedido)
SELECT
    order_id,
    COUNT(*) AS frequencia
FROM `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- 1c. [EXTRA] Quantificar o impacto da duplicação
--     Mostra a diferença real entre somar receita na tabela
--     certa vs. errada — útil para o relatório diagnóstico.
SELECT
    'order_items (correto)'           AS fonte,
    COUNT(DISTINCT order_id)          AS pedidos_unicos,
    ROUND(SUM(sale_price), 2)         AS receita_total
FROM `bigquery-public-data.thelook_ecommerce.order_items`

UNION ALL

SELECT
    'orders (incorreto para receita)' AS fonte,
    COUNT(DISTINCT order_id)          AS pedidos_unicos,
    -- orders não tem sale_price — demonstra que ela não serve para valor
    NULL                              AS receita_total
FROM `bigquery-public-data.thelook_ecommerce.orders`;


-- ============================================================
-- ATIVIDADE 2: VALIDAÇÃO DE CHAVE PRIMÁRIA (PK) — tabela users
-- ------------------------------------------------------------
-- Pergunta: a coluna `id` é realmente única por usuário?
--
-- Por que isso importa?
--   Se houver IDs duplicados na tabela de usuários, nosso
--   "Customer 360" (dim_customers_gold) vai contar o mesmo
--   cliente mais de uma vez — inflando a base de clientes.
-- ============================================================

-- 2a. Teste rápido de unicidade
--     Se total_linhas = ids_unicos → chave está íntegra ✅
--     Se total_linhas > ids_unicos → há duplicatas ❌
SELECT
    COUNT(*)          AS total_linhas,
    COUNT(DISTINCT id) AS ids_unicos,

    -- Calcula quantas linhas são duplicatas
    COUNT(*) - COUNT(DISTINCT id) AS linhas_duplicadas,

    -- Percentual de integridade da PK
    ROUND(
        COUNT(DISTINCT id) / COUNT(*) * 100, 2
    ) AS pct_integridade
FROM `bigquery-public-data.thelook_ecommerce.users`;

-- 2b. [EXTRA] Se houver duplicatas: identificar quais são
--     (substitua a condição por HAVING COUNT(*) > 1 para ver)
SELECT
    id,
    COUNT(*) AS ocorrencias
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY 2 DESC
LIMIT 20;


-- ============================================================
-- ATIVIDADE 3: [EXTRA] DATA QUALITY REPORT COMPLETO
-- ------------------------------------------------------------
-- Entregável adicional: um diagnóstico de qualidade de dados
-- por coluna, respondendo às perguntas:
--   - Quantos nulos existem em cada campo crítico?
--   - Há emails duplicados (mesmo cliente com 2 cadastros)?
--   - Qual a distribuição de países? (detecta inconsistências)
--   - O range de idades faz sentido?
--
-- Este relatório vai direto no README e no relatório final.
-- ============================================================

-- 3a. Nulos por coluna crítica na tabela users
SELECT
    -- Total de registros
    COUNT(*)                                          AS total_usuarios,

    -- Campos de identificação
    COUNTIF(id IS NULL)                               AS nulos_id,
    COUNTIF(email IS NULL)                            AS nulos_email,

    -- Campos demográficos
    COUNTIF(age IS NULL)                              AS nulos_age,
    COUNTIF(country IS NULL)                          AS nulos_country,
    COUNTIF(gender IS NULL)                           AS nulos_gender,

    -- Campos de data (críticos para cálculo de coorte)
    COUNTIF(created_at IS NULL)                       AS nulos_created_at,

    -- Percentual geral de completude
    ROUND(
        (1 - COUNTIF(email IS NULL) / COUNT(*)) * 100, 1
    )                                                 AS pct_completude_email

FROM `bigquery-public-data.thelook_ecommerce.users`;

-- 3b. Distribuição de países (detecta variações de escrita)
--     Ex: "USA", "United States", "US" → devem ser unificados
SELECT
    country,
    COUNT(*) AS total_usuarios,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_do_total
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;

-- 3c. Range de idades — detecta idades impossíveis ou suspeitas
SELECT
    MIN(age)   AS idade_minima,
    MAX(age)   AS idade_maxima,
    AVG(age)   AS idade_media,
    COUNTIF(age < 0 OR age > 120) AS idades_invalidas
FROM `bigquery-public-data.thelook_ecommerce.users`;

-- 3d. Emails duplicados — detecta multi-cadastro do mesmo cliente
SELECT
    LOWER(TRIM(email)) AS email_normalizado,
    COUNT(*)           AS cadastros
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY 2 DESC
LIMIT 20;

-- 3e. Nulos e status na tabela orders (crítico para RFM)
SELECT
    COUNT(*)                          AS total_pedidos,
    COUNTIF(user_id IS NULL)          AS pedidos_sem_usuario,
    COUNTIF(created_at IS NULL)       AS pedidos_sem_data,
    COUNTIF(shipped_at IS NULL)       AS pedidos_sem_envio,
    COUNTIF(delivered_at IS NULL)     AS pedidos_sem_entrega,

    -- Distribuição por status
    COUNTIF(status = 'Complete')      AS completos,
    COUNTIF(status = 'Cancelled')     AS cancelados,
    COUNTIF(status = 'Returned')      AS devolvidos,
    COUNTIF(status = 'Processing')    AS em_processamento,
    COUNTIF(status = 'Shipped')       AS enviados
FROM `bigquery-public-data.thelook_ecommerce.orders`;
