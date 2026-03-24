-- ATIVIDADE 1: VALIDAÇÃO DE GRANULARIDADE

-- 1a. Verificar granularidade da tabela `orders`
SELECT
    order_id,
    COUNT(*) AS frequencia
FROM `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- 1b. Verificar granularidade da tabela `order_items`
SELECT
    order_id,
    COUNT(*) AS frequencia
FROM `bigquery-public-data.thelook_ecommerce.order_items`
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;

-- 1c. [EXTRA] Quantificar o impacto da duplicação
SELECT
    'order_items (correto)' AS fonte,
    COUNT(DISTINCT order_id) AS pedidos_unicos,
    ROUND(SUM(sale_price), 2) AS receita_total
FROM `bigquery-public-data.thelook_ecommerce.order_items`

UNION ALL

SELECT
    'orders (incorreto para receita)' AS fonte,
    COUNT(DISTINCT order_id) AS pedidos_unicos,
    NULL AS receita_total
FROM `bigquery-public-data.thelook_ecommerce.orders`;

-- ATIVIDADE 2: VALIDAÇÃO DE CHAVE PRIMÁRIA (PK) — tabela users

-- 2a. Teste rápido de unicidade
SELECT
    COUNT(*) AS total_linhas,
    COUNT(DISTINCT id) AS ids_unicos,
    COUNT(*) - COUNT(DISTINCT id) AS linhas_duplicadas,
    ROUND(
        COUNT(DISTINCT id) / COUNT(*) * 100, 2
    ) AS pct_integridade
FROM `bigquery-public-data.thelook_ecommerce.users`;

-- 2b. [EXTRA] Se houver duplicatas: identificar quais são
SELECT
    id,
    COUNT(*) AS ocorrencias
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY 2 DESC
LIMIT 20;

-- ATIVIDADE 3: [EXTRA] DATA QUALITY REPORT COMPLETO

-- 3a. Nulos por coluna crítica na tabela users
SELECT
    COUNT(*) AS total_usuarios,
    COUNTIF(id IS NULL) AS nulos_id,
    COUNTIF(email IS NULL) AS nulos_email,
    COUNTIF(age IS NULL) AS nulos_age,
    COUNTIF(country IS NULL) AS nulos_country,
    COUNTIF(gender IS NULL) AS nulos_gender,
    COUNTIF(created_at IS NULL) AS nulos_created_at,
    ROUND(
        (1 - COUNTIF(email IS NULL) / COUNT(*)) * 100, 1
    ) AS pct_completude_email
FROM `bigquery-public-data.thelook_ecommerce.users`;

-- 3b. Distribuição de países (detecta variações de escrita)

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
    MIN(age) AS idade_minima,
    MAX(age) AS idade_maxima,
    AVG(age) AS idade_media,
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
    COUNT(*) AS total_pedidos,
    COUNTIF(user_id IS NULL) AS pedidos_sem_usuario,
    COUNTIF(created_at IS NULL) AS pedidos_sem_data,
    COUNTIF(shipped_at IS NULL) AS pedidos_sem_envio,
    COUNTIF(delivered_at IS NULL) AS pedidos_sem_entrega,
    COUNTIF(status = 'Complete') AS completos,
    COUNTIF(status = 'Cancelled') AS cancelados,
    COUNTIF(status = 'Returned') AS devolvidos,
    COUNTIF(status = 'Processing') AS em_processamento,
    COUNTIF(status = 'Shipped') AS enviados
FROM `bigquery-public-data.thelook_ecommerce.orders`;
