# PROJETO: INTEGRAÇÃO DE DADOS CRM - THE LOOK

## Contexto do problema  
A "The Look" cresceu exponencialmente nos últimos 2 anos, mas sua infraestrutura de dados não acompanhou o crescimento. O time de Marketing usava planilhas exportadas para calcular campanhas, e o Financeiro usava outro sistema para calcular receita. Os números não batiam.
Os três problemas centrais identificados:

| Problema               | Impacto                                                                 |
|------------------------|-------------------------------------------------------------------------|
| Cegueira de cliente    | Sem diferenciação entre cliente de alto valor e cliente inativo        |
| Dados sujos            | Países com grafias inconsistentes, idades nulas, emails duplicados     |
| Métricas inexistentes  | Sem clareza sobre Churn, LTV ou ticket médio real                      |

## Objetivo

Construir um Data Warehouse em camadas no BigQuery que limpe os dados brutos e entregue tabelas prontas para o time de BI.

Bronze (dados brutos) → Silver (dados limpos) → Gold (métricas prontas)

## Stack

- **Plataforma:** Google BigQuery (Standard SQL)  
- **Dataset:** bigquery-public-data.thelook_ecommerce  
- **Padrão:** Medallion Architecture (Bronze → Silver → Gold)  
- **Governança:** CTEs modulares, comentários explicando decisões de negócio  

## Fase 1 — Auditoria (Discovery)  

**Objetivo:** Entender a estrutura dos dados antes de construir qualquer métrica.

**Principais descobertas**  

**1. Granularidade das tabelas**

| Tabela      | Granularidade                | Uso correto                                   |
|-------------|------------------------------|-----------------------------------------------|
| `orders`    | 1 linha por pedido           | Contagem de pedidos, status, datas logísticas |
| `order_items` | 1 linha por item no pedido | Cálculo de receita                            |
| `users`     | 1 linha por usuário          | Base do Customer 360                          |

**2. Integridade da chave primária — users.id**

| Métrica              | Resultado                      |
|----------------------|--------------------------------|
| Total de linhas      | `100000`    |
| IDs únicos           | `100000`    |
| Linhas duplicadas    | `0`    |
| Integridade (%)      | `100`    |

**3. Qualidade por coluna**

| Coluna      | Nulos encontrados            | Ação na Fase 2 |
|-------------|------------------------------|----------------|
| `users.age`    | _0_        | COALESCE(age, 0) |
| `users.country` | _0_ | UPPER(TRIM(country))   |
| `users.email`     | _0_  | LOWER(TRIM(email))  |
| `orders.shipped_at` | _37538_ | Usado para SLA logístico  |
| `orders.returned_at`     | _12564_  | Flag de devolução |

**4. Status de pedidos**

| Status     | Quantidade      |
|------------|------------------|
| Complete   | `31128`    |
| Processing | `24838`    |
| Shipped    | `37538`    |
| Cancelled  | `18660`    |
| Returned   | `12564`    |

## Fase 2 — Limpeza ETL (Camada Silver)  

**Objetivo:** Transformar dados brutos em dados confiáveis e padronizados.

**1. Decisões de limpeza documentadas**

| Campo        | Problema  | Solução    | Justificativa |
|--------------|-------------|-----------|---------------|
| `country`    | Grafias inconsistentes ("USA", "us", "United States") | `UPPER(TRIM(country))`      | Padrão para geotargeting |
| `email`      | Maiúsculas e espaços extras   | `LOWER(TRIM(email))`        | Chave de identificação única |
| `age`        | Valores nulos | `COALESCE(age, 0)`          | Zero sinaliza ausência sem mascarar o problema  |
| `created_at` | Tipo inconsistente  | `CAST AS TIMESTAMP`         | Garante fuso correto para cálculo de datas |

**2. Colunas derivadas criadas**

|Coluna      | Lógica               | Para que serve                                  |
|-------------|------------------------------|-----------------------------------------------|
| `safra_cadastro`    |EXTRACT(YEAR FROM created_at)           | Análise de coorte (clientes de 2022 vs 2023) |
| `faixa_etaria` | CASE WHEN age BETWEEN... | Segmentação demográfica                            |
| `foi_devolvido`     | CASE WHEN status = 'Returned'          | Filtro de receita líquida                         |
| `receita_liquida`     | sale_price onde não devolvido          |Receita real, excluindo devoluções |

## Fase 3 — Modelagem / Joins (Camada Silver)

**Objetivo:** Unir as tabelas limpas da Fase 2 e preparar a base analítica unificada com métricas de eficiência logística.

**1. Decisão de modelagem: granularidade do join**

| Join     | Tipo     | Motivo  |
|------------|---------------- | ---------|
| order_items → orders  | LEFT JOIN | Garante que itens sem pedido pai sejam auditáveis |
| order_items → users | LEFT JOIN | Preserva orphans para investigação |
| Granularidade final   | 1 linha por item | Correto para cálculo de receita sem duplicação |

*Por que LEFT JOIN e não INNER JOIN?*  
Pedidos sem usuário correspondente seriam silenciosamente descartados com INNER JOIN. Com LEFT JOIN, eles aparecem com country IS NULL — detectáveis na validação.

**2. Colunas de SLA logístico criadas**

| Coluna    | Lógica      | Interpretação |
|------------|---------------- | ---------|
| dias_para_envio | DATE_DIFF(shipped_at, created_at, DAY) | Eficiência do fulfillment |
| dias_para_entrega | DATE_DIFF (delivered_at, created_at, DAY) | Experiência do cliente |
| envio_atrasado | dias_para_envio > 3 | Threshold de mercado (ajustável) |

**3. Checklist de validação do join**  

Após executar o join, verificar:  

| Métrica     | Resultado esperado      |
|------------|------------------|
| pedidos_unicos  | `31128`    |
| Processing | `24838`    |
| Shipped    | `37538`    |
| Cancelled  | `18660`    |
| Returned   | `12564`    |
