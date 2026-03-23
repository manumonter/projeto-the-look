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
| `users.age`    | _preencher_        | COALESCE(age, 0) |
| `users.country` | _preencher_ | UPPER(TRIM(country))   |
| `users.email`     | _preencher_  | LOWER(TRIM(email))  |
| `orders.shipped_at` | _preencher_ | Usado para SLA logístico  |
| `orders.returned_at`     | _preencher_  | Flag de devolução |
