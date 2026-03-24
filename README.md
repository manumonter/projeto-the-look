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

## Fase 1: Auditoria (Discovery)  

**Objetivo:** Entender a estrutura dos dados antes de construir qualquer métrica.

**Principais descobertas**  

**1. Granularidade das tabelas**

| Tabela      | Granularidade                | Uso correto                                   |
|-------------|------------------------------|-----------------------------------------------|
| `orders`    | 1 linha por pedido           | Contagem de pedidos, status, datas logísticas |
| `order_items` | 1 linha por item no pedido | Cálculo de receita                            |
| `users`     | 1 linha por usuário          | Base do Customer 360                          |

**2. Integridade da chave primária - users.id**

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

## Fase 2: Limpeza ETL (Camada Silver)  

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

## Fase 3: Modelagem / Joins (Camada Silver)

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
| total_itens  | `181097`    |
| pedidos_unicos | `124728`    |
| receita_bruta_total   | `10780245.99`    |
| perda_por_devolucoes  | `1085492.9`    |
| itens_sem_pais  | `0`    |
| avg_dias_para_envio  | `1.5`    |

## Fase 4: Analytics (Camada Gold)

**Objetivo:** Entregar as tabelas finais prontas para o time de BI, respondendo as perguntas de negócio do brief.

**Tabela 1 — dim_customers_gold**  

**1. Lógica de status do ciclo de vida**  

| Status     | Critério     |
|------------|------------------|
| Novo   | Apenas 1 pedido completo    |
| Recorrente | 2+ pedidos, recência ≤ 90 dias    |
| Churn    | Sem compra há mais de 90 dias    |
| Recuperado  | Estava em Churn (> 90 dias inativo), voltou a comprar  |

*Threshold de 90 dias é padrão de e-commerce. Ajustar conforme o ciclo de compra real do produto.*  

**2. Scores RFM (escala 1–4)**

| Dimensão    | Score 4  | Score 3  | Score 2 |  Score 1 |
|------------|-----------|----------|---------|----------|
| Recência   | ≤ 30 dias | ≤ 60 dias| ≤ 90 dias |> 90 dias|
| Frequência |≥ 10 pedidos| ≥ 5 pedidos| ≥ 2 pedidos| 1 pedido
| Monetário | ≥ $500 |≥ $200 |≥ $50 |< $50

**3. Segmentos de negócio gerados**

| Segmento  | Critério  | Ação de CRM  |
|------------|-----------|----------|
| Champions | R=4, F≥3, M≥3 | Fidelizar, pedir reviews |
| Loyal Customers | R≥3, F≥3 | Upsell, programa de pontos |
| Recent Customers | R=4, F≤2 | Incentivar 2ª compra |
| Big Spenders | R≥3, M≥3 | Produtos premium |
| At Risk | R=2, F≥2 | Campanha de retenção urgente |
| Cant Lose Them | R=1, F≥3 |Reativação prioritária|
| Lost | R=1, F=1 | Baixa prioridade ou descontinuar|
| Needs Attention | Perfil misto | Investigar|

**4. Colunas extras entregues**

| Coluna    | Lógica      | Interpretação |
|------------|---------------- | ---------|
| dias_como_cliente | DATE_DIFF(hoje, user_created_at, DAY) | Janela do LTV |
| ltv_diario_estimado | receita_total / dias_como_cliente | Proxy de LTV sem modelo preditivo |
| primeira_compra_at | MIN(order_created_at) | Análise de coorte |

**Tabela 2 — fct_sales_performance**  
KPIs operacionais por país: receita, ticket médio e SLA logístico.  

| Campo     | Critério     |
|------------|------------------|
| receita_bruta   | Soma de sale_price por país    |
| receita_liquida | Exclui itens devolvidos e cancelados    |
| perda_devolucoes    | Receita bruta − líquida (impacto financeiro das devoluções)   |
| ticket_medio  | Média de sale_price por item  |
| avg_dias_envio  | SLA de fulfillment  |
| pct_envio_atrasado  | % de envios com mais de 3 dias |

**Séries temporais — YoY e MoM**

| Coluna    | Técnica      | Descrição |
|------------|---------------- | ---------|
| variacao_mom_pct | LAG(receita, 1) | Crescimento vs mês anteriore |
| variacao_yoy_pct | LAG(receita, 12) | Crescimento vs mesmo mês do ano anterior |
| media_movel_3m | AVG() OVER (ROWS 2 PRECEDING) | Suaviza sazonalidade |
