# PROJETO: INTEGRAÇÃO DE DADOS CRM - THE LOOK

## Contexto do problema  
A empresa cresceu. Os dados, não.  
A The Look saiu de uma loja pequena para processar milhares de pedidos globalmente em menos de 2 anos. Mas enquanto o volume crescia, a inteligência sobre os clientes ficou para trás.  
O time de Marketing usava planilhas exportadas para planejar campanhas. O Financeiro calculava receita em outro sistema. Os números não batiam. E ninguém sabia responder perguntas básicas:  

- Quem são nossos melhores clientes?  
- Quantos clientes estamos perdendo por mês?  
- Em quais países estamos tendo mais devoluções?  
- Qual é o nosso ticket médio real?  

## Objetivo

Construir um Data Warehouse em camadas no BigQuery que limpe os dados brutos e entregue tabelas prontas para o time de BI.



## Stack

- **Plataforma:** Google BigQuery (Standard SQL)  
- **Dataset:** bigquery-public-data.thelook_ecommerce  
- **Padrão:** Medallion Architecture (Bronze → Silver → Gold)  
- **Governança:** CTEs modulares, comentários explicando decisões de negócio  

## O que foi construído

Uma Single Source of Truth (SSOT) — uma fonte única de verdade para toda a área de CRM e Vendas, estruturada em camadas no Google BigQuery. O projeto seguiu a Arquitetura Medalha, separando claramente cada etapa do processo:  

Bronze → dados brutos, como vieram da fonte  
Silver → dados limpos, padronizados e unidos  
Gold   → métricas prontas para o time de BI tomar decisões

Ao final, duas tabelas Gold foram entregues:  

`dim_customers_gold`: visão 360° de cada cliente, com segmentação RFM em 3 dimensões independentes (Recência, Frequência e Valor Monetário), receita bruta e líquida, e métricas de comportamento de compra. Todos os clientes estão presentes, incluindo quem nunca comprou — informação crítica para campanhas de ativação.  
`fct_sales_performance`: tabela de fatos de vendas com granularidade de 1 linha por pedido, contendo ticket médio, métricas de SLA logístico (dias entre pedido e envio) e variações de crescimento YoY e MoM.

## Como foi feito

O projeto utilizou 3 tabelas brutas do dataset público thelook_ecommerce:

| Tabela      | Granularidade               | O que contém                                  |
|-------------|------------------------------|-----------------------------------------------|
| `orders`    | 1 linha por pedido           | Pedidos realizados, com status e datas logísticas |
| `order_items` | 1 linha por item no pedido | Itens individuais dentro de cada pedido, com valor de venda                            |
| `users`     | 1 linha por usuário          | Dados cadastrais de cada cliente                          |

## O trabalho foi dividido em 4 fases:

**Fase 1 - Auditoria**

Antes de construir qualquer métrica, os dados foram investigados para entender o que existia e o que estava quebrado. Essa etapa respondeu perguntas como: a chave primária de usuários é realmente única? Quantos pedidos estão sem usuário associado? Quais países aparecem com grafias diferentes? Principais achados:

- Países com grafias inconsistentes: "USA", "United States", "us" — todos representando o mesmo lugar  
- Idades nulas em parte da base de clientes  
- Pedidos com status Cancelled e Returned presentes na base — tratados como colunas na camada Gold, não filtrados  

**Fase 2 — Limpeza (Camada Silver)**

Com os problemas mapeados, cada coluna crítica foi tratada:

- País: UPPER(TRIM(country)) — elimina variações de maiúsculas e espaços extras
- Email: LOWER(TRIM(email)) — padroniza como chave de identificação única
- Idade: COALESCE(age, 0) — substitui nulos por zero, mantendo o problema visível em vez de mascarar com média
- Datas: CAST(created_at AS TIMESTAMP) — garante o tipo correto para cálculos de tempo

Além da limpeza, colunas derivadas foram criadas para enriquecer a análise:

- `safra_cadastro` — ano em que o cliente se cadastrou, usado para análise de coorte
- `faixa_etaria` — agrupamento de idade em faixas (18-25, 26-35, 36-45, 46-60, 60+)
- `receita_liquida` — valor de venda calculado via CASE, excluindo devoluções e cancelamentos
- `perda_devolucoes` — valor devolvido isolado como coluna, para análise de impacto financeiro

**Fase 3 — Modelagem (Camada Silver)**

As três tabelas foram unidas em uma base analítica única. A escolha foi por LEFT JOIN — e não INNER JOIN — para preservar todos os registros e torná-los auditáveis, em vez de descartá-los silenciosamente. A granularidade final é 1 linha por item de pedido, o que permite calcular receita corretamente sem duplicação. Métricas de SLA logístico calculadas nessa etapa:

- `dias_para_envio` — intervalo entre a data do pedido e a data de envio
- `dias_para_entrega` — intervalo entre a data do pedido e a data de entrega
- `envio_atrasado` — flag booleana para envios com mais de 3 dias (threshold padrão de mercado)

**Fase 4 — Analytics (Camada Gold)**

Tabelas geradas: `dim_customers_gold`, `fct_sales_performance`  

Com a base limpa e unida, as métricas de negócio foram calculadas.  

A segmentação RFM classifica cada cliente em 3 dimensões independentes, prontas para o BI usar separadamente ou em conjunto:  

| Dimensão             | Coluna      | Categorias  |
|----------------------|------------|---------------|
| Recencia      | `segmento_recencia`    | Ativo, Em risco, Churn, Sem compra |
| Frequência           | `segmento_frequencia`    | Muito recorrente, Recorrente, Ocasional, Compra única, Sem compra |
| Valor   | `segmento_Valor`    | Premium, Alto valor, Médio valor, Baixo valor, Sem compra |

A `fct_sales_performance` entrega 1 linha por pedido com ticket médio, SLA logístico e variações de crescimento calculadas com funções de janela (LAG):

- `variacao_mom_pct` — crescimento vs mês anterior
- `variacao_yoy_pct` — crescimento vs mesmo mês do ano anterior
- `media_movel_3m` — média móvel de 3 meses para suavizar sazonalidade


