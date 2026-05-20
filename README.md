# 🎯 Customer Support BI — Databricks Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-red)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion-green)
![Power BI](https://img.shields.io/badge/Power%20BI-In%20Progress-yellow)
![Status](https://img.shields.io/badge/Status-Concluído-brightgreen)

Pipeline completo de Business Intelligence construído com **Databricks**, seguindo a **Medallion Architecture** (Bronze → Silver → Gold), com ETL em PySpark, EDA avançado, Dashboard gerencial, SQL analítico, orquestração de pipeline e Time Travel com Delta Lake.

---

## 📋 Sobre o Projeto

Análise completa de **8.469 tickets de suporte ao cliente** de uma empresa de tecnologia, cobrindo o período de **Janeiro/2020 a Dezembro/2021**.

O projeto simula um ambiente enterprise real, desde a ingestão do dado bruto até a visualização gerencial, passando por todas as etapas de um pipeline de dados moderno com padrões de mercado.

---

## 🏗️ Arquitetura — Medallion Architecture

```
CSV Bruto
    ↓
┌──────────────────────────────────────────────────────────────┐
│                       DATA LAKE                              │
│                                                              │
│  🥉 BRONZE          🥈 SILVER              🥇 GOLD           │
│  Dado bruto    →   Dado limpo        →   Star Schema         │
│  inferSchema=F     Tipos corretos        9 tabelas Delta     │
│  tudo string       Nulos tratados        Pronto p/ consumo   │
│                    Duplicatas off                            │
│                    Placeholder off                           │
│                    9 testes qualidade                        │
└──────────────────────────────────────────────────────────────┘
         ↓                  ↓                    ↓
   Notebook 01        Notebook 02          Notebook 04
                                                ↓
                                    ┌───────────────────┐
                                    │      ANÁLISE      │
                                    │  03 — EDA         │
                                    │  05 — EDA Viz     │
                                    │  06 — BI Dashboard│
                                    │  07 — SQL Queries │
                                    └───────────────────┘
                                                ↓
                                    ┌───────────────────┐
                                    │    PRODUÇÃO       │
                                    │  Job Orquestrado  │
                                    │  Time Travel      │
                                    │  Databricks SQL   │
                                    └───────────────────┘
                                                ↓
                                    ┌───────────────────┐
                                    │  VISUALIZAÇÃO     │
                                    │  Databricks       │
                                    │  Genie Dashboard  │
                                    │  Power BI (WIP)   │
                                    └───────────────────┘
```

---

## 📁 Estrutura do Repositório

```
customer-support-bi-databricks/
│
├── notebooks/
│   ├── 00_data_profiling.ipynb        # Diagnóstico do dado bruto
│   ├── 01_bronze_ingestao.ipynb       # Ingestão CSV → Delta Lake Bronze
│   ├── 02_silver_etl.ipynb            # ETL, limpeza e 9 testes de qualidade
│   ├── 03_eda.ipynb                   # Análise Exploratória de Dados
│   ├── 04_gold_modelagem.ipynb        # Star Schema → Gold
│   ├── 05_eda_visualizacao.ipynb      # 12 gráficos EDA avançados
│   ├── 06_bi_dashboard.ipynb          # 9 gráficos BI gerenciais
│   ├── 07_sql_queries.ipynb           # SQL analítico + View consolidada
│   ├── 08_time_travel.ipynb           # Delta Lake Time Travel e restauração
│   └── 09_register_tables.ipynb       # Registro das tabelas no Unity Catalog
│
├── assets/
│   ├── Jobs & Pipeline.jpg               # Print do Job orquestrado no Databricks
│   ├── Schedules & triggers.jpg          # Agendamento configurado para execução automática
│   └── Tasks.jpg                         # Sequência das 8 tarefas do pipeline orquestrado           
│
├── images/
│   ├── eda/                           # Gráficos exploratórios
│   │   ├── 01_ticket_status.png
│   │   ├── 02_ticket_priority.png
│   │   ├── 03_ticket_type_donut.png
│   │   ├── 04_ticket_channel.png
│   │   ├── 05_radar_satisfacao_canal.png
│   │   ├── 06_heatmap_satisfacao.png
│   │   ├── 07_line_volume_mensal.png
│   │   ├── 08_stacked_volume.png
│   │   ├── 09_top10_produtos.png
│   │   ├── 10_histogram_idade.png
│   │   ├── 11_violin_satisfacao.png
│   │   └── 12_gauge_resolucao.png
│   │
│   └── bi/                            # Gráficos BI gerenciais
│       ├── 01_kpi_executivo.png
│       ├── 02_funil_resolucao.png
│       ├── 03_evolucao_resolucao.png
│       ├── 04_funil_prioridade.png
│       ├── 05_tendencia_satisfacao.png
│       ├── 06_matriz_produto.png
│       ├── 07_ranking_assuntos.png
│       ├── 08_scorecard_executivo.png
│       └── 09_painel_alertas.png
│
├── data/
│   └── customer_support_tickets - original.csv   # Dataset original
│
└── README.md
```

---

## 🛠️ Stack Tecnológica

| Tecnologia | Uso |
|------------|-----|
| **Databricks Free Edition** | Plataforma principal |
| **Apache Spark / PySpark** | Processamento distribuído e ETL |
| **Delta Lake** | Storage ACID com versionamento e Time Travel |
| **Unity Catalog** | Governança, volumes e tabelas gerenciadas |
| **Databricks Jobs** | Orquestração do pipeline |
| **Databricks SQL** | Consultas analíticas e views |
| **Databricks Genie** | Dashboard com IA generativa |
| **Python 3.10** | Linguagem principal |
| **Pandas** | Manipulação local para visualizações |
| **Matplotlib / Seaborn** | Visualizações EDA e BI |
| **Power BI Desktop** | Dashboard gerencial (em progresso) |

---

## 📊 Dataset

| Atributo | Valor |
|----------|-------|
| **Fonte** | Customer Support Tickets |
| **Período** | Jan/2020 — Dez/2021 |
| **Total de registros** | 8.469 tickets |
| **Colunas originais** | 17 |
| **Formato** | CSV |

---

## 🔄 Pipeline Completo — Etapas Detalhadas

### 00 — Data Profiling
Diagnóstico completo do dado bruto **antes** de qualquer transformação.

| Verificação | Resultado |
|-------------|-----------|
| Duplicatas | ✅ Zero |
| Anomalias numéricas | ✅ Zero |
| Consistência entre colunas | ✅ 100% |
| Placeholder `{product_purchased}` | 🔴 100% das descrições |
| Nulos críticos | 🔴 67.3% em 3 colunas |
| First Response nulos | 🟡 33.3% |

---

### 01 — Bronze (Ingestão)
Ingestão do arquivo bruto para o Data Lake sem transformações.

- Lê o CSV do Unity Catalog Volume
- Preserva todos os dados como string (`inferSchema=false`)
- Renomeia colunas substituindo espaços por underscore
- Salva em formato **Delta Lake**

**Path:** `/Volumes/workspace/default/raw/bronze/`

---

### 02 — Silver (ETL + Qualidade)
Limpeza, tipagem e validação dos dados.

**Transformações:**

| Transformação | Detalhe |
|--------------|---------|
| Correção de tipos | 6 colunas convertidas |
| Tratamento de nulos | `Resolution` → "Sem resolução" |
| Remoção de duplicatas | Baseada em `Ticket_ID` |
| Limpeza de placeholder | `{product_purchased}` → `[produto]` |
| Coluna de controle | `_loaded_at` com timestamp |

**9 Testes de Qualidade — todos aprovados ✅:**

```
✅ Teste 1 — Volume: 8.469 registros
✅ Teste 2 — Ticket_ID sem nulos
✅ Teste 3 — Ticket_ID único
✅ Teste 4 — Customer_Age no range (18-100)
✅ Teste 5 — Satisfaction no range (1-5)
✅ Teste 6 — Placeholder removido
✅ Teste 7 — Resolution sem nulos
✅ Teste 8 — Tickets Closed têm Satisfaction Rating
✅ Teste 9 — _loaded_at preenchido
🎉 Silver aprovada para Gold!
```

**Path:** `/Volumes/workspace/default/raw/silver/`

---

### 03 — EDA (Análise Exploratória)
Análise dos dados limpos da Silver.

| Análise | Insight Principal |
|---------|------------------|
| Estatísticas numéricas | Satisfação média 2.99 — abaixo do corte 3.0 |
| Distribuição categórica | Distribuição uniforme em todas as categorias |
| Satisfação por canal | Chat lidera com 3.08 |
| Satisfação por prioridade | Critical tem pior satisfação (2.96) |
| Satisfação por tipo | Refund Request tem pior satisfação (2.93) |
| Volume temporal | Sem sazonalidade — volume estável |
| Top produtos | Canon EOS lidera com 240 tickets |
| Taxa de resolução | Apenas 32.7% dos tickets resolvidos |

---

### 04 — Gold (Star Schema)
Modelagem dimensional com padrão **Star Schema**.

| Tabela | Linhas | Descrição |
|--------|--------|-----------|
| `f_customer_support_tickets` | 8.469 | Tabela fato central |
| `dim_customer` | 8.320 | Cliente com Age_Group |
| `dim_product` | 42 | Produto |
| `dim_type` | 5 | Tipo de ticket |
| `dim_subject` | 16 | Assunto |
| `dim_status` | 3 | Status |
| `dim_priority` | 4 | Prioridade |
| `dim_channel` | 4 | Canal |
| `dim_ticket_description` | 8.469 | Descrição com 16 categorias NLP |
| `dim_calendario` | 730 | Calendário (730 dias) |

**Métricas calculadas na fato:**

| Métrica | Descrição |
|---------|-----------|
| `Response_Time_Hours` | Tempo até primeira resposta |
| `Resolution_Time_Hours` | Tempo total de resolução |
| `Is_Resolved` | Flag 1/0 — ticket fechado |
| `Purchase_Year` | Ano da compra |
| `Purchase_Month` | Mês da compra |

**Path:** `/Volumes/workspace/default/raw/gold/`

---

### 05 — EDA Visualização
12 gráficos exploratórios avançados.

| # | Gráfico | Tipo |
|---|---------|------|
| 01 | Distribuição por Status | Bar chart |
| 02 | Distribuição por Prioridade | Bar chart |
| 03 | Distribuição por Tipo | Donut chart |
| 04 | Volume por Canal | Horizontal bar |
| 05 | Satisfação por Canal | Radar chart |
| 06 | Satisfação por Prioridade/Tipo | Heatmap |
| 07 | Volume Mensal | Line chart |
| 08 | Volume Stacked | Stacked bar |
| 09 | Top 10 Produtos | Horizontal bar gradiente |
| 10 | Distribuição de Idade | Histogram + KDE |
| 11 | Satisfação por Tipo | Violin plot |
| 12 | Taxa de Resolução | Gauge chart |

---

### 06 — BI Dashboard
9 visuais gerenciais focados em tomada de decisão.

| Visual | Pergunta Respondida |
|--------|---------------------|
| KPI Cards + Gauge | Como está a saúde geral do suporte? |
| Evolução da Taxa de Resolução | Estamos melhorando? |
| Funil de Resolução | Onde estou falhando por prioridade? |
| Tendência de Satisfação | A satisfação está melhorando? |
| Matriz Estratégica Produtos | Quais produtos precisam de atenção? |
| Ranking Assuntos Críticos | Onde focar para melhorar? |
| Scorecard Executivo | Performance por tipo de ticket |
| Painel de Alertas | Quais ações tomar agora? |

---

### 07 — SQL Queries Analíticas
Consultas SQL sobre as tabelas Gold com view consolidada.

| Query | O que analisa |
|-------|--------------|
| 1 | Visão geral da fato |
| 2 | Satisfação canal x tipo |
| 3 | Top 10 produtos por insatisfação |
| 4 | Evolução mensal real vs meta |
| 5 | Performance por faixa etária |
| 6 | View consolidada `vw_performance_geral` |
| 7 | Performance prioridade x canal |

---

### 08 — Time Travel Delta Lake
Demonstração de versionamento e recuperação de dados.

```
Versão 0  → Primeira carga Silver
Versão 1-6 → Reprocessamentos e Jobs
Versão 7  → Simulação de erro (coluna removida) 🔴
Versão 8  → RESTORE — recuperação em < 5 segundos ✅
```

**Resultado:** 8.469 linhas recuperadas sem perda de dados.

---

### 09 — Register Tables
Registro das tabelas Gold no Unity Catalog para uso no Databricks SQL e Genie.

---

## ⚙️ Pipeline Orquestrado — Databricks Jobs

Pipeline completo executado automaticamente em sequência:

```
00_data_profiling → 01_bronze → 02_silver → 03_eda → 
04_gold → 05_eda_viz → 06_bi_dashboard → 07_sql_queries
```

| Atributo | Valor |
|----------|-------|
| **Job name** | `pipeline_customer_support_bi` |
| **Duração** | 3 minutos e 5 segundos |
| **Status** | ✅ Succeeded |
| **Trigger** | Manual / Agendável |

---

## 🔍 Principais Insights

### KPIs Críticos

| KPI | Valor | Status |
|-----|-------|--------|
| Taxa de Resolução | 32.7% | 🔴 Meta: 70% |
| Satisfação Média | 2.99 | 🟡 Abaixo do corte 3.0 |
| Backlog Aberto | 5.700 tickets | 🔴 67.3% sem resolução |
| Total de Tickets | 8.469 | — |

### Insights Cruzados

**🔴 Paradoxo da prioridade**
> Tickets Critical têm satisfação 2.96 e taxa de resolução 34.1% — quase igual ao Low (31.2%). O SLA por prioridade provavelmente não existe ou não está sendo cumprido.

**🔴 Pior combinação identificada**
> Refund Request + prioridade High = satisfação 2.80 — a célula mais crítica do heatmap.

**🟡 Paradoxo do cancelamento**
> Cancellation Request tem satisfação 3.03 — acima da média. O problema não é o atendimento, mas o que levou o cliente a cancelar.

**🟡 Deterioração temporal**
> 2020 média 3.04 vs 2021 média 2.94 — satisfação piorando ao longo do tempo sem intervenção.

**🟢 Canal Chat subutilizado**
> Chat tem o menor volume (24.5%) mas a maior satisfação (3.08). Migrar 10% dos tickets de email para chat poderia elevar a satisfação acima de 3.0.

### Recomendações

1. **Implementar SLA por prioridade** — Critical: 4h, High: 24h
2. **Força-tarefa Refund Request + High** — combinação mais crítica (2.80)
3. **Incentivar canal Chat** — maior satisfação, menor volume
4. **Suporte especializado 51+** — maior segmento, abaixo da média
5. **Onboarding proativo** para Canon EOS e GoPro Hero

---

## ⚠️ Decisões Técnicas Documentadas

| Decisão | Justificativa |
|---------|--------------|
| `inferSchema=false` na Bronze | Preservar dado bruto sem risco de conversão |
| Nulos em `Time_to_Resolution` mantidos | Tickets abertos legitimamente não têm resolução |
| Produtos similares não consolidados | Decisão de negócio — não técnica |
| `Description_Clean` sem limpeza profunda | Preservado para uso futuro em NLP |
| `Is_Resolved` baseado em `Status_ID=2` | Status "Closed" = único critério de resolução |
| `Response_Time_Hours` com nulo explícito | Alinhado com Data Profiling — 33.3% sem resposta |

---

## 🚀 Como Reproduzir

### Pré-requisitos
- Conta Databricks (Free Edition ou superior)
- Python 3.10+
- Power BI Desktop (opcional)

### Passo a passo

```bash
# 1. Clone o repositório
git clone https://github.com/GiseleCp/customer-support-bi-databricks

# 2. No Databricks, crie a estrutura de volumes
# Catalog: workspace | Schema: default | Volume: raw

# 3. Faça upload do CSV para o volume
# /Volumes/workspace/default/raw/customer_support_tickets - original.csv

# 4. Execute os notebooks em ordem
# 00_data_profiling
# 01_bronze_ingestao
# 02_silver_etl
# 03_eda
# 04_gold_modelagem
# 05_eda_visualizacao
# 06_bi_dashboard
# 07_sql_queries
# 08_time_travel
# 09_register_tables

# 5. Configure o Job no Databricks
# Jobs & Pipelines → Create Job → pipeline_customer_support_bi
```

---

## 🔧 Em Desenvolvimento

- [ ] Dashboard interativo no Power BI conectado à Gold
- [ ] Campo `Description_NLP` com limpeza profunda para análise de sentimento
- [ ] Testes de qualidade automatizados na camada Gold
- [ ] Alertas automáticos quando satisfação cair abaixo de 3.0
- [ ] API REST para consumo dos dados da Gold

---

## 👩‍💻 Autora

**Gisele** — [github.com/GiseleCp](https://github.com/GiseleCp)

Projeto desenvolvido como portfólio de **Engenharia e Análise de Dados** com foco em pipeline completo de BI usando Databricks — do dado bruto ao dashboard gerencial.

---

*Projeto desenvolvido com orientação técnica de Claude (Anthropic)*
