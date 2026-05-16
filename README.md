# customer-support-bi-databricks
Pipeline completo de BI com Databricks — Bronze/Silver/Gold, ETL, EDA e Dashboard


# 🎯 Customer Support BI — Databricks Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Edition-red)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion-green)
![Power BI](https://img.shields.io/badge/Power%20BI-Connected-yellow)

Pipeline completo de Business Intelligence construído com **Databricks**, seguindo a **Medallion Architecture** (Bronze → Silver → Gold), com ETL em PySpark, EDA avançado e Dashboard gerencial.

---

## 📋 Sobre o Projeto

Análise completa de **8.469 tickets de suporte ao cliente** de uma empresa de tecnologia, cobrindo o período de **Janeiro/2020 a Dezembro/2021**.

O projeto simula um ambiente enterprise real, desde a ingestão do dado bruto até a visualização gerencial, passando por todas as etapas de um pipeline de dados moderno.

---

## 🏗️ Arquitetura — Medallion Architecture

```
CSV Bruto
    ↓
┌─────────────────────────────────────────────────────────┐
│                    DATA LAKE                            │
│                                                         │
│  🥉 BRONZE          🥈 SILVER          🥇 GOLD          │
│  Dado bruto    →   Dado limpo    →   Star Schema        │
│  inferSchema=F     Tipos corretos    9 tabelas Delta    │
│  tudo string       Nulos tratados    Pronto p/ consumo  │
│                    Duplicatas off                       │
│                    Placeholder off                      │
└─────────────────────────────────────────────────────────┘
    ↓                     ↓                    ↓
Notebook 01          Notebook 02          Notebook 04
                                               ↓
                                        ┌──────────────┐
                                        │   ANÁLISE    │
                                        │  Notebook 03 │
                                        │     EDA      │
                                        │  Notebook 05 │
                                        │  EDA Viz     │
                                        │  Notebook 06 │
                                        │  BI Dashboard│
                                        └──────────────┘
                                               ↓
                                          Power BI
```

---

## 📁 Estrutura do Projeto

```
customer-support-bi-databricks/
│
├── notebooks/
│   ├── 01_bronze_ingestao.py         # Ingestão do CSV → Delta Lake Bronze
│   ├── 02_silver_etl.py              # ETL, limpeza e tipagem → Silver
│   ├── 03_eda.py                     # Análise Exploratória de Dados
│   ├── 04_gold_modelagem.py          # Star Schema → Gold
│   ├── 05_eda_visualizacao.py        # Gráficos EDA avançados
│   └── 06_bi_dashboard.py            # Dashboard BI gerencial
│
├── charts/
│   ├── eda/                          # Gráficos exploratórios
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
│   └── bi/                           # Gráficos BI gerenciais
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
│   └── customer_support_tickets_original.csv   # Dado bruto original
│
└── README.md
```

---

## 🛠️ Stack Tecnológica

| Tecnologia | Versão | Uso |
|------------|--------|-----|
| Databricks | Free Edition | Plataforma principal |
| Apache Spark | 3.x | Processamento distribuído |
| PySpark | 3.x | ETL e transformações |
| Delta Lake | 3.x | Storage ACID com versionamento |
| Unity Catalog | - | Governança e volumes |
| Python | 3.10 | Linguagem principal |
| Pandas | 2.x | Manipulação local |
| Matplotlib | 3.x | Visualizações |
| Seaborn | 0.13 | Visualizações estatísticas |
| Power BI | Desktop | Dashboard final |

---

## 📊 Dataset

| Atributo | Valor |
|----------|-------|
| **Fonte** | Customer Support Tickets |
| **Período** | Jan/2020 — Dez/2021 |
| **Total de registros** | 8.469 tickets |
| **Colunas originais** | 17 |
| **Formato** | CSV |

### Colunas do dataset original

| Coluna | Tipo Original | Tipo Silver | Descrição |
|--------|--------------|-------------|-----------|
| Ticket ID | string | integer | Identificador único |
| Customer Name | string | string | Nome do cliente |
| Customer Email | string | string | Email do cliente |
| Customer Age | string | integer | Idade do cliente |
| Customer Gender | string | string | Gênero do cliente |
| Product Purchased | string | string | Produto adquirido |
| Date of Purchase | string | date | Data da compra |
| Ticket Type | string | string | Tipo do ticket |
| Ticket Subject | string | string | Assunto do ticket |
| Ticket Description | string | string | Descrição do problema |
| Ticket Status | string | string | Status atual |
| Resolution | string | string | Texto da resolução |
| Ticket Priority | string | string | Prioridade |
| Ticket Channel | string | string | Canal de atendimento |
| First Response Time | string | timestamp | Hora do primeiro atendimento |
| Time to Resolution | string | timestamp | Hora da resolução |
| Customer Satisfaction Rating | string | double | Nota de satisfação (1-5) |

---

## 🔄 Pipeline — Etapas Detalhadas

### 🥉 Etapa 1 — Bronze (01_bronze_ingestao.py)

Ingestão do arquivo bruto para o Data Lake sem nenhuma transformação.

**O que faz:**
- Lê o CSV do Unity Catalog Volume (`/Volumes/workspace/default/raw/`)
- Preserva todos os dados como string (`inferSchema=false`)
- Trata quebras de linha no campo `Ticket_Description` (`multiLine=true`)
- Renomeia colunas substituindo espaços por underscore
- Salva em formato **Delta Lake** na camada Bronze

**Path:** `/Volumes/workspace/default/raw/bronze/`

---

### 🥈 Etapa 2 — Silver (02_silver_etl.py)

Limpeza, tipagem e tratamento de qualidade dos dados.

**Transformações aplicadas:**

| Transformação | Detalhe |
|--------------|---------|
| Correção de tipos | 6 colunas convertidas para tipos corretos |
| Tratamento de nulos | `Resolution` preenchida com "Sem resolução" |
| Remoção de duplicatas | Baseada em `Ticket_ID` |
| Limpeza de placeholder | `{product_purchased}` → `[produto]` |
| Coluna de controle | `_loaded_at` com timestamp de processamento |

**Nulos identificados e tratados:**

| Coluna | Nulos | Motivo |
|--------|-------|--------|
| First_Response_Time | 2.819 | Tickets sem primeira resposta |
| Time_to_Resolution | 5.700 | Tickets abertos/pendentes |
| Customer_Satisfaction_Rating | 5.700 | Só preenchido quando resolvido |

**Path:** `/Volumes/workspace/default/raw/silver/`

---

### 📊 Etapa 3 — EDA (03_eda.py)

Análise Exploratória dos dados limpos da camada Silver.

**Análises realizadas:**

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

### 🥇 Etapa 4 — Gold / Star Schema (04_gold_modelagem.py)

Modelagem dimensional seguindo o padrão **Star Schema**.

**Tabelas criadas:**

| Tabela | Linhas | Descrição |
|--------|--------|-----------|
| `f_customer_support_tickets` | 8.469 | Tabela fato central |
| `dim_customer` | 8.320 | Dimensão cliente com Age_Group |
| `dim_product` | 42 | Dimensão produto |
| `dim_type` | 5 | Dimensão tipo de ticket |
| `dim_subject` | 16 | Dimensão assunto |
| `dim_status` | 3 | Dimensão status |
| `dim_priority` | 4 | Dimensão prioridade |
| `dim_channel` | 4 | Dimensão canal |
| `dim_ticket_description` | 8.469 | Dimensão descrição com classificação NLP |
| `dim_calendario` | 730 | Dimensão calendário (730 dias) |

**Métricas calculadas na tabela fato:**

| Métrica | Cálculo |
|---------|---------|
| `Response_Time_Hours` | Hora + minutos do primeiro atendimento |
| `Resolution_Time_Hours` | Diferença em horas entre resposta e resolução |
| `Is_Resolved` | Flag 1/0 — ticket fechado ou não |
| `Purchase_Year` | Ano extraído da data de compra |
| `Purchase_Month` | Mês extraído da data de compra |

**Path:** `/Volumes/workspace/default/raw/gold/`

---

### 📈 Etapa 5 — EDA Visualização (05_eda_visualizacao.py)

Gráficos exploratórios avançados usando Matplotlib e Seaborn.

| Gráfico | Tipo | Insight |
|---------|------|---------|
| Distribuição por Status | Bar chart | 67% dos tickets ainda abertos |
| Distribuição por Prioridade | Bar chart | Distribuição uniforme — sem hierarquia real |
| Distribuição por Tipo | Donut chart | Refund Request lidera com 20.7% |
| Volume por Canal | Horizontal bar | Email domina com 25.3% |
| Satisfação por Canal | Radar chart | Chat único acima da média |
| Satisfação por Prioridade/Tipo | Heatmap | Refund+High = 2.80 — pior combinação |
| Volume Mensal | Line chart | Sem sazonalidade |
| Volume Stacked | Stacked bar | 2020 e 2021 praticamente iguais |
| Top 10 Produtos | Horizontal bar | Canon EOS lidera |
| Distribuição de Idade | Histogram+KDE | Uniforme entre 18-70 anos |
| Satisfação por Tipo | Violin plot | Distribuição bimodal em todos os tipos |
| Taxa de Resolução | Gauge chart | 32.7% — zona crítica |

---

### 📊 Etapa 6 — BI Dashboard (06_bi_dashboard.py)

Dashboard gerencial com foco em tomada de decisão.

| Visual | Pergunta Respondida |
|--------|---------------------|
| KPI Cards + Gauge | Como está a saúde geral do suporte? |
| Evolução da Taxa de Resolução | Estamos melhorando ao longo do tempo? |
| Funil de Resolução | Onde estou falhando por prioridade? |
| Tendência de Satisfação | A satisfação está melhorando? |
| Matriz Estratégica de Produtos | Quais produtos precisam de atenção? |
| Ranking de Assuntos Críticos | Onde focar para melhorar a satisfação? |
| Scorecard Executivo | Performance completa por tipo de ticket |
| Painel de Alertas | Quais ações tomar agora? |

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

**🟡 Paradoxo do cancelamento**
> Cancellation Request tem satisfação 3.03 — acima da média geral. O problema não é o atendimento do cancelamento, mas o que levou o cliente a cancelar.

**🟢 Canal Chat subutilizado**
> Chat tem o menor volume (24.5%) mas a maior satisfação (3.08). Migrar 10% dos tickets de email para chat poderia elevar a satisfação média acima de 3.0.

**🔴 Pior combinação identificada**
> Refund Request + prioridade High = satisfação 2.80 — a célula mais crítica do heatmap.

### Recomendações

1. **Implementar SLA por prioridade** — Critical: 4h, High: 24h
2. **Força-tarefa Refund Request + High** — combinação mais crítica
3. **Incentivar canal Chat** — maior satisfação, menor volume
4. **Suporte especializado 51+** — maior segmento, abaixo da média
5. **Onboarding proativo** para Canon EOS e GoPro Hero

---

## ⚠️ Decisões Técnicas Documentadas

| Decisão | Justificativa |
|---------|--------------|
| `inferSchema=false` na Bronze | Preservar dado bruto sem risco de conversão |
| Nulos em Time_to_Resolution mantidos | Tickets abertos legitimamente não têm resolução |
| Produtos similares não consolidados | Decisão de negócio — não técnica |
| `Description_Clean` sem limpeza profunda | Preservado para uso futuro em NLP |
| `Is_Resolved` baseado em Status_ID=2 | Status "Closed" = único critério de resolução |

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
# Catalog: workspace
# Schema: default
# Volume: raw

# 3. Faça upload do CSV para o volume
# /Volumes/workspace/default/raw/customer_support_tickets - original.csv

# 4. Execute os notebooks em ordem
# 01_bronze_ingestao.py
# 02_silver_etl.py
# 03_eda.py
# 04_gold_modelagem.py
# 05_eda_visualizacao.py
# 06_bi_dashboard.py
```

---

## 📌 Pendências e Próximos Passos

- [ ] Conexão Power BI → Databricks Gold
- [ ] Dashboard interativo no Power BI
- [ ] Campo `Description_NLP` com limpeza profunda para análise de sentimento
- [ ] Agendamento do pipeline com Databricks Jobs
- [ ] Testes de qualidade de dados automatizados

---

## 👩‍💻 Autora

**Gisele** — [github.com/GiseleCp](https://github.com/GiseleCp)

Projeto desenvolvido como portfólio de **Engenharia e Análise de Dados** com foco em pipeline completo de BI usando Databricks.

---

*Projeto desenvolvido com orientação técnica de Claude (Anthropic)*
