# 🎯 Customer Support BI Platform

Enterprise-grade Data Engineering & Analytics Platform built with Databricks, PySpark, Delta Lake, FastAPI and Power BI.

---

## 🚀 Overview

This project simulates a real enterprise-grade Customer Support Analytics Platform, covering the complete modern data lifecycle:

- Raw data ingestion
- Distributed ETL processing
- Data quality validation
- Dimensional modeling
- Business Intelligence dashboards
- REST API serving layer
- Pipeline orchestration
- Delta Lake versioning & recovery
- NLP enrichment

The platform was designed following modern Data Engineering and Analytics Engineering best practices using the Medallion Architecture pattern.

---

# 🏛️ Enterprise Architecture

```text
                            ┌──────────────────────┐
                            │   Raw CSV Dataset    │
                            └──────────┬───────────┘
                                       │
                                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                         DATABRICKS                              │
│                                                                  │
│   🥉 BRONZE              🥈 SILVER              🥇 GOLD          │
│                                                                  │
│   Raw ingestion      Clean & validated      Star Schema         │
│   All string types   Data Quality Checks    Business-ready      │
│   Delta Lake         ETL transformations    Analytics layer     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
                                       │
             ┌─────────────────────────┼─────────────────────────┐
             │                         │                         │
             ▼                         ▼                         ▼
   ┌────────────────┐      ┌────────────────────┐     ┌────────────────┐
   │ Databricks SQL │      │    FastAPI REST    │     │  Power BI /    │
   │  Analytics     │      │        API         │     │ Genie Dashboard│
   └────────────────┘      └────────────────────┘     └────────────────┘
                                       │
                                       ▼
                          ┌────────────────────────┐
                          │ JSON API Consumption   │
                          └────────────────────────┘
```

---

# 🏗️ Medallion Architecture

```text
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
```

---

# 🚀 Enterprise Features

- ✅ Medallion Architecture
- ✅ Delta Lake ACID Transactions
- ✅ Time Travel & Restore
- ✅ Data Quality Framework
- ✅ Star Schema Modeling
- ✅ Unity Catalog Governance
- ✅ Pipeline Orchestration
- ✅ FastAPI REST API
- ✅ NLP Enrichment
- ✅ Databricks SQL Analytics
- ✅ Power BI Dashboard
- ✅ Databricks Genie Dashboard
- ✅ Distributed Processing with Spark
- ✅ Layered Architecture API
- ✅ Repository Pattern
- ✅ Service Layer Pattern

---

# 📁 Repository Structure

```text
customer-support-bi-databricks/
│
├── notebooks/
│   ├── 00_data_profiling.ipynb
│   ├── 01_bronze_ingestao.ipynb
│   ├── 02_silver_etl.ipynb
│   ├── 03_eda.ipynb
│   ├── 04_gold_modelagem.ipynb
│   ├── 05_eda_visualizacao.ipynb
│   ├── 06_bi_dashboard.ipynb
│   ├── 07_sql_queries.ipynb
│   ├── 08_time_travel.ipynb
│   ├── 09_register_tables.ipynb
│   ├── 10_gold_quality_checks.ipynb
│   └── 11_description_nlp.ipynb
│
├── customer-support-api/
│   ├── app/
│   │   ├── api/
│   │   ├── services/
│   │   ├── data/
│   │   ├── models/
│   │   ├── core/
│   │   └── main.py
│   │
│   ├── requirements.txt
│   └── Dockerfile
│
├── assets/
├── images/
├── data/
└── README.md
```

---

# 🛠️ Tech Stack

| Technology | Purpose |
|---|---|
| Apache Spark / PySpark | Distributed ETL |
| Databricks | Data Platform |
| Delta Lake | ACID Storage |
| Unity Catalog | Governance |
| FastAPI | REST API |
| Python 3.10+ | Main Language |
| Databricks SQL | Analytics |
| Power BI | BI Dashboard |
| Pandas | Local manipulation |
| Matplotlib / Seaborn | Data Visualization |
| Docker | Containerization |
| GitHub Actions | CI/CD |

---

# 🌐 REST API — FastAPI

The project includes a production-oriented REST API built with FastAPI following enterprise software engineering patterns.

## 🏛️ API Architecture

```text
Client Request
      ↓
FastAPI Routes
      ↓
Service Layer
      ↓
Repository Layer
      ↓
Spark / Delta Lake
```

## 📂 API Structure

```text
customer-support-api/
│
├── app/
│   ├── api/
│   │   └── routes.py
│   │
│   ├── services/
│   │   └── support_service.py
│   │
│   ├── data/
│   │   └── spark_repository.py
│   │
│   ├── models/
│   │   └── schemas.py
│   │
│   ├── core/
│   │   ├── config.py
│   │   └── cache.py
│   │
│   └── main.py
│
├── requirements.txt
└── Dockerfile
```

## ✅ API Features

- Layered architecture
- Separation of concerns
- Repository Pattern
- Service Layer Pattern
- JSON responses
- Spark integration
- Swagger documentation
- Production-ready structure
- Docker-ready

## 📡 API Endpoints

| Endpoint | Description |
|---|---|
| GET / | API Status |
| GET /kpis | Executive KPIs |
| GET /satisfacao/canal | Satisfaction by channel |
| GET /produtos/top10 | Worst products ranking |
| GET /tickets | Ticket search with filters |

## 📸 Suggested Screenshots

Add the following images to improve repository presentation:

```text
images/api/
├── swagger_ui.png
├── endpoint_kpis.png
├── vscode_structure.png
└── json_response.png
```

---

# 📊 Dataset

| Attribute | Value |
|---|---|
| Source | Customer Support Tickets |
| Period | Jan/2020 — Dec/2021 |
| Total Records | 8,469 tickets |
| Original Columns | 17 |
| Format | CSV |

---

# 🔄 Pipeline Stages

## 00 — Data Profiling

Complete diagnosis of raw data before transformations.

### Key Findings

| Validation | Result |
|---|---|
| Duplicate records | ✅ Zero |
| Numeric anomalies | ✅ Zero |
| Column consistency | ✅ 100% |
| Placeholder issue | 🔴 100% |
| Critical nulls | 🔴 67.3% |
| First response nulls | 🟡 33.3% |

---

## 01 — Bronze Layer

Raw ingestion into Delta Lake preserving source fidelity.

### Main Characteristics

- CSV ingestion from Unity Catalog Volume
- All columns loaded as string
- No transformations
- Delta Lake storage
- Raw data preservation

---

## 02 — Silver Layer

ETL, cleansing and validation layer.

### Transformations

| Transformation | Description |
|---|---|
| Type correction | 6 converted columns |
| Null treatment | Resolution standardized |
| Duplicate removal | Based on Ticket_ID |
| Placeholder cleanup | Product placeholder fixed |
| Control column | _loaded_at timestamp |

---

# ✅ Data Quality Framework

The project implements automated quality validation checks.

## Silver Layer Checks

- ✅ Volume validation
- ✅ Null validation
- ✅ Unique key validation
- ✅ Range validation
- ✅ Placeholder validation
- ✅ Referential consistency
- ✅ Mandatory fields validation
- ✅ Business rule validation
- ✅ Timestamp validation

## Gold Layer Checks

- ✅ Fact table validation
- ✅ Dimension validation
- ✅ Referential integrity
- ✅ Star schema consistency
- ✅ Business metrics validation

---

## 03 — Exploratory Data Analysis

Business and operational analysis over curated data.

### Key Insights

| Analysis | Main Finding |
|---|---|
| Average satisfaction | 2.99 |
| Best channel | Chat (3.08) |
| Worst priority | Critical |
| Worst ticket type | Refund Request |
| Resolution rate | 32.7% |

---

## 04 — Gold Layer — Star Schema

Enterprise dimensional modeling.

### Gold Tables

| Table | Description |
|---|---|
| f_customer_support_tickets | Fact table |
| dim_customer | Customer dimension |
| dim_product | Product dimension |
| dim_type | Ticket type |
| dim_subject | Ticket subject |
| dim_status | Ticket status |
| dim_priority | Priority |
| dim_channel | Channel |
| dim_ticket_description | NLP description |
| dim_calendario | Calendar dimension |

---

# 📈 BI & Visualization

## EDA Visualizations

- Ticket status distribution
- Ticket priority distribution
- Satisfaction heatmaps
- Radar charts
- Violin plots
- Gauge charts
- Monthly trends
- Product rankings

## Executive BI Dashboard

### Business Questions Answered

| Dashboard | Business Question |
|---|---|
| KPI Executive Panel | Overall support health |
| Resolution Funnel | Operational bottlenecks |
| Satisfaction Trend | Service deterioration |
| Strategic Product Matrix | Critical products |
| Alert Panel | Immediate actions |

---

# ⚙️ Pipeline Orchestration

The complete pipeline is orchestrated using Databricks Jobs.

```text
00_data_profiling
↓
01_bronze_ingestao
↓
02_silver_etl
↓
03_eda
↓
04_gold_modelagem
↓
05_eda_visualizacao
↓
06_bi_dashboard
↓
07_sql_queries
```

| Attribute | Value |
|---|---|
| Job Name | pipeline_customer_support_bi |
| Duration | 3 minutes 5 seconds |
| Status | ✅ Success |
| Trigger | Manual / Scheduled |

---

# 🕒 Delta Lake Time Travel

The project demonstrates Delta Lake recovery and versioning.

| Version | Action |
|---|---|
| Version 0 | Initial load |
| Version 1-6 | Reprocessing |
| Version 7 | Simulated failure |
| Version 8 | Restore successful |

### Result

- 8,469 rows restored
- Recovery in under 5 seconds
- No data loss

---

# 🔍 Executive Insights

## Critical KPIs

| KPI | Value | Status |
|---|---|---|
| Resolution Rate | 32.7% | 🔴 |
| Average Satisfaction | 2.99 | 🟡 |
| Open Backlog | 5,700 tickets | 🔴 |
| Total Tickets | 8,469 | — |

---

## Strategic Insights

### 🔴 Priority Paradox

Critical tickets have almost the same resolution rate as Low priority tickets.

### 🔴 Worst Combination

Refund Request + High Priority generated the lowest satisfaction score.

### 🟡 Temporal Deterioration

Customer satisfaction declined from 2020 to 2021.

### 🟢 Chat Opportunity

Chat has the highest satisfaction and lowest operational volume.

---

# ⚠️ Technical Decisions

| Decision | Justification |
|---|---|
| inferSchema=false | Preserve raw fidelity |
| Resolution nulls preserved | Open tickets legitimately unresolved |
| Similar products not merged | Business decision |
| Minimal NLP cleanup | Future NLP enrichment |
| Explicit null handling | Data profiling alignment |

---

# 💼 Skills Demonstrated

- Data Engineering
- Analytics Engineering
- PySpark Development
- Distributed ETL
- Delta Lake
- Data Quality Engineering
- Star Schema Modeling
- FastAPI Development
- REST APIs
- SQL Analytics
- NLP Processing
- Pipeline Orchestration
- BI Development
- Dashboard Design
- Data Governance
- Data Architecture

---

# 🚀 How to Reproduce

## Prerequisites

- Databricks Account
- Python 3.10+
- Power BI Desktop (optional)
- Docker Desktop (optional)

---

## Clone Repository

```bash
git clone https://github.com/GiseleCp/customer-support-bi-databricks
```

---

## Databricks Setup

```text
Catalog: workspace
Schema: default
Volume: raw
```

Upload dataset to:

```text
/Volumes/workspace/default/raw/customer_support_tickets - original.csv
```

---

## Execute Notebooks

Run notebooks sequentially:

```text
00_data_profiling
01_bronze_ingestao
02_silver_etl
03_eda
04_gold_modelagem
05_eda_visualizacao
06_bi_dashboard
07_sql_queries
08_time_travel
09_register_tables
```

---

# 🐳 Dockerizing the API

## Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Build Docker Image

```bash
docker build -t customer-support-api .
```

## Run Container

```bash
docker run -p 8000:8000 customer-support-api
```

---

# ☁️ API Deployment

## Suggested Platforms

- Render
- Railway
- Azure App Service
- AWS ECS
- Google Cloud Run

## Recommended First Deployment

### Render

1. Connect GitHub repository
2. Select Docker deployment
3. Set start command
4. Deploy automatically

---

# 🔄 GitHub Actions — CI/CD

Create:

```text
.github/workflows/api-ci.yml
```

## Example Pipeline

```yaml
name: API CI

on:
  push:
    branches: [ main ]

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v4

    - name: Setup Python
      uses: actions/setup-python@v5
      with:
        python-version: '3.11'

    - name: Install dependencies
      run: pip install -r requirements.txt

    - name: Run tests
      run: pytest
```

---

# 📸 Recommended Screenshots

## Suggested Visual Assets

```text
images/
├── architecture/
│   ├── enterprise_architecture.png
│   ├── star_schema.png
│   └── medallion_architecture.png
│
├── api/
│   ├── swagger_ui.png
│   ├── endpoint_kpis.png
│   └── vscode_structure.png
│
├── pipeline/
│   ├── databricks_jobs.png
│   ├── schedules.png
│   └── pipeline_tasks.png
│
└── dashboard/
    ├── powerbi_dashboard.png
    └── genie_dashboard.png
```

---

# 🔮 Roadmap

- [ ] Dockerized production API
- [ ] Cloud deployment
- [ ] CI/CD with GitHub Actions
- [ ] JWT Authentication
- [ ] Redis Cache
- [ ] Automated Testing
- [ ] Monitoring & Logging
- [ ] Power BI Service deployment
- [ ] ML classification layer
- [ ] Feature Store
- [ ] Real-time ingestion

---

# 👩‍💻 Author

## Gisele

GitHub:

```text
https://github.com/GiseleCp
```

Project developed as an Enterprise Data Engineering & Analytics portfolio project focused on Databricks, Delta Lake, FastAPI and Business Intelligence.

---

# ⭐ Final Result

This project demonstrates:

- Enterprise Data Engineering
- Modern Analytics Engineering
- Distributed ETL Pipelines
- Production-oriented API Development
- Data Quality Engineering
- BI & Executive Analytics
- Data Governance & Architecture
- End-to-end Data Platform Design

