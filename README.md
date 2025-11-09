# 🚀 E-commerce Data Pipeline

End-to-end data engineering project: **Airflow + DBT + BigQuery** on Google Cloud Platform.

[![Airflow](https://img.shields.io/badge/Airflow-3.1-blue)](https://airflow.apache.org/)
[![DBT](https://img.shields.io/badge/DBT-1.8-orange)](https://www.getdbt.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-yellow)](https://cloud.google.com/bigquery)
[![Python](https://img.shields.io/badge/Python-3.12-green)](https://www.python.org/)

## 📊 Project Overview

A production-ready data pipeline that extracts e-commerce data, loads it to BigQuery, and transforms it using DBT for analytics.

### Architecture

```
Extract (Airflow) → Load (BigQuery) → Transform (DBT) → Analytics
```

**Tech Stack:**
- **Orchestration**: Apache Airflow 3.1 (Docker)
- **Transformation**: DBT Core 1.8 + dbt-bigquery
- **Data Warehouse**: Google BigQuery
- **Infrastructure**: Docker Compose, GCP
- **CI/CD**: GitHub Actions (planned)
- **Language**: Python 3.12

---

## 🏗️ Data Architecture

### Data Flow

1. **Ingestion**: Airflow DAG extracts sample e-commerce data
2. **Raw Layer**: Data loaded to `raw_data.orders` (BigQuery)
3. **Staging**: DBT cleans and types data → `staging.stg_orders`
4. **Warehouse**: DBT creates fact tables → `warehouse.fact_orders`
5. **Analytics**: DBT builds business metrics → `analytics.mart_customer_summary`

### Datasets

| Dataset | Layer | Description |
|---------|-------|-------------|
| `raw_data` | Bronze | Raw ingested data |
| `staging` | Silver | Cleaned & typed data |
| `warehouse` | Gold | Fact/dimension tables |
| `analytics` | Platinum | Business metrics |

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- GCP account with BigQuery enabled
- Service account JSON key

### Setup

1. **Clone repository**
```
git clone https://github.com/YOUR_USERNAME/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

2. **Configure credentials**
```
# Add GCP service account key
cp your-service-account.json credentials/gcp-key.json

# Create .env file
cp .env.example .env
# Edit .env with your GCP project ID
```

3. **Start Airflow**
```
docker-compose up -d
```

4. **Access Airflow UI**
- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

5. **Trigger pipeline**
- Find `ecommerce_etl_pipeline` DAG
- Click "Trigger DAG"

---

## 📂 Project Structure

```
ecommerce-data-pipeline/
├── airflow/
│   ├── dags/
│   │   └── ecommerce_etl_pipeline.py  # Main ETL DAG
│   ├── plugins/
│   └── requirements.txt
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_orders.sql
│   │   │   └── sources.yml
│   │   ├── warehouse/
│   │   │   └── fact_orders.sql
│   │   └── marts/
│   │       └── mart_customer_summary.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── credentials/
│   └── .gitkeep
├── docker-compose.yaml
├── .env.example
├── .gitignore
└── README.md
```

---

## 🧪 Testing

### DBT Tests

```
cd dbt_project
dbt test
```

**Tests include:**
- Unique & not null constraints
- Data quality checks
- Referential integrity

### Pipeline Testing

1. Trigger `ecommerce_etl_pipeline` DAG
2. Check all tasks complete successfully
3. Query BigQuery:

```
SELECT * FROM `your-project.analytics.mart_customer_summary`
ORDER BY total_amount DESC
LIMIT 10
```

---

## 💰 Cost Optimization

- **BigQuery partitioning** by date
- **Clustering** on frequently queried columns
- **Incremental DBT models** (planned)
- **Same-region deployment** (US)
- **Free tier usage**: 1TB query/month

**Estimated monthly cost**: $0-5 (within free tier)


---

## 🙏 Acknowledgments

- Apache Airflow community
- DBT Labs
- Google Cloud Platform

