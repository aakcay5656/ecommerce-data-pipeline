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
git clone https://github.com/aakcay5656/ecommerce-data-pipeline.git
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

---
TR

# 🚀 E-ticaret Veri Pipeline'ı

Google Cloud Platform üzerinde uçtan uca veri mühendisliği projesi: **Airflow + DBT + BigQuery**

### Mimari

```
Extract (Airflow) → Load (BigQuery) → Transform (DBT) → Analytics
```

**Teknoloji Yığını:**
- **Orkestrasyon**: Apache Airflow 3.1 (Docker)
- **Dönüştürme**: DBT Core 1.8 + dbt-bigquery
- **Veri Ambarı**: Google BigQuery
- **Altyapı**: Docker Compose, GCP
- **CI/CD**: GitHub Actions (planlanıyor)
- **Dil**: Python 3.12

***

## 🏗️ Veri Mimarisi

### Veri Akışı

1. **Veri Toplama**: Airflow DAG örnek e-ticaret verilerini çeker
2. **Ham Katman**: Veriler `raw_data.orders` tablosuna yüklenir (BigQuery)
3. **Staging**: DBT verileri temizler ve tiplendirir → `staging.stg_orders`
4. **Warehouse**: DBT fact tablolarını oluşturur → `warehouse.fact_orders`
5. **Analytics**: DBT iş metriklerini üretir → `analytics.mart_customer_summary`

### Veri Setleri

| Dataset | Katman | Açıklama |
|---------|--------|----------|
| `raw_data` | Bronze | Ham toplanan veri |
| `staging` | Silver | Temizlenmiş & tiplendirilmiş veri |
| `warehouse` | Gold | Fact/dimension tabloları |
| `analytics` | Platinum | İş metrikleri |

***

## 🚀 Hızlı Başlangıç

### Ön Gereksinimler

- Docker & Docker Compose
- BigQuery aktif GCP hesabı
- Service account JSON anahtarı

### Kurulum

1. **Repository'yi klonla**
```bash
git clone https://github.com/aakcay5656/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

2. **Credentials'ları yapılandır**
```bash
# GCP service account anahtarını ekle
cp your-service-account.json credentials/gcp-key.json

# .env dosyasını oluştur
cp .env.example .env
# .env dosyasını GCP proje ID'nizle düzenleyin
```

3. **Airflow'u başlat**
```bash
docker-compose up -d
```

4. **Airflow UI'a eriş**
- URL: http://localhost:8080
- Kullanıcı adı: `airflow`
- Şifre: `airflow`

5. **Pipeline'ı tetikle**
- `ecommerce_etl_pipeline` DAG'ını bul
- "Trigger DAG" butonuna tıkla

---

## 📂 Proje Yapısı

```
ecommerce-data-pipeline/
├── airflow/
│   ├── dags/
│   │   └── ecommerce_etl_pipeline.py  # Ana ETL DAG
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

***

## 🧪 Test Etme

### DBT Testleri

```bash
cd dbt_project
dbt test
```

**Testler şunları içerir:**
- Unique & not null kısıtlamaları
- Veri kalitesi kontrolleri
- İlişkisel bütünlük

### Pipeline Testi

1. `ecommerce_etl_pipeline` DAG'ını tetikle
2. Tüm görevlerin başarıyla tamamlandığını kontrol et
3. BigQuery'de sorgula:

```sql
SELECT * FROM `your-project.analytics.mart_customer_summary`
ORDER BY total_amount DESC
LIMIT 10
```

***

## 💰 Maliyet Optimizasyonu

- **BigQuery partitioning** tarih bazlı
- Sık sorgulanan kolonlarda **Clustering**
- **Incremental DBT modelleri** (planlanıyor)
- **Aynı bölge deployment'ı** (US)
- **Ücretsiz kullanım**: Aylık 1TB sorgu

**Tahmini aylık maliyet**: $0-5 (ücretsiz kotada)



***

