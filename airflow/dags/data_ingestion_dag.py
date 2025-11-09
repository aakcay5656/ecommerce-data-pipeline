

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 6)
}


def create_sample_data():  # ← context KALDIRDIK
    """Sample e-commerce veri oluştur"""
    try:
        logger.info("📥 Sample data oluşturuluyor...")

        orders_data = {
            'order_id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'customer_id': [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            'order_date': [
                '2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05',
                '2025-01-06', '2025-01-07', '2025-01-08', '2025-01-09', '2025-01-10'
            ],
            'amount': [100.50, 250.00, 75.25, 150.00, 300.75, 200.00, 125.50, 350.00, 80.25, 175.00],
            'product': [
                'Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones',
                'Webcam', 'Desk', 'Chair', 'USB Cable', 'Mousepad'
            ],
            'status': [
                'completed', 'completed', 'pending', 'completed', 'completed',
                'completed', 'cancelled', 'completed', 'completed', 'pending'
            ]
        }

        df = pd.DataFrame(orders_data)
        csv_path = '/opt/airflow/data/raw/orders.csv'
        df.to_csv(csv_path, index=False)

        logger.info(f"✅ Data kaydedildi: {csv_path}")
        logger.info(f"📊 Total rows: {len(df)}")
        logger.info(f"💰 Total amount: ${df['amount'].sum():.2f}")

        return csv_path

    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise


def upload_to_bigquery():  # ← context KALDIRDIK
    """BigQuery'e yükle"""
    try:
        logger.info("📤 BigQuery'e yükleniyor...")

        csv_path = '/opt/airflow/data/raw/orders.csv'
        df = pd.read_csv(csv_path)

        logger.info(f"📋 Shape: {df.shape}")

        client = bigquery.Client()

        project_id = os.getenv('GCP_PROJECT_ID', 'ecommerce-data-pipeline-476900')
        dataset_id = os.getenv('BQ_RAW_DATASET', 'raw_data')
        table_id = f"{project_id}.{dataset_id}.orders"

        logger.info(f"📍 Table: {table_id}")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )

        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        load_job.result()

        destination_table = client.get_table(table_id)
        logger.info(f"✅ Loaded {destination_table.num_rows} rows to {table_id}")

        return table_id

    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise


# DAG - Airflow 3.x
with DAG(
        dag_id='ecommerce_data_ingestion',
        default_args=default_args,
        description='E-commerce data ingestion pipeline',
        schedule='@daily',
        catchup=False,
        tags=['data_engineering', 'ingestion', 'bigquery']
) as dag:
    create_data = PythonOperator(
        task_id='create_sample_data',
        python_callable=create_sample_data
        # ← provide_context=True KALDIRDIK
    )

    load_data = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=upload_to_bigquery
        # ← provide_context=True KALDIRDIK
    )

    verify = BashOperator(
        task_id='verify_data',
        bash_command='echo "✅ Pipeline completed successfully!" && date'
    )

    create_data >> load_data >> verify

