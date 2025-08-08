from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import sys
import os

# ====================================
# Add scripts directory to sys.path
# ====================================
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from extract import extract_data_from_all_endpoints
from transform import transform_and_load_data_from_all_endpoints

# ====================================
# Default arguments for the DAG
# ====================================
default_args = {
    'owner': 'gabcosta',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# ====================================
# Define the DAG
# ====================================
with DAG(
    dag_id='etl_sap_pipeline',
    default_args=default_args,
    description="""
        Extract data from SAP fake data API and save to MinIO,
        then transform the data tables and upload to Postgres DB.
    """,
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=['pipeline', 'etl'],
) as dag:

    extract_operator = PythonOperator(
        task_id='extract_all_endpoints',
        python_callable=extract_data_from_all_endpoints,
    )

    transform_operator = PythonOperator(
        task_id='transform_and_load',
        python_callable=transform_and_load_data_from_all_endpoints,
    )

    load_dim_products = PostgresOperator(
        task_id='load_dim_products',
        postgres_conn_id='postgres_dw_conn',
        sql='CALL process_dim_products();'
    )

    # Task dependencies
    extract_operator >> transform_operator >> load_dim_products
