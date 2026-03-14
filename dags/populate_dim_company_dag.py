"""
DAG: populate_dim_company
=========================
One-shot manual trigger to seed dim_company with all 9 tracked stocks.
Run this once before starting the ETL DAGs.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys, os

sys.path.append("/opt/airflow/scripts")
from populate_dim_company import populate_dim_company

with DAG(
    dag_id="populate_dim_company",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["init", "dimension"],
    description="Seed dim_company with all 9 tracked NASDAQ stocks",
) as dag:
    PythonOperator(
        task_id="populate_dim_company_table",
        python_callable=populate_dim_company,
    )
