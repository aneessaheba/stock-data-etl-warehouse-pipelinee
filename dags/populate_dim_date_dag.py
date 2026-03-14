"""
DAG: populate_dim_date
======================
One-shot manual trigger to seed dim_date with every calendar day from
1980-01-01 to 2035-12-31 (covers entire price history and future dates).
Run this once before starting the ETL DAGs.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/scripts")
from populate_dim_date import generate_dates

with DAG(
    dag_id="populate_dim_date",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["init", "dimension"],
    description="Seed dim_date with every calendar day 1980–2035",
) as dag:
    PythonOperator(
        task_id="populate_dim_date_table",
        python_callable=generate_dates,
    )
