"""
Monthly Aggregate DAG
=====================
Runs at the end of each month to populate fact_stock_price_monthly
from daily price data via aggregate_monthly.sql.
"""

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="monthly_aggregate_dag",
    default_args=default_args,
    schedule_interval="@monthly",
    template_searchpath=["/opt/airflow/sql"],
    catchup=False,
    tags=["aggregate", "monthly"],
    description="Aggregates daily prices into monthly summaries",
) as dag:

    ensure_schema = PostgresOperator(
        task_id="ensure_schema",
        postgres_conn_id="timescaledb_conn",
        sql="schema.sql",
    )

    aggregate = PostgresOperator(
        task_id="aggregate_monthly_data",
        postgres_conn_id="timescaledb_conn",
        sql="aggregate_monthly.sql",
    )

    ensure_schema >> aggregate
