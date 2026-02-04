"""
Airflow DAG for e-commerce batch data pipeline.

Pipeline:
1. Ingest raw CSV data into MySQL
2. Transform data into staging tables
3. Build analytics-ready fact and dimension tables
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    dag_id="ecommerce_batch_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw_data",
        bash_command="python3 /opt/project/scripts/ingest_raw_data.py"
    )

    transform_and_analytics = BashOperator(
    task_id="transform_and_analytics",
    bash_command="""
    mysql -h $MYSQL_HOST \
          -u $MYSQL_USER \
          -p$MYSQL_PASSWORD \
          $MYSQL_DATABASE < /opt/project/sql/transform_and_analytics.sql
    """,
)

build_analytics = BashOperator(
    task_id="build_analytics",
    bash_command="""
    mysql -h $MYSQL_HOST \
          -u $MYSQL_USER \
          -p$MYSQL_PASSWORD \
          $MYSQL_DATABASE < /opt/project/sql/transform_and_analytics.sql
    """
)


ingest_raw >> transform_staging >> build_analytics
