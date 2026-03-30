from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "movie-analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="movie_analytics_pipeline",
    default_args=default_args,
    description="Movie Analytics Pipeline using Airflow, Spark, and dbt",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    extract_raw = BashOperator(
        task_id="extract_raw",
        bash_command="python /opt/airflow/scripts/processing/extract_data.py",
    )

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="python /opt/airflow/scripts/ingestion/ingest_movies.py",
    )

    spark_enrich = BashOperator(
        task_id="spark_enrich",
        bash_command="python /opt/airflow/scripts/processing/enrich_bronze_spark.py",
    )

    clean_pandas = BashOperator(
        task_id="clean_pandas",
        bash_command="python /opt/airflow/scripts/processing/clean_pandas.py",
    )

    spark_transform = BashOperator(
        task_id="spark_transform",
        bash_command="python /opt/airflow/scripts/processing/transform_spark.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "mkdir -p /tmp/dbt_logs /tmp/dbt_target && "
            "cd /opt/airflow/dbt && "
            "DBT_LOG_PATH=/tmp/dbt_logs DBT_TARGET_PATH=/tmp/dbt_target "
            "dbt build --profiles-dir . --target prod"
        ),
    )

    (
        extract_raw
        >> ingest_raw
        >> spark_enrich
        >> clean_pandas
        >> spark_transform
        >> dbt_build
    )
