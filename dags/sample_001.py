from datetime import datetime
from pendulum import timezone
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from fisia.operators.pandas_to_bigquery import PandasToBigQueryOperator

with DAG(
    dag_id="sample_001",
    start_date=datetime(2023, 2, 1, tzinfo=timezone("America/Sao_Paulo")),
    default_args={
        "owner": "analytics.engineering",
    },
    schedule_interval="@daily",
) as dag:

    jobs = {
        "start": EmptyOperator(task_id="start"),
        "stop": EmptyOperator(task_id="stop"),
    }

    jobs["execute"] = PandasToBigQueryOperator(task_id="execute")

    jobs["execute"].set_upstream(jobs["start"])
    jobs["execute"].set_downstream(jobs["stop"])
