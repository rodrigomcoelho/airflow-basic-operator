from datetime import datetime
from os.path import dirname, join

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from fisia.financeiro.operators.pandas_to_bigquery import PandasToBigQueryOperator
from pendulum import timezone

with DAG(
    dag_id="sample_001",
    start_date=datetime(2023, 2, 1, tzinfo=timezone("America/Sao_Paulo")),
    catchup=False,
    default_args={
        "owner": "analytics.engineering",
    },
    schedule_interval="@daily",
) as dag:
    jobs = {
        "start": EmptyOperator(task_id="start"),
        "stop": EmptyOperator(task_id="stop"),
    }

    jobs["execute"] = PandasToBigQueryOperator(
        task_id="execute",
        source_code_path=join(dirname(__file__), "dataframe", "mycode.py"),
        destination_table="refined.fis_ref_gld_dim_data",
        location="US",
        project_id="PROJETO AQUI",
        write_mode="replace",
    )

    jobs["execute"].set_upstream(jobs["start"])
    jobs["execute"].set_downstream(jobs["stop"])
