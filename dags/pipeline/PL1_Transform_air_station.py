from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime, timedelta
from function.ingest import transform_all, truncate_all_stations
import pendulum
import logging

local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    'owner': 'Dechthana Arunchaiya',
    'start_date': datetime(2026, 6, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="PL1_transform_air_station",
    default_args=default_args,
    description="Transform air quality data per station into separate tables (single server-side batch)",
    schedule=None,
    catchup=False,
    params={
        "truncate": Param(
            default=False,
            type="boolean",
            description="True = ล้างข้อมูลทุก station table (ใช้ manual trigger เท่านั้น)",
        ),
    },
) as dag:

    @task
    def run_transform(**context) -> str:
        if context["params"]["truncate"]:
            truncate_all_stations()
            return "truncated all station tables"

        # One server-side call transforms every station in the latest snapshot.
        count = transform_all('latest')
        return f"transformed {count} stations (batch)"

    run_transform()
