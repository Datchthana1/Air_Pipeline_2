from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from datetime import datetime, timedelta
import pendulum
import logging
from function.ingest import get_stationfromsupabase, get_latest_created_at, transform_station, truncate_all_stations

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
    description="Transform air quality data per station into separate tables",
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
    def get_inputs(**context) -> list:
        if context["params"]["truncate"]:
            truncate_all_stations()
            return []

        snapshot_at = get_latest_created_at()
        stations = get_stationfromsupabase()
        logging.info(f"Found {len(stations)} stations | snapshot_at={snapshot_at}")
        return [{"station_id": s, "snapshot_at": snapshot_at} for s in stations]

    @task
    def transform(item: dict):
        transform_station(
            station_id=item["station_id"],
            snapshot_at=item["snapshot_at"],
        )

    transform.expand(item=get_inputs())
