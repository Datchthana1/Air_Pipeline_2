from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from function.ingest import list_stations, transform_station, truncate_all_stations, resolve_reload
from function.reload_params import reload_params, RELOAD_CONF
import pendulum
import logging

local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    'owner': 'Dechthana Arunchaiya',
    'start_date': datetime(2026, 6, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

MAX_CONCURRENT_STATIONS = 5

with DAG(
    dag_id="PL1_transform_air_station",
    default_args=default_args,
    description=(
        "Transform air quality data per station into separate tables. "
        "Fans out one Airflow mapped task per station (dynamic task mapping) "
        "instead of one all-at-once server-side batch, so runtime per call "
        "stays bounded and doesn't grow into Postgres statement_timeout as "
        "data accumulates."
    ),
    schedule=None,
    catchup=False,
    params={
        **reload_params(default_mode="latest"),
        "truncate": Param(
            default=False,
            type="boolean",
            description="True = ล้างข้อมูลทุก station table (ใช้ manual trigger เท่านั้น)",
        ),
    },
) as dag:

    @task
    def run_truncate(**context) -> bool:
        if context["params"]["truncate"]:
            truncate_all_stations()
            return True
        return False

    @task
    def get_stations(did_truncate: bool, **context) -> list:
        if did_truncate:
            return []
        r = resolve_reload(context)
        return list_stations(
            mode=r["mode"],
            date=r["date"],
            date_from=r["date_from"],
            date_to=r["date_to"],
            station_id=r["station_id"],
        )

    @task(max_active_tis_per_dagrun=MAX_CONCURRENT_STATIONS)
    def transform_one_station(station_id: str, **context) -> str:
        r = resolve_reload(context)
        count = transform_station(
            station_id=station_id,
            mode=r["mode"],
            date=r["date"],
            date_from=r["date_from"],
            date_to=r["date_to"],
        )
        return f"{station_id}: {count} rows [{r['mode']}]"

    trigger_dim_fact = TriggerDagRunOperator(
        task_id="trigger_build_dim_fact",
        trigger_dag_id="PL2_build_dim_fact",
        conf=RELOAD_CONF,
        wait_for_completion=False,
    )

    truncated = run_truncate()
    stations = get_stations(truncated)
    transformed = transform_one_station.expand(station_id=stations)

    transformed >> trigger_dim_fact
