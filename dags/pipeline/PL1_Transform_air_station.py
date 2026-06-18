from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from function.ingest import transform_all, truncate_all_stations, resolve_reload
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

with DAG(
    dag_id="PL1_transform_air_station",
    default_args=default_args,
    description="Transform air quality data per station into separate tables (single server-side batch)",
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
    def run_transform(**context) -> str:
        if context["params"]["truncate"]:
            truncate_all_stations()
            return "truncated all station tables"

        r = resolve_reload(context)
        count = transform_all(
            mode=r["mode"],
            date=r["date"],
            date_from=r["date_from"],
            date_to=r["date_to"],
            station_id=r["station_id"],
        )
        return f"transformed {count} stations [{r['mode']}]"

    trigger_dim_fact = TriggerDagRunOperator(
        task_id="trigger_build_dim_fact",
        trigger_dag_id="PL2_build_dim_fact",
        conf=RELOAD_CONF,
        wait_for_completion=False,
    )

    run_transform() >> trigger_dim_fact
