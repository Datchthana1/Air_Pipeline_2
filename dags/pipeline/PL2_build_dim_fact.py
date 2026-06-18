from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from function.ingest import build_dim_fact, resolve_reload
from function.reload_params import reload_params
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
    dag_id="PL2_build_dim_fact",
    default_args=default_args,
    description="Build the star-schema mart (dim_station + dim_date + fact_air_quality) from the per-station tables",
    schedule=None,
    catchup=False,
    params=reload_params(default_mode="latest"),
) as dag:

    @task
    def run_build(**context) -> str:
        r = resolve_reload(context)
        count = build_dim_fact(
            mode=r["mode"],
            date=r["date"],
            date_from=r["date_from"],
            date_to=r["date_to"],
            station_id=r["station_id"],
        )
        return f"built dim/fact mart: {count} fact rows [{r['mode']}]"

    run_build()
