from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime, timedelta
from function.ingest import ingest_all, push_to_supabase
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
    dag_id="PL0_ingestion_air_station",
    default_args=default_args,
    description="Ingest air quality data from AIR4Thai and OpenWeather to Supabase",
    schedule=CronDataIntervalTimetable(
        cron="30 * * * *",
        timezone=local_tz,
    ),
    catchup=False,
) as dag:

    @task
    def ingest():
        df = ingest_all()
        push_to_supabase(df)
        logging.info(f"Ingested {len(df)} rows successfully")

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_pipeline",
        trigger_dag_id="PL1_transform_air_station",
        wait_for_completion=False,
    )

    ingest() >> trigger_transform
