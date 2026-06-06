from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime, timedelta
import pendulum
import logging
from function.ingest import ingest_all, push_to_supabase

local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    'owner': 'Dechthana Arunchaiya',
    'start_date': datetime(2026, 6, 6),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def run_ingest():
    df = ingest_all()
    push_to_supabase(df)
    logging.info(f"Ingested {len(df)} rows successfully")
    

with DAG(
    dag_id="PL0_ingestion_air_station",
    default_args=default_args,
    description="Ingest air quality data from AIR4Thai and OpenWeather to Supabase",
    schedule=CronDataIntervalTimetable(
        cron="0 * * * *",
        timezone=local_tz,
    ),
    catchup=False,
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_openweather_air4thai",
        python_callable=run_ingest,
    )
