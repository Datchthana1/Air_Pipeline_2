from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from function.ingest_earthquake import (
    check_api_connection,
    check_supabase_connection,
    fetch_report_api,
    push_report_to_supabase,
)
from function.reload_params import reload_params, EARTHQUAKE_RELOAD_CONF

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
}

with DAG(
    dag_id='PL0_ingestion_earthquake',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['PL0', 'Ingest', 'EQ', 'earthquake'],
    params=reload_params(default_mode="latest", include_station_id=False),
) as dag:

    @task()
    def check_connections():
        check_api_connection()
        check_supabase_connection()

    @task()
    def fetch_report():
        return fetch_report_api()

    @task()
    def push_report(report_data: dict):
        push_report_to_supabase(report_data)

    trigger_PL1_transform = TriggerDagRunOperator(
        task_id='trigger_PL1_transform',
        trigger_dag_id='PL1_transform_earthquake',
        conf=EARTHQUAKE_RELOAD_CONF,
        wait_for_completion=False,
    )

    report_data = fetch_report()

    check_connections() >> report_data >> push_report(report_data) >> trigger_PL1_transform
