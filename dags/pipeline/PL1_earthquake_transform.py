from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from function.ingest_earthquake import get_latest_report, process_report, push_events_to_supabase

with DAG(
    dag_id='PL1_transform_earthquake',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['PL1', 'Transform', 'EQ', 'earthquake'],
) as dag:

    @task()
    def fetch_latest_report():
        return get_latest_report()

    @task()
    def transform_and_push(report_data: dict):
        df = process_report(report_data)
        push_events_to_supabase(df)

    transform_and_push(fetch_latest_report())
