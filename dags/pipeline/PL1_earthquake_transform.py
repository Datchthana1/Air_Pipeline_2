from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from function.ingest import resolve_reload
from function.ingest_earthquake import get_reports, process_reports, push_events_to_supabase
from function.reload_params import reload_params

with DAG(
    dag_id='PL1_transform_earthquake',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['PL1', 'Transform', 'EQ', 'earthquake'],
    params=reload_params(default_mode="latest", include_station_id=False),
) as dag:

    @task()
    def fetch_reports(**context):
        r = resolve_reload(context)
        return get_reports(mode=r["mode"], date=r["date"], date_from=r["date_from"], date_to=r["date_to"])

    @task()
    def transform_and_push(reports: list) -> str:
        df = process_reports(reports)
        count = push_events_to_supabase(df)
        return f"{len(reports)} report(s) reprocessed -> {count} unique event(s) upserted"

    transform_and_push(fetch_reports())
