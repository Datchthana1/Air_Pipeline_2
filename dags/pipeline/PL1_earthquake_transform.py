from airflow import DAG
from airflow.decorators import task
import pandas as pd
from datetime import datetime, timedelta

with DAG(
    dag_id='PL1_transform_earthquake',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['PL1', 'Transform', 'EQ', 'earthquake'],
):
    @task()
    def process_report(report_data: dict) -> pd.DataFrame:
        def convert_to_datetime(date_str):
            try:
                return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return None

        def convert_to_float(value):
            try:
                return float(value)
            except ValueError:
                return None

        def convert_to_int(value):
            try:
                return int(value)
            except ValueError:
                return None

        df = pd.json_normalize(report_data['DailyEarthquakes'])
        df['DateTimeThai'] = df['DateTimeThai'].apply(convert_to_datetime)
        df['DateTimeUTC'] = df['DateTimeUTC'].apply(convert_to_datetime)
        df['Latitude'] = df['Latitude'].apply(convert_to_float)
        df['Longitude'] = df['Longitude'].apply(convert_to_float)
        df['Magnitude'] = df['Magnitude'].apply(convert_to_float)
        df['Depth'] = df['Depth'].apply(convert_to_int)
        return df
