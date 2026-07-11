import logging
import pytz
import requests
from datetime import datetime
from airflow.exceptions import AirflowException
from function.ingest import client, SUPABASE_URL, SUPABASE_KEY

EARTHQUAKE_API_URL = 'https://data.tmd.go.th/api/DailySeismicEvent/v1/'
EARTHQUAKE_API_PARAMS = {'uid': 'demo', 'ukey': 'demokey', 'format': 'json'}


def check_api_connection() -> None:
    """Confirms the TMD earthquake API is reachable before the DAG bothers fetching."""
    try:
        response = requests.get(
            EARTHQUAKE_API_URL,
            headers={'Content-Type': 'application/json'},
            params=EARTHQUAKE_API_PARAMS,
            timeout=10,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"TMD earthquake API is unreachable: {e}")
    logging.info(f"TMD earthquake API reachable (status {response.status_code})")


def check_supabase_connection() -> None:
    """Hits the Supabase PostgREST root so a bad URL/key fails fast, before push_report_to_supabase."""
    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            timeout=10,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Supabase is unreachable: {e}")
    logging.info(f"Supabase reachable (status {response.status_code})")


def fetch_report_api() -> dict:
    response = requests.get(
        EARTHQUAKE_API_URL,
        headers={'Content-Type': 'application/json'},
        params=EARTHQUAKE_API_PARAMS,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def push_report_to_supabase(report_data: dict, table: str = "earthquake_reports_raw") -> int:
    """Stores the raw API payload as-is (JSONB) so PL1 can reprocess it without re-fetching."""
    bkk_tz = pytz.timezone("Asia/Bangkok")
    fetched_at = datetime.now(bkk_tz).strftime('%Y-%m-%d %H:%M:%S')
    record = {"fetched_at": fetched_at, "payload": report_data}
    client.table(table).insert(record).execute()
    event_count = len(report_data.get('DailyEarthquakes', []))
    logging.info(f"Pushed earthquake report ({event_count} events) to '{table}' at {fetched_at}")
    return event_count
