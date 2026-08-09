import logging
import re
import pytz
import requests
import pandas as pd
from datetime import datetime
from airflow.exceptions import AirflowException
from function.ingest import client, SUPABASE_URL, SUPABASE_KEY

EARTHQUAKE_API_URL = 'https://data.tmd.go.th/api/DailySeismicEvent/v1/'
EARTHQUAKE_API_PARAMS = {'uid': 'demo', 'ukey': 'demokey', 'format': 'json'}


def check_api_connection() -> None:
    try:
        response = requests.get(
            EARTHQUAKE_API_URL,
            headers={'Content-Type': 'application/json'},
            params=EARTHQUAKE_API_PARAMS,
            timeout=30,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"TMD earthquake API is unreachable: {e}")
    logging.info(f"TMD earthquake API reachable (status {response.status_code})")


def check_supabase_connection() -> None:
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
    bkk_tz = pytz.timezone("Asia/Bangkok")
    fetched_at = datetime.now(bkk_tz).strftime('%Y-%m-%d %H:%M:%S')
    record = {"fetched_at": fetched_at, "payload": report_data}
    client.table(table).insert(record).execute()
    event_count = len(report_data.get('DailyEarthquakes', []))
    logging.info(f"Pushed earthquake report ({event_count} events) to '{table}' at {fetched_at}")
    return event_count


def get_latest_report(table: str = "earthquake_reports_raw") -> dict:
    response = client.table(table).select('payload').order('fetched_at', desc=True).limit(1).execute()
    if not response.data:
        raise AirflowException(f"No rows found in '{table}'")
    return response.data[0]['payload']


def get_reports(mode: str = 'latest', date: str = None, date_from: str = None,
                 date_to: str = None, table: str = "earthquake_reports_raw") -> list:
    """Return raw report payloads to (re)process, picked from what's already
    in `table` by `fetched_at`. This reprocesses history already captured by
    the pipeline - the TMD API itself has no date/range parameter, so it
    can't be asked for data from before the pipeline started polling it.
    """
    if mode == 'latest':
        return [get_latest_report(table)]

    query = client.table(table).select('payload').order('fetched_at')
    if mode == 'day' and date:
        query = query.gte('fetched_at', f"{date} 00:00:00").lte('fetched_at', f"{date} 23:59:59")
    elif mode == 'range' and date_from and date_to:
        query = query.gte('fetched_at', f"{date_from} 00:00:00").lte('fetched_at', f"{date_to} 23:59:59")
    elif mode != 'full':
        raise AirflowException(
            f"reload_mode={mode!r} needs a date (for 'day') or date_from/date_to (for 'range')"
        )

    response = query.execute()
    logging.info(f"{len(response.data)} report(s) to reprocess [{mode}]")
    return [row['payload'] for row in response.data]


_LOCATION_RE = re.compile(r'(?:ต\.(?P<tambon>.+?)\s+)?(?:อ\.(?P<amphoe>.+?)\s+)?จ\.(?P<province>.+?)\s*\(')


def _to_datetime_text(value):
    try:
        return datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')
    except (TypeError, ValueError):
        return None


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_location(title_th):
    if not isinstance(title_th, str):
        return None, None, None, None
    m = _LOCATION_RE.search(title_th)
    tambon = m.group('tambon').strip() if m and m.group('tambon') else None
    amphoe = m.group('amphoe').strip() if m and m.group('amphoe') else None
    province = m.group('province').strip() if m else None
    idx = title_th.rfind('(')
    location_en = title_th[idx + 1:].rstrip(')').strip() if idx != -1 else None
    return tambon, amphoe, province, (location_en or None)


_EVENT_COLUMNS = [
    'datetime_utc', 'datetime_thai', 'magnitude', 'depth_km', 'lat', 'lon',
    'title_th', 'tambon_th', 'amphoe_th', 'province_th', 'location_en', 'is_domestic',
]


def process_report(report_data: dict) -> pd.DataFrame:
    rows = []
    for event in report_data.get('DailyEarthquakes', []):
        title_th = event.get('TitleThai')
        tambon, amphoe, province, location_en = _parse_location(title_th)
        rows.append({
            'datetime_utc':  _to_datetime_text(event.get('DateTimeUTC')),
            'datetime_thai': _to_datetime_text(event.get('DateTimeThai')),
            'magnitude':     _to_float(event.get('Magnitude')),
            'depth_km':      _to_int(event.get('Depth')),
            'lat':           _to_float(event.get('Latitude')),
            'lon':           _to_float(event.get('Longitude')),
            'title_th':      title_th,
            'tambon_th':     tambon,
            'amphoe_th':     amphoe,
            'province_th':   province,
            'location_en':   location_en,
            'is_domestic':   province is not None,
        })
    # Explicit `columns=` keeps an empty report (0 events - a quiet day, or a
    # report reprocessed from a range with no matches) from producing a
    # columnless DataFrame that .astype() below would reject.
    df = pd.DataFrame(rows, columns=_EVENT_COLUMNS)
    return df.astype({
        'depth_km':    'Int64',
        'magnitude':   'float64',
        'lat':         'float64',
        'lon':         'float64',
        'is_domestic': 'boolean',
    })


def process_reports(reports: list) -> pd.DataFrame:
    frames = [process_report(r) for r in reports]
    if not frames:
        return process_report({})
    return pd.concat(frames, ignore_index=True)


def push_events_to_supabase(df: pd.DataFrame, table: str = "earthquake_events") -> int:
    if df.empty:
        logging.info("No events to push - nothing in range")
        return 0
    deduped = df.drop_duplicates(subset=['datetime_utc', 'lat', 'lon'], keep='first')
    if len(deduped) < len(df):
        logging.info(f"Dropped {len(df) - len(deduped)} duplicate events on (datetime_utc, lat, lon)")
    records = deduped.to_dict(orient='records')
    client.table(table).upsert(records, on_conflict='datetime_utc,lat,lon').execute()
    logging.info(f"Pushed {len(records)} earthquake events to '{table}'")
    return len(records)
