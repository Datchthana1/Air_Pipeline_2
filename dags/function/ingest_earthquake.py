"""TMD daily earthquake report: fetch, store raw payloads, parse events, and upsert them to Supabase.

Cleaning rules for this module are listed in docs/DATA_CLEANING.md (E1-E12).
"""
from __future__ import annotations

import logging
import math
import re
from datetime import datetime

import pandas as pd
import requests
from airflow.exceptions import AirflowException

from function.common import HTTP_TIMEOUT, SUPABASE_KEY, SUPABASE_URL, get_client, now_bkk_text

EARTHQUAKE_API_URL = 'https://data.tmd.go.th/api/DailySeismicEvent/v1/'
EARTHQUAKE_API_PARAMS = {'uid': 'demo', 'ukey': 'demokey', 'format': 'json'}
_TMD_HEADERS = {'Content-Type': 'application/json'}
SUPABASE_CHECK_TIMEOUT = 10
_DATETIME_INPUT_FORMATS = ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S')
_DATETIME_OUTPUT_FORMAT = '%Y-%m-%d %H:%M:%S'
EVENT_KEY = ['datetime_utc', 'lat', 'lon']


def _get_tmd() -> requests.Response:
    """GET the TMD endpoint and raise for HTTP errors."""
    response = requests.get(
        EARTHQUAKE_API_URL,
        headers=_TMD_HEADERS,
        params=EARTHQUAKE_API_PARAMS,
        timeout=HTTP_TIMEOUT,
    )
    response.raise_for_status()
    return response


def check_api_connection() -> None:
    """Fail the Airflow task if the TMD API cannot be reached."""
    try:
        response = _get_tmd()
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"TMD earthquake API is unreachable: {e}")
    logging.info(f"TMD earthquake API reachable (status {response.status_code})")


def check_supabase_connection() -> None:
    """Fail the Airflow task if the Supabase REST endpoint cannot be reached."""
    try:
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/",
            headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
            timeout=SUPABASE_CHECK_TIMEOUT,
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise AirflowException(f"Supabase is unreachable: {e}")
    logging.info(f"Supabase reachable (status {response.status_code})")


def fetch_report_api() -> dict:
    """Return the raw TMD daily-seismic JSON payload."""
    return _get_tmd().json()


def push_report_to_supabase(report_data: dict, table: str = "earthquake_reports_raw") -> int:
    """Insert the raw report as a jsonb row and return how many events it contains."""
    fetched_at = now_bkk_text()
    record = {"fetched_at": fetched_at, "payload": report_data}
    get_client().table(table).insert(record).execute()
    event_count = len(report_data.get('DailyEarthquakes', []))
    logging.info(f"Pushed earthquake report ({event_count} events) to '{table}' at {fetched_at}")
    return event_count


def get_latest_report(table: str = "earthquake_reports_raw") -> dict:
    """Return the payload of the newest stored report. Raises AirflowException when the table is empty."""
    response = get_client().table(table).select('payload').order('fetched_at', desc=True).limit(1).execute()
    if not response.data:
        raise AirflowException(f"No rows found in '{table}'")
    return response.data[0]['payload']


def report_time_window(mode: str, date: str | None, date_from: str | None,
                       date_to: str | None) -> tuple[str, str] | None:
    """Return the fetched_at (start, end) text window for a reload mode, or None for 'full'.

    Raises AirflowException when 'day' lacks a date or 'range' lacks both dates, or the mode is unknown.
    """
    if mode == 'full':
        return None
    if mode == 'day' and date:
        return (f"{date} 00:00:00", f"{date} 23:59:59")
    if mode == 'range' and date_from and date_to:
        return (f"{date_from} 00:00:00", f"{date_to} 23:59:59")
    raise AirflowException(
        f"reload_mode={mode!r} needs a date (for 'day') or date_from/date_to (for 'range')"
    )


def get_reports(mode: str = 'latest', date: str | None = None, date_from: str | None = None,
                date_to: str | None = None, table: str = "earthquake_reports_raw") -> list:
    """Return raw report payloads to (re)process, picked from what's already
    in `table` by `fetched_at`. This reprocesses history already captured by
    the pipeline - the TMD API itself has no date/range parameter, so it
    can't be asked for data from before the pipeline started polling it.
    """
    if mode == 'latest':
        return [get_latest_report(table)]

    window = report_time_window(mode, date, date_from, date_to)
    query = get_client().table(table).select('payload').order('fetched_at')
    if window:
        query = query.gte('fetched_at', window[0]).lte('fetched_at', window[1])

    response = query.execute()
    logging.info(f"{len(response.data)} report(s) to reprocess [{mode}]")
    return [row['payload'] for row in response.data]


_LOCATION_RE = re.compile(r'(?:ต\.(?P<tambon>.+?)\s+)?(?:อ\.(?P<amphoe>.+?)\s+)?จ\.(?P<province>.+?)\s*\(')


def _to_datetime_text(value) -> str | None:
    """Parse a TMD timestamp (with or without fractional seconds) to 'YYYY-MM-DD HH:MM:SS'.

    Fractional seconds are truncated, not rounded. Returns None when the value is not a string or cannot be parsed.
    """
    for fmt in _DATETIME_INPUT_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime(_DATETIME_OUTPUT_FORMAT)
        except (TypeError, ValueError):
            continue
    return None


def _to_float(value) -> float | None:
    """Convert to float; None for missing, invalid, NaN or infinite input."""
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _to_int(value) -> int | None:
    """Convert via float() and truncate toward zero; None for missing, invalid, NaN or infinite input."""
    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_location(title_th):
    """Split a Thai TMD title into (tambon, amphoe, province, location_en).

    The province is required and must be followed by '('. Tambon and amphoe are optional.
    location_en is the text after the last '(' with the trailing ')' removed. Empty values become None.
    """
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
    """Turn one raw TMD report into a typed events DataFrame with the 12 _EVENT_COLUMNS.

    An empty report yields a zero-row frame that still has all columns.
    """
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
    """Concatenate process_report over reports in input order. An empty list returns the empty frame."""
    frames = [process_report(r) for r in reports]
    if not frames:
        return process_report({})
    return pd.concat(frames, ignore_index=True)


def _is_missing(v) -> bool:
    return v is None or v is pd.NA or (isinstance(v, float) and math.isnan(v))


def prepare_events_for_upsert(df: pd.DataFrame) -> list[dict]:
    """Return JSON-safe upsert records: null datetimes dropped (FIX-6), duplicates on EVENT_KEY removed
    keeping the first, and NaN/pd.NA converted to None (FIX-7).
    """
    valid = df.dropna(subset=['datetime_utc', 'datetime_thai'])
    dropped_null = len(df) - len(valid)
    if dropped_null:
        logging.warning("Dropped %d event(s) with unparseable datetime_utc/datetime_thai", dropped_null)
    deduped = valid.drop_duplicates(subset=EVENT_KEY, keep='first')
    if len(deduped) < len(valid):
        logging.info(f"Dropped {len(valid) - len(deduped)} duplicate events on (datetime_utc, lat, lon)")
    records = deduped.to_dict(orient='records')
    return [{k: (None if _is_missing(v) else v) for k, v in r.items()} for r in records]


def push_events_to_supabase(df: pd.DataFrame, table: str = "earthquake_events") -> int:
    """Upsert events on (datetime_utc, lat, lon) and return how many records were sent."""
    if df.empty:
        logging.info("No events to push - nothing in range")
        return 0
    records = prepare_events_for_upsert(df)
    if not records:
        logging.info("No valid events to push")
        return 0
    get_client().table(table).upsert(records, on_conflict='datetime_utc,lat,lon').execute()
    logging.info(f"Pushed {len(records)} earthquake events to '{table}'")
    return len(records)
