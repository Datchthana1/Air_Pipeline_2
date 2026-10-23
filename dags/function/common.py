"""Shared config and helpers for the ingest modules. Import-safe: no Airflow, no network, no DB at import time."""
from __future__ import annotations

import functools
import os
from datetime import datetime

import pytz
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../assets/.env'))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

BKK_TZ = pytz.timezone("Asia/Bangkok")
TIMESTAMP_FMT = '%Y-%m-%d %H:%M:%S'
HTTP_TIMEOUT = 30          # seconds, AIR4Thai / OpenWeather / TMD
RELOAD_MODES = ("latest", "day", "range", "full")


@functools.lru_cache(maxsize=1)
def get_client():
    """Return the process-wide Supabase client, creating it on first use."""
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def now_bkk_text() -> str:
    """Current Asia/Bangkok wall-clock time as 'YYYY-MM-DD HH:MM:SS' (no offset)."""
    return datetime.now(BKK_TZ).strftime(TIMESTAMP_FMT)


def resolve_reload(context) -> dict:
    """Resolve reload parameters from an Airflow context.

    Precedence: dag_run.conf over params. '' and None both count as missing.
    Defaults: mode='latest', date/date_from/date_to/station_id=None.
    """
    dag_run = context.get("dag_run")
    conf = (dag_run.conf if dag_run and dag_run.conf else None) or {}
    params = context.get("params") or {}

    def pick(key, default=None):
        value = conf.get(key)
        if value is None or value == "":
            value = params.get(key, default)
        return value if value not in ("", None) else default

    return {
        "mode":       pick("reload_mode", "latest"),
        "date":       pick("reload_date"),
        "date_from":  pick("date_from"),
        "date_to":    pick("date_to"),
        "station_id": pick("station_id"),
    }
