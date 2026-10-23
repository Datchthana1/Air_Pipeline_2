import re
import sys
import types
from datetime import datetime

import pytz
import pytest

from function import common


def test_now_bkk_text_format():
    assert re.fullmatch(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', common.now_bkk_text())


def test_now_bkk_text_is_bangkok(monkeypatch):
    fixed_utc = datetime(2026, 1, 1, 0, 0, tzinfo=pytz.utc)

    class FakeDatetime:
        @staticmethod
        def now(tz):
            return fixed_utc.astimezone(tz)

    monkeypatch.setattr(common, 'datetime', FakeDatetime)
    assert common.now_bkk_text() == '2026-01-01 07:00:00'


@pytest.mark.parametrize('conf, params, expected', [
    # (a) conf wins over params
    ({'reload_mode': 'day', 'reload_date': '2026-07-01'}, {'reload_mode': 'latest'},
     {'mode': 'day', 'date': '2026-07-01', 'date_from': None, 'date_to': None, 'station_id': None}),
    # (b) empty conf value counts as missing, falls back to params
    ({'reload_mode': ''}, {'reload_mode': 'range', 'date_from': '2026-07-01', 'date_to': '2026-07-03'},
     {'mode': 'range', 'date': None, 'date_from': '2026-07-01', 'date_to': '2026-07-03', 'station_id': None}),
    # (f) station_id from conf
    ({'station_id': '33t'}, {}, {'mode': 'latest', 'date': None, 'date_from': None, 'date_to': None, 'station_id': '33t'}),
])
def test_resolve_reload_precedence(conf, params, expected):
    ctx = {'dag_run': types.SimpleNamespace(conf=conf), 'params': params}
    assert common.resolve_reload(ctx) == expected


def test_resolve_reload_no_dag_run_uses_defaults():
    assert common.resolve_reload({'dag_run': None, 'params': {}}) == {
        'mode': 'latest', 'date': None, 'date_from': None, 'date_to': None, 'station_id': None,
    }


def test_resolve_reload_conf_none():
    ctx = {'dag_run': types.SimpleNamespace(conf=None), 'params': {'reload_mode': 'full'}}
    assert common.resolve_reload(ctx)['mode'] == 'full'


def test_resolve_reload_blank_station_id_is_none():
    ctx = {'dag_run': None, 'params': {'station_id': ''}}
    assert common.resolve_reload(ctx)['station_id'] is None


def test_common_imports_without_airflow_or_supabase():
    assert callable(common.get_client)
    common.get_client.cache_clear()
    assert common.get_client.cache_info().currsize == 0
    assert 'airflow' not in common.__dict__
