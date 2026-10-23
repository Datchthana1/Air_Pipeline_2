"""Static checks on the SQL files: no database needed.

They pin the bounds and regex that the cleaning rules rely on (S1, S6, S9)
and check that dq-checks.sql is read-only with a well-formed check list.
"""
import os
import re

import pytest

from conftest import REPO_ROOT

SQL_DIR = os.path.join(REPO_ROOT, 'dags', 'sqlscript')


def read_sql(name):
    with open(os.path.join(SQL_DIR, name), encoding='utf-8') as f:
        return f.read()


def safe_numeric_ref(v, lo, hi):
    """Python mirror of safe_numeric(v text, lo, hi) in air-station-transform.sql."""
    if v is None or not re.fullmatch(r'^-?\d+(\.\d+)?$', v, re.ASCII):
        return None
    x = float(v)
    return x if lo <= x <= hi else None


def test_safe_numeric_bounds():
    sql = read_sql('air-station-transform.sql')
    found = {col: (int(lo), int(hi)) for col, lo, hi in
             re.findall(r'safe_numeric\((\w+)::text,\s*(\d+),\s*(\d+)\)', sql)}
    assert found == {
        'pm25_value': (0, 900), 'pm25_aqi': (0, 500),
        'pm10_value': (0, 900), 'pm10_aqi': (0, 500),
        'o3_value': (0, 900), 'o3_aqi': (0, 500),
        'co_value': (0, 900), 'co_aqi': (0, 500),
        'no2_value': (0, 900), 'no2_aqi': (0, 500),
        'so2_value': (0, 600), 'so2_aqi': (0, 500),
        'aqi': (0, 500),
    }
    for col, (_, hi) in found.items():
        if col.endswith('_aqi') or col == 'aqi':
            assert hi == 500, col


def test_safe_numeric_regex_semantics():
    sql = read_sql('air-station-transform.sql')
    pattern = re.search(r"v !~ '([^']+)'", sql).group(1)
    assert pattern == r'^-?\d+(\.\d+)?$'


@pytest.mark.parametrize('s', ['12.5', '0', '-1', '900'])
def test_safe_numeric_accepts(s):
    assert re.fullmatch(r'^-?\d+(\.\d+)?$', s, re.ASCII) is not None


@pytest.mark.parametrize('s', ['', 'N/A', '1e3', '.5', '5.', ' 5', '+5', '12,5'])
def test_safe_numeric_rejects(s):
    assert re.fullmatch(r'^-?\d+(\.\d+)?$', s, re.ASCII) is None


def test_safe_numeric_reference_model():
    assert safe_numeric_ref('501', 0, 500) is None
    assert safe_numeric_ref('500', 0, 500) == 500
    assert safe_numeric_ref('-1', 0, 500) is None
    assert safe_numeric_ref(None, 0, 500) is None
    assert safe_numeric_ref('0', 0, 500) == 0


def test_build_dim_fact_drops_bad_timestamps():
    sql = read_sql('build-dim-fact.sql')
    assert 'exception when others then' in sql
    assert sql.count('where safe_timestamp(s.recorded_at) is not null') == 3
    assert 'join dim_station ds on ds.station_id = s.station_id' in sql


def _dq_statements():
    text = read_sql('dq-checks.sql')
    stripped = re.sub(r'--[^\n]*', '', text)
    return [p.strip() for p in stripped.split(';') if p.strip()]


def test_dq_checks_read_only():
    for piece in _dq_statements():
        assert re.match(r'(select|with)\b', piece, re.I), piece[:80]
        assert not re.search(r'\b(insert|update|delete|drop|alter|truncate|create|grant|set)\b',
                             piece, re.I), piece[:80]


def test_dq_checks_ids_and_headers():
    text = read_sql('dq-checks.sql')
    headers = re.findall(r'-- (DQ\d{2}) \[(error|warn)\]', text)
    ids = [h[0] for h in headers]
    assert ids == [f'DQ{n:02d}' for n in range(1, 24)]
    assert len(set(ids)) == len(ids)
    assert len(_dq_statements()) == len(headers)
