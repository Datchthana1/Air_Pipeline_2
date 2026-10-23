import json

import pandas as pd
import pytest
import requests

from function import ingest_earthquake as eq


def make_event(utc='2026-01-01 03:04:05.123', thai='2026-01-01 10:04:05.123', magnitude='3.2',
               depth='10.9', lat='19.5', lon='98.2', title='ต.แม่ฮี้ อ.ปาย จ.แม่ฮ่องสอน (Mae Hong Son, Thailand)'):
    return {
        'DateTimeUTC': utc, 'DateTimeThai': thai, 'Magnitude': magnitude, 'Depth': depth,
        'Latitude': lat, 'Longitude': lon, 'TitleThai': title,
    }


# ---- E3: datetime ----------------------------------------------------------

@pytest.mark.parametrize('value, expected', [
    ('2026-01-01 03:04:05.123', '2026-01-01 03:04:05'),
    ('2026-01-01 03:04:05.999999', '2026-01-01 03:04:05'),   # truncated, not rounded
    ('2026-01-01 03:04:05', '2026-01-01 03:04:05'),          # FIX-5
    ('2026-01-01T03:04:05.1', None),
    ('', None),
    (None, None),
    (123, None),
    ('2026-13-01 00:00:00.0', None),
])
def test_to_datetime_text(value, expected):
    assert eq._to_datetime_text(value) == expected


# ---- E4 / E5: numbers ------------------------------------------------------

@pytest.mark.parametrize('value, expected', [
    ('3.2', 3.2),
    (' 3.5 ', 3.5),
    (4, 4.0),
    ('abc', None),
    (None, None),
    ('', None),
    ('nan', None),    # FIX-3
    ('inf', None),    # FIX-3
    ('-inf', None),   # FIX-3
])
def test_to_float(value, expected):
    assert eq._to_float(value) == expected


@pytest.mark.parametrize('value, expected', [
    ('10', 10),
    ('10.9', 10),
    ('-3.7', -3),     # truncates toward zero
    ('abc', None),
    (None, None),
    ('nan', None),
    ('', None),
    ('inf', None),    # FIX-4
])
def test_to_int(value, expected):
    assert eq._to_int(value) == expected


# ---- E6: location ----------------------------------------------------------

@pytest.mark.parametrize('title, expected', [
    ('ต.แม่ฮี้ อ.ปาย จ.แม่ฮ่องสอน (Mae Hong Son, Thailand)',
     ('แม่ฮี้', 'ปาย', 'แม่ฮ่องสอน', 'Mae Hong Son, Thailand')),
    ('อ.เมือง จ.เชียงราย (Chiang Rai)', (None, 'เมือง', 'เชียงราย', 'Chiang Rai')),
    ('จ.เชียงใหม่ (Chiang Mai)', (None, None, 'เชียงใหม่', 'Chiang Mai')),
    ('ประเทศเมียนมา (Myanmar)', (None, None, None, 'Myanmar')),
    ('อ.เมือง จ.เชียงราย', (None, None, None, None)),   # no paren: documented current behaviour
    ('จ.ตาก ()', (None, None, 'ตาก', None)),
    ('จ.ตาก(Tak)', (None, None, 'ตาก', 'Tak')),
    ('จ. ตาก  (Tak))', (None, None, 'ตาก', 'Tak')),
    (None, (None, None, None, None)),
    (123, (None, None, None, None)),
])
def test_parse_location(title, expected):
    assert eq._parse_location(title) == expected


# ---- E7 / E8: report -> frame ----------------------------------------------

def test_process_report_types_and_flags():
    report = {'DailyEarthquakes': [
        make_event(),
        make_event(utc='2026-01-01 05:00:00.000', lat='10.0', lon='100.0',
                   title='ประเทศเมียนมา (Myanmar)'),
    ]}
    df = eq.process_report(report)
    assert list(df.columns) == eq._EVENT_COLUMNS
    assert str(df.dtypes['depth_km']) == 'Int64'
    assert str(df.dtypes['magnitude']) == 'float64'
    assert str(df.dtypes['lat']) == 'float64'
    assert str(df.dtypes['lon']) == 'float64'
    assert str(df.dtypes['is_domestic']) == 'boolean'
    assert df.iloc[0]['province_th'] == 'แม่ฮ่องสอน'
    assert bool(df.iloc[0]['is_domestic']) is True
    assert bool(df.iloc[1]['is_domestic']) is False


@pytest.mark.parametrize('report', [{}, {'DailyEarthquakes': []}])
def test_process_report_empty_has_columns(report):
    df = eq.process_report(report)
    assert len(df) == 0
    assert list(df.columns) == eq._EVENT_COLUMNS


def test_process_reports_empty_list_has_columns():
    df = eq.process_reports([])
    assert len(df) == 0
    assert list(df.columns) == eq._EVENT_COLUMNS


def test_process_reports_concat_preserves_order():
    first = {'DailyEarthquakes': [make_event(magnitude='1.0', utc='2026-01-01 01:00:00.000')]}
    second = {'DailyEarthquakes': [make_event(magnitude='2.0', utc='2026-01-01 02:00:00.000')]}
    df = eq.process_reports([first, second])
    assert df['magnitude'].tolist() == [1.0, 2.0]
    assert df.index.tolist() == [0, 1]


# ---- E9 / E10 / FIX-6 / FIX-7: prepare for upsert ---------------------------

def test_prepare_events_dedupes_keep_first():
    df = eq.process_report({'DailyEarthquakes': [
        make_event(magnitude='3.0'),
        make_event(magnitude='3.5'),
    ]})
    records = eq.prepare_events_for_upsert(df)
    assert len(records) == 1
    assert records[0]['magnitude'] == 3.0


def test_prepare_events_drops_null_datetimes(caplog):
    df = eq.process_report({'DailyEarthquakes': [
        make_event(),
        make_event(utc='garbage', lat='11.0'),
    ]})
    with caplog.at_level('WARNING'):
        records = eq.prepare_events_for_upsert(df)
    assert len(records) == 1
    assert records[0]['lat'] == 19.5
    assert any('Dropped 1 event' in r.getMessage() for r in caplog.records)


def test_prepare_events_records_are_json_safe():
    df = eq.process_report({'DailyEarthquakes': [
        make_event(magnitude=None, depth=None, lat='nan', title='จ.เชียงใหม่ (Chiang Mai)'),
        make_event(utc='2026-01-01 06:00:00.000', depth='10', lat='12.0'),
    ]})
    records = eq.prepare_events_for_upsert(df)
    bad = next(r for r in records if r['lat'] is None or r['magnitude'] is None)
    assert bad['magnitude'] is None
    assert bad['depth_km'] is None
    assert bad['lat'] is None
    assert bad['tambon_th'] is None
    json.dumps(records, allow_nan=False)   # must not raise

    good = next(r for r in records if r['lat'] == 12.0)
    assert type(good['depth_km']) is int
    assert type(good['is_domestic']) is bool


def test_push_events_to_supabase_empty_returns_zero(fake_client):
    assert eq.push_events_to_supabase(eq.process_reports([])) == 0
    fake_client.table.assert_not_called()


def test_push_events_to_supabase_upsert_key(fake_client):
    df = eq.process_report({'DailyEarthquakes': [
        make_event(utc='2026-01-01 01:00:00.000'),
        make_event(utc='2026-01-01 02:00:00.000'),
        make_event(utc='2026-01-01 02:00:00.000'),   # duplicate key
    ]})
    count = eq.push_events_to_supabase(df)
    assert count == 2
    kwargs = fake_client.table.return_value.upsert.call_args.kwargs
    assert kwargs['on_conflict'] == 'datetime_utc,lat,lon'


# ---- Reload windows ----------------------------------------------------------

@pytest.mark.parametrize('mode, date, date_from, date_to, expected', [
    ('full', None, None, None, None),
    ('day', '2026-07-01', None, None, ('2026-07-01 00:00:00', '2026-07-01 23:59:59')),
    ('range', None, '2026-07-01', '2026-07-03', ('2026-07-01 00:00:00', '2026-07-03 23:59:59')),
])
def test_report_time_window(mode, date, date_from, date_to, expected):
    assert eq.report_time_window(mode, date, date_from, date_to) == expected


@pytest.mark.parametrize('mode, date, date_from, date_to', [
    ('day', None, None, None),
    ('range', None, '2026-07-01', None),
    ('weekly', None, None, None),
])
def test_report_time_window_invalid_raises(mode, date, date_from, date_to):
    with pytest.raises(eq.AirflowException):
        eq.report_time_window(mode, date, date_from, date_to)


def test_get_reports_latest_empty_raises(fake_client):
    fake_client.table.return_value.select.return_value.order.return_value.limit.return_value \
        .execute.return_value.data = []
    with pytest.raises(eq.AirflowException):
        eq.get_reports('latest')


def test_get_reports_day_filters_fetched_at(fake_client):
    q = fake_client.table.return_value.select.return_value.order.return_value
    q.gte.return_value.lte.return_value.execute.return_value.data = [{'payload': {'DailyEarthquakes': []}}]
    result = eq.get_reports('day', date='2026-07-01')
    q.gte.assert_called_once_with('fetched_at', '2026-07-01 00:00:00')
    assert result == [{'DailyEarthquakes': []}]


# ---- E1 / E2: raw report and connectivity ----------------------------------

def test_push_report_to_supabase_counts_events(fake_client, monkeypatch):
    monkeypatch.setattr(eq, 'now_bkk_text', lambda: '2026-07-01 10:00:00')
    payload = {'DailyEarthquakes': [make_event(), make_event(utc='2026-01-01 05:00:00.000')]}
    assert eq.push_report_to_supabase(payload) == 2
    fake_client.table.return_value.insert.assert_called_once_with(
        {'fetched_at': '2026-07-01 10:00:00', 'payload': payload})


def test_push_report_to_supabase_missing_key_counts_zero(fake_client, monkeypatch):
    monkeypatch.setattr(eq, 'now_bkk_text', lambda: '2026-07-01 10:00:00')
    assert eq.push_report_to_supabase({'Other': 1}) == 0


def test_check_api_connection_wraps_errors(monkeypatch):
    def boom(*args, **kwargs):
        raise requests.ConnectionError('no route')
    monkeypatch.setattr(eq.requests, 'get', boom)
    with pytest.raises(eq.AirflowException):
        eq.check_api_connection()
