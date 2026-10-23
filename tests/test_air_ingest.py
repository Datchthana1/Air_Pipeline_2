import logging

import pandas as pd
import pytest
import requests

from function import ingest
from function.ingest import build_station_row, parse_openweather


def make_station(**overrides):
    station = {
        'stationID': '33t',
        'areaTH': 'ต.ทดสอบ',
        'areaEN': 'Mueang, Bangkok',
        'stationType': 'GROUND',
        'lat': '13.7',
        'long': '100.5',
        'AQILast': {
            'date': '2026-07-01',
            'time': '10:00',
            'AQI': {'aqi': '42', 'param': 'PM25'},
            **{p: {'value': '12.3', 'aqi': '21'} for p in ('PM25', 'PM10', 'O3', 'CO', 'NO2', 'SO2')},
        },
    }
    station.update(overrides)
    return station


def station_without(key):
    station = make_station()
    del station[key]
    return station


OW_AIR = {'list': [{'components': {'co': 1, 'no': 2, 'no2': 3, 'o3': 4, 'so2': 5,
                                    'pm2_5': 12.3, 'pm10': 20, 'nh3': 6}}]}
OW_WX = {'main': {'temp': 30, 'feels_like': 33, 'humidity': 70, 'pressure': 1009},
         'wind': {'speed': 2.5, 'deg': 180},
         'clouds': {'all': 40},
         'weather': [{'description': 'light rain'}]}

OW_KEYS = [
    'ow_aqi', 'ow_co', 'ow_no', 'ow_no2', 'ow_o3', 'ow_so2', 'ow_pm25', 'ow_pm10', 'ow_nh3',
    'ow_temp', 'ow_feels_like', 'ow_humidity', 'ow_pressure', 'ow_wind_speed', 'ow_wind_deg',
    'ow_clouds', 'ow_weather',
]

ROW_KEYS = [
    'station_id', 'area_th', 'area_en', 'station_type', 'lat', 'lon', 'recorded_at',
    'aqi', 'aqi_param', 'pm25_value', 'pm25_aqi', 'pm10_value', 'pm10_aqi', 'o3_value', 'o3_aqi',
    'co_value', 'co_aqi', 'no2_value', 'no2_aqi', 'so2_value', 'so2_aqi',
] + OW_KEYS + ['created_at']


def test_parse_openweather_maps_fields():
    ow = parse_openweather(OW_AIR, OW_WX)
    assert ow['ow_aqi'] == 21
    assert ow['ow_pm25'] == 12.3
    assert ow['ow_weather'] == 'light rain'
    assert list(ow.keys()) == OW_KEYS


def test_parse_openweather_empty_list_raises_indexerror():
    with pytest.raises(IndexError):
        parse_openweather({'list': []}, OW_WX)


def test_parse_openweather_missing_component_raises_keyerror():
    air = {'list': [{'components': {'co': 1}}]}
    with pytest.raises(KeyError):
        parse_openweather(air, OW_WX)


def test_build_station_row_preserves_raw_values():
    ow = parse_openweather(OW_AIR, OW_WX)
    row = build_station_row(make_station(), ow, '2026-07-01 10:45:00')
    assert row['pm25_value'] == '12.3'           # A6: strings pass through unchanged
    assert isinstance(row['lat'], float) and row['lat'] == 13.7   # A3
    assert isinstance(row['lon'], float) and row['lon'] == 100.5
    assert row['recorded_at'] == '2026-07-01 10:00'               # A7
    assert row['created_at'] == '2026-07-01 10:45:00'
    assert list(row.keys()) == ROW_KEYS


def test_build_station_row_bad_lat_raises_valueerror():
    with pytest.raises(ValueError):
        build_station_row(make_station(lat='abc'), parse_openweather(OW_AIR, OW_WX), 'x')


def test_ingest_all_skips_bad_stations(monkeypatch, caplog):
    stations = [
        make_station(),
        station_without('AQILast'),
        None,
        'garbage',
        make_station(stationID='o24', lat='18.8', long='98.9'),
    ]
    monkeypatch.setattr(ingest, 'fetch_air4thai', lambda: stations)

    def fake_ow(lat, lon):
        if lat == '18.8':
            raise requests.Timeout('timed out')
        return parse_openweather(OW_AIR, OW_WX)

    monkeypatch.setattr(ingest, 'fetch_openweather', fake_ow)
    monkeypatch.setattr(ingest, 'now_bkk_text', lambda: '2026-07-01 10:45:00')

    with caplog.at_level(logging.WARNING):
        df = ingest.ingest_all()

    assert len(df) == 1
    assert df['station_id'].tolist() == ['33t']
    assert sum('Skipping station' in r.getMessage() for r in caplog.records) >= 3
    assert df['created_at'].unique().tolist() == ['2026-07-01 10:45:00']   # A8


def test_ingest_all_does_not_call_openweather_for_malformed_station(monkeypatch):
    calls = []

    def counting_ow(lat, lon):
        calls.append(lat)
        return parse_openweather(OW_AIR, OW_WX)

    monkeypatch.setattr(ingest, 'fetch_air4thai', lambda: [station_without('lat'), make_station()])
    monkeypatch.setattr(ingest, 'fetch_openweather', counting_ow)
    df = ingest.ingest_all()
    assert calls == ['13.7']          # only the valid station used OpenWeather quota
    assert len(df) == 1


def test_ingest_all_empty_ow_list_is_skipped_not_fatal(monkeypatch):
    def fake_ow(lat, lon):
        if lat == '13.7':
            return parse_openweather({'list': []}, OW_WX)   # FIX-1: IndexError
        return parse_openweather(OW_AIR, OW_WX)

    monkeypatch.setattr(ingest, 'fetch_air4thai',
                        lambda: [make_station(), make_station(stationID='o24', lat='18.8', long='98.9')])
    monkeypatch.setattr(ingest, 'fetch_openweather', fake_ow)
    df = ingest.ingest_all()
    assert len(df) == 1
    assert df['station_id'].tolist() == ['o24']


def test_ingest_all_all_failed_raises_runtimeerror(monkeypatch):
    monkeypatch.setattr(ingest, 'fetch_air4thai',
                        lambda: [station_without('AQILast'), station_without('lat')])
    monkeypatch.setattr(ingest, 'fetch_openweather', lambda lat, lon: parse_openweather(OW_AIR, OW_WX))
    with pytest.raises(RuntimeError, match='All 2 station'):
        ingest.ingest_all()


def test_ingest_all_empty_station_list_returns_empty_df(monkeypatch):
    monkeypatch.setattr(ingest, 'fetch_air4thai', lambda: [])
    df = ingest.ingest_all()
    assert isinstance(df, pd.DataFrame) and df.empty


def test_push_to_supabase_empty_df_skips(fake_client):
    ingest.push_to_supabase(pd.DataFrame())
    fake_client.table.assert_not_called()


def test_push_to_supabase_upserts_on_natural_key(fake_client):
    df = pd.DataFrame([build_station_row(make_station(), parse_openweather(OW_AIR, OW_WX), 'x')])
    ingest.push_to_supabase(df)
    fake_client.table.assert_called_once_with('air_stations')
    kwargs = fake_client.table.return_value.upsert.call_args.kwargs
    assert kwargs['on_conflict'] == 'station_id,recorded_at'


@pytest.mark.parametrize('func, rpc_name', [
    (ingest.list_stations, 'get_distinct_stations'),
    (ingest.build_dim_fact, 'build_dim_fact'),
    (ingest.transform_all, 'transform_all_stations'),
])
def test_rpc_wrappers_blank_to_none(fake_client, func, rpc_name):
    func(mode='day', date='2026-07-01', date_from='', date_to='', station_id='')
    fake_client.rpc.assert_called_once_with(rpc_name, {
        'p_mode': 'day', 'p_date': '2026-07-01', 'p_date_from': None,
        'p_date_to': None, 'p_station_id': None,
    })


def test_list_stations_returns_ids(fake_client):
    fake_client.rpc.return_value.execute.return_value.data = [{'station_id': '33t'}]
    assert ingest.list_stations() == ['33t']


def test_transform_station_rpc_args(fake_client):
    ingest.transform_station('33t', mode='range', date_from='2026-07-01', date_to='2026-07-02')
    fake_client.rpc.assert_called_once_with('transform_station', {
        'p_station_id': '33t', 'p_mode': 'range', 'p_date': None,
        'p_date_from': '2026-07-01', 'p_date_to': '2026-07-02',
    })
