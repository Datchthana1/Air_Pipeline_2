import os
import requests
from dotenv import load_dotenv
import pandas as pd
from supabase import create_client
from datetime import datetime
import pytz

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../assets/.env'))

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

air4thai_url      = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
openweather_air   = "https://api.openweathermap.org/data/2.5/air_pollution"
openweather_wx    = "https://api.openweathermap.org/data/2.5/weather"


def fetch_air4thai() -> list:
    response = requests.get(air4thai_url, timeout=30)
    response.raise_for_status()
    return response.json()['stations']


def fetch_openweather(lat: float, lon: float) -> dict:
    api_key = os.getenv("OPENWEATHER_API_KEY")
    params  = {"lat": lat, "lon": lon, "appid": api_key}

    air     = requests.get(openweather_air, params=params, timeout=30)
    weather = requests.get(openweather_wx,  params={**params, "units": "metric"}, timeout=30)

    air.raise_for_status()
    weather.raise_for_status()

    air_data = air.json()['list'][0]
    wx_data  = weather.json()

    return {
        "ow_aqi":         air_data['main']['aqi'],
        "ow_co":          air_data['components']['co'],
        "ow_no":          air_data['components']['no'],
        "ow_no2":         air_data['components']['no2'],
        "ow_o3":          air_data['components']['o3'],
        "ow_so2":         air_data['components']['so2'],
        "ow_pm25":        air_data['components']['pm2_5'],
        "ow_pm10":        air_data['components']['pm10'],
        "ow_nh3":         air_data['components']['nh3'],
        "ow_temp":        wx_data['main']['temp'],
        "ow_feels_like":  wx_data['main']['feels_like'],
        "ow_humidity":    wx_data['main']['humidity'],
        "ow_pressure":    wx_data['main']['pressure'],
        "ow_wind_speed":  wx_data['wind']['speed'],
        "ow_wind_deg":    wx_data['wind']['deg'],
        "ow_clouds":      wx_data['clouds']['all'],
        "ow_weather":     wx_data['weather'][0]['description'],
    }


def ingest_all() -> pd.DataFrame:
    rows = []
    bkk_tz = pytz.timezone("Asia/Bangkok")
    created_at = datetime.now(bkk_tz).strftime('%Y-%m-%d %H:%M:%S')
    for station in fetch_air4thai():
        aqi_last = station['AQILast']
        lat = station['lat']
        lon = station['long']

        ow = fetch_openweather(lat, lon)

        rows.append({
            'station_id':   station['stationID'],
            'area_th':      station['areaTH'],
            'area_en':      station['areaEN'],
            'station_type': station['stationType'],
            'lat':          float(lat),
            'lon':          float(lon),
            'recorded_at':  f"{aqi_last['date']} {aqi_last['time']}",
            'aqi':          aqi_last['AQI']['aqi'],
            'aqi_param':    aqi_last['AQI']['param'],
            'pm25_value':   aqi_last['PM25']['value'],
            'pm25_aqi':     aqi_last['PM25']['aqi'],
            'pm10_value':   aqi_last['PM10']['value'],
            'pm10_aqi':     aqi_last['PM10']['aqi'],
            'o3_value':     aqi_last['O3']['value'],
            'o3_aqi':       aqi_last['O3']['aqi'],
            'co_value':     aqi_last['CO']['value'],
            'co_aqi':       aqi_last['CO']['aqi'],
            'no2_value':    aqi_last['NO2']['value'],
            'no2_aqi':      aqi_last['NO2']['aqi'],
            'so2_value':    aqi_last['SO2']['value'],
            'so2_aqi':      aqi_last['SO2']['aqi'],
            **ow,
            'created_at': created_at,
        })

    return pd.DataFrame(rows)


def push_to_supabase(df: pd.DataFrame, table: str = "air_stations"):
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    records = df.to_dict(orient='records')
    client.table(table).insert(records).execute()
    print(f"Pushed {len(records)} rows to '{table}'")
