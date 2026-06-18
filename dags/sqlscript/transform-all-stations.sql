-- ===========================================================================
-- Server-side batch transform: process EVERY station in one call.
-- Run this ONCE in the Supabase SQL Editor (same project as air_stations).
--
-- Replaces the old "one exec_sql round-trip per station" pattern: instead of
-- ~178 network calls (which ran sequentially under Airflow's SequentialExecutor),
-- Airflow now makes a SINGLE call to this function, and the per-station loop runs
-- inside Postgres (DB-local, no network per station).
--
-- Mirrors air-station-transform.sql (schema + validation/SPLIT_PART transform),
-- but loops over all stations in the target window. This function is the
-- source of truth for the regular run AND for reloads.
--
-- RELOAD MODES (p_mode):
--   'latest' (default) → the most recent ingestion snapshot (created_at = MAX) — normal run
--   'day'              → all readings recorded on p_date          (recorded_at::date = p_date)
--   'range'            → all readings between p_date_from..p_date_to (recorded_at::date)
--   'full'             → every row (full dump)
-- Optionally scope to ONE station with p_station_id (else every station).
-- ===========================================================================

-- Drop the previous single-arg signature so PostgREST doesn't see two overloads.
drop function if exists transform_all_stations(text);

create or replace function transform_all_stations(
  p_mode       text default 'latest',
  p_date       text default null,
  p_date_from  text default null,
  p_date_to    text default null,
  p_station_id text default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  r        record;
  v_filter text;
  v_scope  text := '';
  v_tbl    text;
  v_count  int := 0;
begin
  -- Which rows to (re)process. Every literal is escaped with %L → no injection.
  if p_mode = 'day' and p_date is not null and p_date <> '' then
    v_filter := format('recorded_at::date = %L', p_date);
  elsif p_mode = 'range' and p_date_from is not null and p_date_from <> ''
                         and p_date_to   is not null and p_date_to   <> '' then
    v_filter := format('recorded_at::date BETWEEN %L AND %L', p_date_from, p_date_to);
  elsif p_mode = 'full' then
    v_filter := 'TRUE';
  else
    -- 'latest' (default): the newest ingestion snapshot.
    v_filter := 'created_at = (SELECT MAX(created_at) FROM air_stations)';
  end if;

  -- Optionally restrict to a single station.
  if p_station_id is not null and p_station_id <> '' then
    v_scope := format(' AND station_id = %L', p_station_id);
  end if;

  -- Only stations that actually have rows in this window (skips dead ids).
  for r in execute
    format('SELECT DISTINCT station_id FROM air_stations WHERE %s%s', v_filter, v_scope)
  loop
    v_tbl := 'station_' || regexp_replace(r.station_id, '[^a-zA-Z0-9]', '_', 'g');

    -- Create the per-station table if it doesn't exist yet.
    execute format($f$
      CREATE TABLE IF NOT EXISTS %I (
        id            BIGINT,
        station_id    TEXT,
        area_th       TEXT,
        area_en       TEXT,
        location      TEXT,
        station_type  TEXT,
        lat           FLOAT,
        lon           FLOAT,
        pm25_value    NUMERIC, pm25_aqi NUMERIC,
        pm10_value    NUMERIC, pm10_aqi NUMERIC,
        o3_value      NUMERIC, o3_aqi   NUMERIC,
        co_value      NUMERIC, co_aqi   NUMERIC,
        no2_value     NUMERIC, no2_aqi  NUMERIC,
        so2_value     NUMERIC, so2_aqi  NUMERIC,
        aqi           NUMERIC, aqi_param TEXT,
        ow_aqi        NUMERIC, ow_no NUMERIC, ow_no2 NUMERIC, ow_o3 NUMERIC,
        ow_so2        NUMERIC, ow_pm25 NUMERIC, ow_pm10 NUMERIC, ow_nh3 NUMERIC,
        ow_temp       NUMERIC, ow_feels_like NUMERIC, ow_humidity NUMERIC,
        ow_pressure   NUMERIC, ow_wind_speed NUMERIC, ow_wind_deg NUMERIC,
        ow_clouds     NUMERIC, ow_weather TEXT,
        recorded_at   TEXT,
        created_at    TEXT,
        UNIQUE (station_id, recorded_at)
      )
    $f$, v_tbl);

    -- Backfill the overall-AQI columns onto tables created before they existed.
    execute format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS aqi NUMERIC',     v_tbl);
    execute format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS aqi_param TEXT',  v_tbl);

    -- Insert this station's validated rows for the chosen snapshot.
    execute format($f$
      INSERT INTO %I (
        id, station_id, area_th, area_en, location, station_type, lat, lon,
        pm25_value, pm25_aqi, pm10_value, pm10_aqi,
        o3_value, o3_aqi, co_value, co_aqi,
        no2_value, no2_aqi, so2_value, so2_aqi,
        aqi, aqi_param,
        ow_aqi, ow_no, ow_no2, ow_o3, ow_so2, ow_pm25, ow_pm10, ow_nh3,
        ow_temp, ow_feels_like, ow_humidity, ow_pressure,
        ow_wind_speed, ow_wind_deg, ow_clouds, ow_weather,
        recorded_at, created_at
      )
      SELECT
        id, station_id, area_th, area_en,
        TRIM(SPLIT_PART(area_en, ',', -1)) AS location,
        station_type, lat, lon,
        CASE WHEN pm25_value::numeric NOT BETWEEN 0 AND 900 THEN 0 ELSE pm25_value::numeric END,
        CASE WHEN pm25_aqi::numeric   NOT BETWEEN 0 AND 300 THEN 0 ELSE pm25_aqi::numeric  END,
        CASE WHEN pm10_value::numeric NOT BETWEEN 0 AND 900 THEN 0 ELSE pm10_value::numeric END,
        CASE WHEN pm10_aqi::numeric   NOT BETWEEN 0 AND 300 THEN 0 ELSE pm10_aqi::numeric  END,
        CASE WHEN o3_value::numeric   NOT BETWEEN 0 AND 900 THEN 0 ELSE o3_value::numeric  END,
        CASE WHEN o3_aqi::numeric     NOT BETWEEN 0 AND 300 THEN 0 ELSE o3_aqi::numeric    END,
        CASE WHEN co_value::numeric   NOT BETWEEN 0 AND 900 THEN 0 ELSE co_value::numeric  END,
        CASE WHEN co_aqi::numeric     NOT BETWEEN 0 AND 300 THEN 0 ELSE co_aqi::numeric    END,
        CASE WHEN no2_value::numeric  NOT BETWEEN 0 AND 900 THEN 0 ELSE no2_value::numeric END,
        CASE WHEN no2_aqi::numeric    NOT BETWEEN 0 AND 300 THEN 0 ELSE no2_aqi::numeric   END,
        CASE WHEN so2_value::numeric  NOT BETWEEN 0 AND 600 THEN 0 ELSE so2_value::numeric END,
        CASE WHEN so2_aqi::numeric    NOT BETWEEN 0 AND 300 THEN 0 ELSE so2_aqi::numeric   END,
        -- Overall AQI: keep the real index (0–500+); air4thai uses -1 for "no reading" → NULL.
        CASE WHEN aqi::numeric < 0 THEN NULL ELSE aqi::numeric END,
        aqi_param,
        ow_aqi, ow_no, ow_no2, ow_o3, ow_so2, ow_pm25, ow_pm10, ow_nh3,
        ow_temp, ow_feels_like, ow_humidity, ow_pressure,
        ow_wind_speed, ow_wind_deg, ow_clouds, ow_weather,
        recorded_at, created_at
      FROM air_stations
      WHERE station_id = %L AND %s
      -- Overwrite on conflict so a reload actually re-cleans existing rows.
      ON CONFLICT (station_id, recorded_at) DO UPDATE SET
        id = EXCLUDED.id, area_th = EXCLUDED.area_th, area_en = EXCLUDED.area_en,
        location = EXCLUDED.location, station_type = EXCLUDED.station_type,
        lat = EXCLUDED.lat, lon = EXCLUDED.lon,
        pm25_value = EXCLUDED.pm25_value, pm25_aqi = EXCLUDED.pm25_aqi,
        pm10_value = EXCLUDED.pm10_value, pm10_aqi = EXCLUDED.pm10_aqi,
        o3_value = EXCLUDED.o3_value, o3_aqi = EXCLUDED.o3_aqi,
        co_value = EXCLUDED.co_value, co_aqi = EXCLUDED.co_aqi,
        no2_value = EXCLUDED.no2_value, no2_aqi = EXCLUDED.no2_aqi,
        so2_value = EXCLUDED.so2_value, so2_aqi = EXCLUDED.so2_aqi,
        aqi = EXCLUDED.aqi, aqi_param = EXCLUDED.aqi_param,
        ow_aqi = EXCLUDED.ow_aqi, ow_no = EXCLUDED.ow_no, ow_no2 = EXCLUDED.ow_no2,
        ow_o3 = EXCLUDED.ow_o3, ow_so2 = EXCLUDED.ow_so2, ow_pm25 = EXCLUDED.ow_pm25,
        ow_pm10 = EXCLUDED.ow_pm10, ow_nh3 = EXCLUDED.ow_nh3, ow_temp = EXCLUDED.ow_temp,
        ow_feels_like = EXCLUDED.ow_feels_like, ow_humidity = EXCLUDED.ow_humidity,
        ow_pressure = EXCLUDED.ow_pressure, ow_wind_speed = EXCLUDED.ow_wind_speed,
        ow_wind_deg = EXCLUDED.ow_wind_deg, ow_clouds = EXCLUDED.ow_clouds,
        ow_weather = EXCLUDED.ow_weather, created_at = EXCLUDED.created_at
    $f$, v_tbl, r.station_id, v_filter);

    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;

grant execute on function transform_all_stations(text, text, text, text, text) to anon, authenticated, service_role;
