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
-- but loops over all stations in the target snapshot. This function is the
-- source of truth for the regular run; air-station-transform.sql +
-- transform_station() are kept for ad-hoc / date-range backfills.
-- ===========================================================================

create or replace function transform_all_stations(p_snapshot text default 'latest')
returns int
language plpgsql
as $$
declare
  r        record;
  v_filter text;
  v_tbl    text;
  v_count  int := 0;
begin
  -- Which snapshot to transform.
  if p_snapshot is null or p_snapshot = 'latest' then
    v_filter := 'created_at = (SELECT MAX(created_at) FROM air_stations)';
  else
    v_filter := format('created_at = %L', p_snapshot);
  end if;

  -- Only stations that actually have rows in this snapshot (skips dead ids).
  for r in execute
    format('SELECT DISTINCT station_id FROM air_stations WHERE %s', v_filter)
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

    -- Insert this station's validated rows for the chosen snapshot.
    execute format($f$
      INSERT INTO %I (
        id, station_id, area_th, area_en, location, station_type, lat, lon,
        pm25_value, pm25_aqi, pm10_value, pm10_aqi,
        o3_value, o3_aqi, co_value, co_aqi,
        no2_value, no2_aqi, so2_value, so2_aqi,
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
        ow_aqi, ow_no, ow_no2, ow_o3, ow_so2, ow_pm25, ow_pm10, ow_nh3,
        ow_temp, ow_feels_like, ow_humidity, ow_pressure,
        ow_wind_speed, ow_wind_deg, ow_clouds, ow_weather,
        recorded_at, created_at
      FROM air_stations
      WHERE station_id = %L AND %s
      ON CONFLICT (station_id, recorded_at) DO NOTHING
    $f$, v_tbl, r.station_id, v_filter);

    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;

grant execute on function transform_all_stations(text) to anon, authenticated, service_role;
