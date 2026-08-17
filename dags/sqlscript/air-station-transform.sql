-- Casts raw TEXT sensor readings to NUMERIC without ever raising: a
-- non-numeric value (empty string, 'N/A', sensor placeholder) or a value
-- outside [lo, hi] both come back NULL instead of aborting the INSERT
-- this is used in.
create or replace function safe_numeric(v text, lo numeric, hi numeric)
returns numeric
language plpgsql
immutable
as $$
begin
  if v is null or v !~ '^-?\d+(\.\d+)?$' then
    return null;
  end if;
  if v::numeric not between lo and hi then
    return null;
  end if;
  return v::numeric;
end;
$$;

grant execute on function safe_numeric(text, numeric, numeric) to anon, authenticated, service_role;


drop function if exists transform_station(text, text, date, date);

create or replace function transform_station(
  p_station_id text,
  p_mode       text default 'latest',
  p_date       text default null,
  p_date_from  text default null,
  p_date_to    text default null
)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare
  v_filter text;
  v_tbl    text;
  v_count  int;
begin
  if p_mode = 'day' and p_date is not null and p_date <> '' then
    v_filter := format('recorded_at::date = %L', p_date);
  elsif p_mode = 'range' and p_date_from is not null and p_date_from <> ''
                         and p_date_to   is not null and p_date_to   <> '' then
    v_filter := format('recorded_at::date BETWEEN %L AND %L', p_date_from, p_date_to);
  elsif p_mode = 'full' then
    v_filter := 'TRUE';
  else
    v_filter := 'created_at = (SELECT MAX(created_at) FROM air_stations WHERE station_id = ' || quote_literal(p_station_id) || ')';
  end if;

  v_tbl := 'station_' || regexp_replace(p_station_id, '[^a-zA-Z0-9]', '_', 'g');

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
      ow_aqi        NUMERIC, ow_co NUMERIC, ow_no NUMERIC, ow_no2 NUMERIC, ow_o3 NUMERIC,
      ow_so2        NUMERIC, ow_pm25 NUMERIC, ow_pm10 NUMERIC, ow_nh3 NUMERIC,
      ow_temp       NUMERIC, ow_feels_like NUMERIC, ow_humidity NUMERIC,
      ow_pressure   NUMERIC, ow_wind_speed NUMERIC, ow_wind_deg NUMERIC,
      ow_clouds     NUMERIC, ow_weather TEXT,
      recorded_at   TEXT,
      created_at    TEXT,
      UNIQUE (station_id, recorded_at)
    )
  $f$, v_tbl);

  execute format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS aqi NUMERIC',    v_tbl);
  execute format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS aqi_param TEXT', v_tbl);
  execute format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS ow_co NUMERIC',  v_tbl);

  execute format($f$
    INSERT INTO %I (
      id, station_id, area_th, area_en, location, station_type, lat, lon,
      pm25_value, pm25_aqi, pm10_value, pm10_aqi,
      o3_value, o3_aqi, co_value, co_aqi,
      no2_value, no2_aqi, so2_value, so2_aqi,
      aqi, aqi_param,
      ow_aqi, ow_co, ow_no, ow_no2, ow_o3, ow_so2, ow_pm25, ow_pm10, ow_nh3,
      ow_temp, ow_feels_like, ow_humidity, ow_pressure,
      ow_wind_speed, ow_wind_deg, ow_clouds, ow_weather,
      recorded_at, created_at
    )
    SELECT
      id, station_id, area_th, area_en,
      TRIM(SPLIT_PART(area_en, ',', -1)) AS location,
      station_type, lat, lon,
      safe_numeric(pm25_value::text, 0, 900),
      safe_numeric(pm25_aqi::text,   0, 500),
      safe_numeric(pm10_value::text, 0, 900),
      safe_numeric(pm10_aqi::text,   0, 500),
      safe_numeric(o3_value::text,   0, 900),
      safe_numeric(o3_aqi::text,     0, 500),
      safe_numeric(co_value::text,   0, 900),
      safe_numeric(co_aqi::text,     0, 500),
      safe_numeric(no2_value::text,  0, 900),
      safe_numeric(no2_aqi::text,    0, 500),
      safe_numeric(so2_value::text,  0, 600),
      safe_numeric(so2_aqi::text,    0, 500),
      safe_numeric(aqi::text,        0, 500),
      aqi_param,
      ow_aqi, ow_co, ow_no, ow_no2, ow_o3, ow_so2, ow_pm25, ow_pm10, ow_nh3,
      ow_temp, ow_feels_like, ow_humidity, ow_pressure,
      ow_wind_speed, ow_wind_deg, ow_clouds, ow_weather,
      recorded_at, created_at
    FROM air_stations
    WHERE station_id = %L AND %s
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
      ow_aqi = EXCLUDED.ow_aqi, ow_co = EXCLUDED.ow_co, ow_no = EXCLUDED.ow_no, ow_no2 = EXCLUDED.ow_no2,
      ow_o3 = EXCLUDED.ow_o3, ow_so2 = EXCLUDED.ow_so2, ow_pm25 = EXCLUDED.ow_pm25,
      ow_pm10 = EXCLUDED.ow_pm10, ow_nh3 = EXCLUDED.ow_nh3, ow_temp = EXCLUDED.ow_temp,
      ow_feels_like = EXCLUDED.ow_feels_like, ow_humidity = EXCLUDED.ow_humidity,
      ow_pressure = EXCLUDED.ow_pressure, ow_wind_speed = EXCLUDED.ow_wind_speed,
      ow_wind_deg = EXCLUDED.ow_wind_deg, ow_clouds = EXCLUDED.ow_clouds,
      ow_weather = EXCLUDED.ow_weather, created_at = EXCLUDED.created_at
  $f$, v_tbl, p_station_id, v_filter);

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

grant execute on function transform_station(text, text, text, text, text) to anon, authenticated, service_role;


drop function if exists get_distinct_stations();
drop function if exists get_distinct_stations(text, text, text, text, text);

create or replace function get_distinct_stations(
  p_mode       text default 'latest',
  p_date       text default null,
  p_date_from  text default null,
  p_date_to    text default null,
  p_station_id text default null
)
returns table(station_id text)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_filter text;
  v_scope  text := '';
begin
  if p_mode = 'day' and p_date is not null and p_date <> '' then
    v_filter := format('recorded_at::date = %L', p_date);
  elsif p_mode = 'range' and p_date_from is not null and p_date_from <> ''
                         and p_date_to   is not null and p_date_to   <> '' then
    v_filter := format('recorded_at::date BETWEEN %L AND %L', p_date_from, p_date_to);
  elsif p_mode = 'full' then
    v_filter := 'TRUE';
  else
    v_filter := 'created_at = (SELECT MAX(created_at) FROM air_stations)';
  end if;

  if p_station_id is not null and p_station_id <> '' then
    v_scope := format(' AND station_id = %L', p_station_id);
  end if;

  return query execute
    format('SELECT DISTINCT air_stations.station_id FROM air_stations WHERE %s%s ORDER BY 1', v_filter, v_scope);
end;
$$;

grant execute on function get_distinct_stations(text, text, text, text, text) to anon, authenticated, service_role;
