-- ===========================================================================
-- PL2 — Build the star-schema mart (dim_station + dim_date + fact_air_quality).
-- Run this ONCE in the Supabase SQL Editor (same project as air_stations).
--
-- Source = the per-station `station_*` tables produced by PL1 (NOT air_stations).
-- This keeps the layering clean:
--     air_stations (raw)  ->  PL1  ->  station_* (cleaned, per station)
--                                       ->  PL2  ->  dim_station / dim_date / fact_air_quality
-- WebResume then reads from this mart, never from the raw bucket.
--
-- The function unions every `station_%` table inside Postgres (one round-trip),
-- mirroring the dynamic-SQL style of transform_all_stations(). It is idempotent:
-- dimensions are upserted (SCD type-1) and facts are keyed on
-- (station_id, recorded_at), so re-runs only add/refresh rows.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- Dimension + fact tables (created up front so the schema is explicit and
-- doesn't depend on a successful first run to exist).
-- ---------------------------------------------------------------------------
create table if not exists dim_station (
  station_key  bigint generated always as identity primary key,
  station_id   text unique not null,          -- natural / business key
  area_th      text,
  area_en      text,
  location     text,
  station_type text,
  lat          float,
  lon          float,
  updated_at   timestamptz default now()
);

create table if not exists dim_date (
  date_key     int  primary key,              -- YYYYMMDD
  full_date    date not null unique,
  year         int,
  quarter      int,
  month        int,
  month_name   text,
  day          int,
  day_of_week  int,                           -- ISO: 1 = Monday .. 7 = Sunday
  day_name     text,
  week_of_year int,                           -- ISO week
  is_weekend   boolean
);

create table if not exists fact_air_quality (
  fact_key     bigint generated always as identity primary key,
  station_key  bigint not null references dim_station(station_key),
  date_key     int    not null references dim_date(date_key),
  station_id   text   not null,               -- degenerate dimension (eases joins/upserts)
  recorded_at  timestamp not null,            -- measurement time (Asia/Bangkok, naive)
  created_at   timestamp,                     -- ingestion snapshot time
  -- Air4Thai measures
  aqi          numeric, aqi_param text,
  pm25_value   numeric, pm25_aqi numeric,
  pm10_value   numeric, pm10_aqi numeric,
  o3_value     numeric, o3_aqi   numeric,
  co_value     numeric, co_aqi   numeric,
  no2_value    numeric, no2_aqi  numeric,
  so2_value    numeric, so2_aqi  numeric,
  -- OpenWeather measures
  ow_aqi       numeric, ow_no numeric, ow_no2 numeric, ow_o3 numeric,
  ow_so2       numeric, ow_pm25 numeric, ow_pm10 numeric, ow_nh3 numeric,
  ow_temp      numeric, ow_feels_like numeric, ow_humidity numeric,
  ow_pressure  numeric, ow_wind_speed numeric, ow_wind_deg numeric,
  ow_clouds    numeric, ow_weather text,
  unique (station_id, recorded_at)
);

create index if not exists idx_fact_station_recorded
  on fact_air_quality (station_id, recorded_at desc);
create index if not exists idx_fact_created
  on fact_air_quality (created_at desc);


-- Drop the previous no-arg signature so PostgREST doesn't see two overloads.
drop function if exists build_dim_fact();

create or replace function build_dim_fact(
  p_mode       text default 'full',
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
-- RELOAD MODES (p_mode), applied to the staged station_* rows:
--   'full' (default) → rebuild the whole mart
--   'latest'         → only the newest snapshot (created_at = MAX) — used by the normal run
--   'day'            → only readings recorded on p_date           (recorded_at::date = p_date)
--   'range'          → only readings between p_date_from..p_date_to (recorded_at::date)
-- Optionally scope to ONE station with p_station_id.
declare
  r       record;
  v_union text := '';
  v_cond  text;          -- mode/station predicate, columns qualified with s.
  v_count int  := 0;
begin
  -- 1. Union every per-station table (the "separated stations" from PL1).
  --    Each table is first self-healed to guarantee the aqi columns exist, so
  --    PL2 works even on station_* tables PL1 hasn't re-touched yet.
  for r in
    select tablename from pg_tables
    where schemaname = 'public' and tablename like 'station_%'
    order by tablename
  loop
    execute format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS aqi NUMERIC',    r.tablename);
    execute format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS aqi_param TEXT', r.tablename);

    if v_union <> '' then
      v_union := v_union || ' UNION ALL ';
    end if;
    v_union := v_union || format($u$
      SELECT station_id, area_th, area_en, location, station_type, lat, lon,
             aqi, aqi_param,
             pm25_value, pm25_aqi, pm10_value, pm10_aqi,
             o3_value, o3_aqi, co_value, co_aqi,
             no2_value, no2_aqi, so2_value, so2_aqi,
             ow_aqi, ow_no, ow_no2, ow_o3, ow_so2, ow_pm25, ow_pm10, ow_nh3,
             ow_temp, ow_feels_like, ow_humidity, ow_pressure,
             ow_wind_speed, ow_wind_deg, ow_clouds, ow_weather,
             recorded_at, created_at
      FROM %I$u$, r.tablename);
  end loop;

  -- No station_* tables yet → nothing to build.
  if v_union = '' then
    return 0;
  end if;

  -- 2. Stage the union once. recorded_at/created_at are TEXT in station_*; cast
  --    to real timestamps here so the mart is properly typed (rows with an
  --    unparseable / null recorded_at are dropped — they can't be placed in time).
  execute 'create temp table _src on commit drop as ' || v_union;

  -- 2b. Build the mode/station predicate (columns qualified with s. so it can be
  --     dropped straight into every statement, incl. the joined fact insert).
  if p_mode = 'day' and p_date is not null and p_date <> '' then
    v_cond := format('s.recorded_at::date = %L', p_date);
  elsif p_mode = 'range' and p_date_from is not null and p_date_from <> ''
                         and p_date_to   is not null and p_date_to   <> '' then
    v_cond := format('s.recorded_at::date BETWEEN %L AND %L', p_date_from, p_date_to);
  elsif p_mode = 'latest' then
    v_cond := 's.created_at = (SELECT MAX(created_at) FROM _src)';
  else
    -- 'full' (default)
    v_cond := 'TRUE';
  end if;
  if p_station_id is not null and p_station_id <> '' then
    v_cond := v_cond || format(' AND s.station_id = %L', p_station_id);
  end if;

  -- 3. dim_station — one row per station, SCD type-1 (latest attributes win).
  execute format($f$
    insert into dim_station (station_id, area_th, area_en, location, station_type, lat, lon)
    select distinct on (s.station_id)
           s.station_id, s.area_th, s.area_en, s.location, s.station_type, s.lat::float, s.lon::float
    from _src s
    where s.recorded_at is not null and s.recorded_at <> '' and (%s)
    order by s.station_id, s.recorded_at::timestamp desc
    on conflict (station_id) do update set
      area_th      = excluded.area_th,
      area_en      = excluded.area_en,
      location     = excluded.location,
      station_type = excluded.station_type,
      lat          = excluded.lat,
      lon          = excluded.lon,
      updated_at   = now()
  $f$, v_cond);

  -- 4. dim_date — one row per calendar day present in the (filtered) data.
  execute format($f$
    insert into dim_date (date_key, full_date, year, quarter, month, month_name,
                          day, day_of_week, day_name, week_of_year, is_weekend)
    select
      (to_char(d, 'YYYYMMDD'))::int,
      d,
      extract(year    from d)::int,
      extract(quarter from d)::int,
      extract(month   from d)::int,
      trim(to_char(d, 'Month')),
      extract(day     from d)::int,
      extract(isodow  from d)::int,
      trim(to_char(d, 'Day')),
      extract(week    from d)::int,
      extract(isodow  from d) in (6, 7)
    from (
      select distinct (s.recorded_at::timestamp)::date as d
      from _src s
      where s.recorded_at is not null and s.recorded_at <> '' and (%s)
    ) x
    on conflict (date_key) do nothing
  $f$, v_cond);

  -- 5. fact_air_quality — one row per (station, recorded_at) measurement.
  execute format($f$
    insert into fact_air_quality (
      station_key, date_key, station_id, recorded_at, created_at,
      aqi, aqi_param,
      pm25_value, pm25_aqi, pm10_value, pm10_aqi,
      o3_value, o3_aqi, co_value, co_aqi,
      no2_value, no2_aqi, so2_value, so2_aqi,
      ow_aqi, ow_no, ow_no2, ow_o3, ow_so2, ow_pm25, ow_pm10, ow_nh3,
      ow_temp, ow_feels_like, ow_humidity, ow_pressure,
      ow_wind_speed, ow_wind_deg, ow_clouds, ow_weather
    )
    select
      ds.station_key,
      (to_char(s.recorded_at::timestamp, 'YYYYMMDD'))::int,
      s.station_id,
      s.recorded_at::timestamp,
      nullif(s.created_at, '')::timestamp,
      s.aqi, s.aqi_param,
      s.pm25_value, s.pm25_aqi, s.pm10_value, s.pm10_aqi,
      s.o3_value, s.o3_aqi, s.co_value, s.co_aqi,
      s.no2_value, s.no2_aqi, s.so2_value, s.so2_aqi,
      s.ow_aqi, s.ow_no, s.ow_no2, s.ow_o3, s.ow_so2, s.ow_pm25, s.ow_pm10, s.ow_nh3,
      s.ow_temp, s.ow_feels_like, s.ow_humidity, s.ow_pressure,
      s.ow_wind_speed, s.ow_wind_deg, s.ow_clouds, s.ow_weather
    from _src s
    join dim_station ds on ds.station_id = s.station_id
    where s.recorded_at is not null and s.recorded_at <> '' and (%s)
    on conflict (station_id, recorded_at) do update set
      date_key      = excluded.date_key,
      created_at    = excluded.created_at,
      aqi           = excluded.aqi,
      aqi_param     = excluded.aqi_param,
      pm25_value    = excluded.pm25_value, pm25_aqi = excluded.pm25_aqi,
      pm10_value    = excluded.pm10_value, pm10_aqi = excluded.pm10_aqi,
      o3_value      = excluded.o3_value,   o3_aqi   = excluded.o3_aqi,
      co_value      = excluded.co_value,   co_aqi   = excluded.co_aqi,
      no2_value     = excluded.no2_value,  no2_aqi  = excluded.no2_aqi,
      so2_value     = excluded.so2_value,  so2_aqi  = excluded.so2_aqi,
      ow_aqi        = excluded.ow_aqi,  ow_no = excluded.ow_no, ow_no2 = excluded.ow_no2,
      ow_o3         = excluded.ow_o3,   ow_so2 = excluded.ow_so2, ow_pm25 = excluded.ow_pm25,
      ow_pm10       = excluded.ow_pm10, ow_nh3 = excluded.ow_nh3, ow_temp = excluded.ow_temp,
      ow_feels_like = excluded.ow_feels_like, ow_humidity = excluded.ow_humidity,
      ow_pressure   = excluded.ow_pressure,   ow_wind_speed = excluded.ow_wind_speed,
      ow_wind_deg   = excluded.ow_wind_deg,    ow_clouds = excluded.ow_clouds,
      ow_weather    = excluded.ow_weather
  $f$, v_cond);

  get diagnostics v_count = row_count;
  return v_count;
end;
$$;

grant execute on function build_dim_fact(text, text, text, text, text) to anon, authenticated, service_role;
