-- =====================================================================
-- Data-quality checks for the air-station and earthquake pipelines.
--
-- READ-ONLY. Every statement is a select. Nothing here writes to the database.
-- Each statement returns rows ONLY when it finds a violation, and every row has
-- the same 4 columns: check_id, severity, ref (table or object), detail.
--
-- How to run:
--   psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f dags/sqlscript/dq-checks.sql
--   or paste one block at a time into the Supabase SQL editor. The editor only
--   shows the result of the last statement, so run the blocks one by one there.
--
-- Requires safe_timestamp(text) from dags/sqlscript/build-dim-fact.sql.
-- Severity:
--   error = must return 0 rows. A row is a real bug.
--   warn  = known or upstream issue to review. It is not a hard failure.
-- Known, currently expected results are noted inline next to each check.
-- =====================================================================

-- DQ01 [error] air_stations rows with null or empty station_id or recorded_at (proves rule A11)
select 'DQ01'::text as check_id, 'error'::text as severity, 'air_stations'::text as ref,
       concat_ws(', ', 'station_id=' || coalesce(station_id::text, 'NULL'),
                       'recorded_at=' || coalesce(recorded_at::text, 'NULL')) as detail
from air_stations
where station_id is null or station_id::text = ''
   or recorded_at is null or recorded_at::text = ''
limit 100;

-- DQ02 [error] air_stations duplicated (station_id, recorded_at) pairs (proves rule A11)
select 'DQ02'::text, 'error'::text, 'air_stations'::text,
       concat_ws(', ', 'station_id=' || station_id::text,
                       'recorded_at=' || recorded_at::text,
                       'rows=' || count(*)::text)
from air_stations
group by station_id, recorded_at
having count(*) > 1
limit 100;

-- DQ03 [warn] air_stations recorded_at that safe_timestamp cannot parse; PL2 drops these rows (proves rules A7, S5, S6)
select 'DQ03'::text, 'warn'::text, 'air_stations'::text,
       concat_ws(', ', 'station_id=' || coalesce(station_id::text, 'NULL'),
                       'recorded_at=' || coalesce(recorded_at::text, 'NULL'))
from air_stations
where safe_timestamp(recorded_at::text) is null
limit 100;

-- DQ04 [error] air_stations lat/lon missing or outside the valid coordinate range (proves rule A3)
select 'DQ04'::text, 'error'::text, 'air_stations'::text,
       concat_ws(', ', 'station_id=' || coalesce(station_id::text, 'NULL'),
                       'lat=' || coalesce(lat::text, 'NULL'),
                       'lon=' || coalesce(lon::text, 'NULL'))
from air_stations
where lat is null or lon is null
   or lat::float not between -90 and 90
   or lon::float not between -180 and 180
limit 100;

-- DQ05 [warn] dim_station outside the Thailand bounding box, lat 5..21 and lon 97..106 (proves rule S7)
select 'DQ05'::text, 'warn'::text, 'dim_station'::text,
       concat_ws(', ', 'station_id=' || station_id,
                       'lat=' || coalesce(lat::text, 'NULL'),
                       'lon=' || coalesce(lon::text, 'NULL'))
from dim_station
where lat not between 5 and 21
   or lon not between 97 and 106
limit 100;

-- DQ06 [error] dim_station with null, empty or duplicated station_id (proves rule S7)
select 'DQ06'::text, 'error'::text, 'dim_station'::text,
       concat_ws(', ', 'station_key=' || coalesce(station_key::text, 'NULL'),
                       'station_id=' || coalesce(station_id, 'NULL'))
from dim_station
where station_id is null or station_id = ''
   or station_id in (select station_id from dim_station group by station_id having count(*) > 1)
limit 100;

-- DQ07 [error] dim_date fields that disagree with full_date (proves rule S8)
select 'DQ07'::text, 'error'::text, 'dim_date'::text,
       concat_ws(', ', 'date_key=' || date_key::text,
                       'full_date=' || full_date::text)
from dim_date
where date_key <> to_char(full_date, 'YYYYMMDD')::int
   or day_of_week <> extract(isodow from full_date)::int
   or is_weekend <> (extract(isodow from full_date) in (6, 7))
   or year <> extract(year from full_date)::int
   or month <> extract(month from full_date)::int
   or day <> extract(day from full_date)::int
limit 100;

-- DQ08 [error] fact_air_quality rows with a null key, station_id or timestamp (proves rule S9)
select 'DQ08'::text, 'error'::text, 'fact_air_quality'::text,
       concat_ws(', ', 'fact_key=' || coalesce(fact_key::text, 'NULL'),
                       'station_id=' || coalesce(station_id, 'NULL'),
                       'recorded_at=' || coalesce(recorded_at::text, 'NULL'))
from fact_air_quality
where station_key is null or date_key is null
   or station_id is null or recorded_at is null
limit 100;

-- DQ09 [error] fact_air_quality duplicated (station_id, recorded_at) (proves rule S9)
select 'DQ09'::text, 'error'::text, 'fact_air_quality'::text,
       concat_ws(', ', 'station_id=' || station_id,
                       'recorded_at=' || recorded_at::text,
                       'rows=' || count(*)::text)
from fact_air_quality
group by station_id, recorded_at
having count(*) > 1
limit 100;

-- DQ10 [error] fact rows that are orphaned or point at the wrong dim_station row (proves rule S9)
select 'DQ10'::text, 'error'::text, 'fact_air_quality'::text,
       concat_ws(', ', 'fact_key=' || f.fact_key::text,
                       'station_key=' || coalesce(f.station_key::text, 'NULL'),
                       'fact.station_id=' || coalesce(f.station_id, 'NULL'),
                       'dim.station_id=' || coalesce(ds.station_id, 'NULL'))
from fact_air_quality f
left join dim_station ds on ds.station_key = f.station_key
where ds.station_key is null or ds.station_id <> f.station_id
limit 100;

-- DQ11 [error] fact rows whose date_key has no dim_date row, or disagrees with recorded_at (proves rules S8, S9)
select 'DQ11'::text, 'error'::text, 'fact_air_quality'::text,
       concat_ws(', ', 'fact_key=' || f.fact_key::text,
                       'date_key=' || coalesce(f.date_key::text, 'NULL'),
                       'recorded_at=' || coalesce(f.recorded_at::text, 'NULL'))
from fact_air_quality f
left join dim_date dd on dd.date_key = f.date_key
where dd.date_key is null
   or f.date_key <> to_char(f.recorded_at, 'YYYYMMDD')::int
limit 100;

-- DQ12 [error] fact AQI columns outside 0..500. NULLs pass. Known: station_33t aqi=501 rows until they are backfilled (proves rules S1, A9)
select 'DQ12'::text, 'error'::text, 'fact_air_quality'::text,
       concat_ws(', ', 'fact_key=' || fact_key::text,
                       case when aqi not between 0 and 500 then 'aqi=' || aqi::text end,
                       case when pm25_aqi not between 0 and 500 then 'pm25_aqi=' || pm25_aqi::text end,
                       case when pm10_aqi not between 0 and 500 then 'pm10_aqi=' || pm10_aqi::text end,
                       case when o3_aqi not between 0 and 500 then 'o3_aqi=' || o3_aqi::text end,
                       case when co_aqi not between 0 and 500 then 'co_aqi=' || co_aqi::text end,
                       case when no2_aqi not between 0 and 500 then 'no2_aqi=' || no2_aqi::text end,
                       case when so2_aqi not between 0 and 500 then 'so2_aqi=' || so2_aqi::text end,
                       case when ow_aqi not between 0 and 500 then 'ow_aqi=' || ow_aqi::text end)
from fact_air_quality
where aqi not between 0 and 500
   or pm25_aqi not between 0 and 500
   or pm10_aqi not between 0 and 500
   or o3_aqi not between 0 and 500
   or co_aqi not between 0 and 500
   or no2_aqi not between 0 and 500
   or so2_aqi not between 0 and 500
   or ow_aqi not between 0 and 500
limit 100;

-- DQ13 [error] fact pollutant concentrations outside range: pm25/pm10/o3/co/no2 0..900, so2 0..600 (proves rule S1)
select 'DQ13'::text, 'error'::text, 'fact_air_quality'::text,
       concat_ws(', ', 'fact_key=' || fact_key::text,
                       case when pm25_value not between 0 and 900 then 'pm25_value=' || pm25_value::text end,
                       case when pm10_value not between 0 and 900 then 'pm10_value=' || pm10_value::text end,
                       case when o3_value not between 0 and 900 then 'o3_value=' || o3_value::text end,
                       case when co_value not between 0 and 900 then 'co_value=' || co_value::text end,
                       case when no2_value not between 0 and 900 then 'no2_value=' || no2_value::text end,
                       case when so2_value not between 0 and 600 then 'so2_value=' || so2_value::text end)
from fact_air_quality
where pm25_value not between 0 and 900
   or pm10_value not between 0 and 900
   or o3_value not between 0 and 900
   or co_value not between 0 and 900
   or no2_value not between 0 and 900
   or so2_value not between 0 and 600
limit 100;

-- DQ14 [warn] OpenWeather fields outside plausible range or negative. Not enforced by the pipeline (proves rules A10, S2)
select 'DQ14'::text, 'warn'::text, 'fact_air_quality'::text,
       concat_ws(', ', 'fact_key=' || fact_key::text,
                       'ow_humidity=' || coalesce(ow_humidity::text, 'NULL'),
                       'ow_clouds=' || coalesce(ow_clouds::text, 'NULL'),
                       'ow_wind_deg=' || coalesce(ow_wind_deg::text, 'NULL'),
                       'ow_wind_speed=' || coalesce(ow_wind_speed::text, 'NULL'))
from fact_air_quality
where ow_humidity not between 0 and 100
   or ow_clouds not between 0 and 100
   or ow_wind_deg not between 0 and 360
   or ow_wind_speed < 0
   or ow_no < 0 or ow_no2 < 0 or ow_o3 < 0 or ow_so2 < 0
   or ow_pm25 < 0 or ow_pm10 < 0 or ow_nh3 < 0
limit 100;

-- DQ15 [warn] timestamps in the future or created_at before recorded_at, which suggests a timezone mix-up (proves rules A7, A8, S10)
select 'DQ15'::text, 'warn'::text, 'fact_air_quality'::text,
       concat_ws(', ', 'fact_key=' || fact_key::text,
                       'recorded_at=' || recorded_at::text,
                       'created_at=' || coalesce(created_at::text, 'NULL'))
from fact_air_quality
where recorded_at > (now() at time zone 'Asia/Bangkok') + interval '1 hour'
   or created_at < recorded_at - interval '1 hour'
limit 100;

-- DQ16 [warn] station_* table with no matching dim_station row. Known: o24, o29 and o67 are expected here (proves rules S4, S9)
select 'DQ16'::text, 'warn'::text, 'pg_tables'::text,
       t.tablename::text
from pg_tables t
where t.schemaname = 'public'
  and t.tablename like 'station\_%'
  and not exists (
    select 1 from dim_station ds
    where 'station_' || regexp_replace(ds.station_id, '[^a-zA-Z0-9]', '_', 'g') = t.tablename
  )
limit 100;

-- DQ17 [warn] fact_air_quality is more than 3 hours behind air_stations (proves rule S11)
select 'DQ17'::text, 'warn'::text, 'fact_air_quality'::text,
       'max_fact_created_at=' || coalesce((select max(created_at)::text from fact_air_quality), 'NULL')
       || ', max_raw_created_at=' || coalesce((select max(safe_timestamp(created_at::text))::text from air_stations), 'NULL')
where (select max(created_at) from fact_air_quality)
      < (select max(safe_timestamp(created_at::text)) from air_stations) - interval '3 hours'
limit 100;

-- DQ18 [error] earthquake_events with a null or non-standard datetime_utc or datetime_thai (proves rules E3, E10)
select 'DQ18'::text, 'error'::text, 'earthquake_events'::text,
       concat_ws(', ', 'id=' || coalesce(id::text, 'NULL'),
                       'datetime_utc=' || coalesce(datetime_utc, 'NULL'),
                       'datetime_thai=' || coalesce(datetime_thai, 'NULL'))
from earthquake_events
where datetime_utc is null or datetime_thai is null
   or datetime_utc !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
   or datetime_thai !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
limit 100;

-- DQ19 [error] earthquake_events duplicated on (datetime_utc, lat, lon) (proves rule E9)
select 'DQ19'::text, 'error'::text, 'earthquake_events'::text,
       concat_ws(', ', 'datetime_utc=' || datetime_utc,
                       'lat=' || coalesce(lat::text, 'NULL'),
                       'lon=' || coalesce(lon::text, 'NULL'),
                       'rows=' || count(*)::text)
from earthquake_events
group by datetime_utc, lat, lon
having count(*) > 1
limit 100;

-- DQ20 [error] earthquake_events outside magnitude 0..10, depth 0..800, or global lat/lon (proves rules E4, E5, E12)
select 'DQ20'::text, 'error'::text, 'earthquake_events'::text,
       concat_ws(', ', 'id=' || coalesce(id::text, 'NULL'),
                       'magnitude=' || coalesce(magnitude::text, 'NULL'),
                       'depth_km=' || coalesce(depth_km::text, 'NULL'),
                       'lat=' || coalesce(lat::text, 'NULL'),
                       'lon=' || coalesce(lon::text, 'NULL'))
from earthquake_events
where magnitude not between 0 and 10
   or depth_km not between 0 and 800
   or lat not between -90 and 90
   or lon not between -180 and 180
limit 100;

-- DQ21 [warn] datetime_thai is not exactly datetime_utc + 7 hours (proves rule E12, timezone)
select 'DQ21'::text, 'warn'::text, 'earthquake_events'::text,
       concat_ws(', ', 'id=' || coalesce(id::text, 'NULL'),
                       'datetime_utc=' || coalesce(datetime_utc, 'NULL'),
                       'datetime_thai=' || coalesce(datetime_thai, 'NULL'))
from earthquake_events
where safe_timestamp(datetime_thai) - safe_timestamp(datetime_utc) <> interval '7 hours'
limit 100;

-- DQ22 [error] is_domestic disagrees with whether province_th is set (proves rule E7)
select 'DQ22'::text, 'error'::text, 'earthquake_events'::text,
       concat_ws(', ', 'id=' || coalesce(id::text, 'NULL'),
                       'is_domestic=' || coalesce(is_domestic::text, 'NULL'),
                       'province_th=' || coalesce(province_th, 'NULL'))
from earthquake_events
where is_domestic is distinct from (province_th is not null)
limit 100;

-- DQ23 [warn] raw reports with a non-standard fetched_at, or no DailyEarthquakes array in the payload (proves rule E2)
select 'DQ23'::text, 'warn'::text, 'earthquake_reports_raw'::text,
       concat_ws(', ', 'id=' || coalesce(id::text, 'NULL'),
                       'fetched_at=' || coalesce(fetched_at, 'NULL'),
                       'payload_type=' || coalesce(jsonb_typeof(payload -> 'DailyEarthquakes'), 'missing'))
from earthquake_reports_raw
where fetched_at !~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
   or jsonb_typeof(payload -> 'DailyEarthquakes') is distinct from 'array'
limit 100;
