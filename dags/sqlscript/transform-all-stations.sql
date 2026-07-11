-- transform_all_stations: kept for manual/ad-hoc use in the SQL editor
-- (e.g. "transform everything, right now, I don't care about timeouts").
--
-- IMPORTANT: the Airflow PL1 DAG no longer calls this function. Calling it
-- still runs every station inside ONE statement/transaction, so it is still
-- subject to the 8-9s statement_timeout once station/data volume grows.
-- The production path is now: PL1 DAG -> get_distinct_stations() -> one
-- transform_station() call per station (dynamic task mapping), which is what
-- actually removes the timeout risk. This wrapper just loops over
-- transform_station() per station so the two never drift out of sync.

drop function if exists transform_all_stations(text);
drop function if exists transform_all_stations(text, text, text, text, text);

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
  v_count  int := 0;
begin
  for r in
    select station_id from get_distinct_stations(p_mode, p_date, p_date_from, p_date_to, p_station_id)
  loop
    perform transform_station(r.station_id, p_mode, p_date, p_date_from, p_date_to);
    v_count := v_count + 1;
  end loop;

  return v_count;
end;
$$;

grant execute on function transform_all_stations(text, text, text, text, text) to anon, authenticated, service_role;
