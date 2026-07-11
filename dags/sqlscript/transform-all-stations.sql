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
