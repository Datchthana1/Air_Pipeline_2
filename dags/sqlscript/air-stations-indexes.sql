create index if not exists idx_air_stations_created_at
  on air_stations (created_at desc);

create index if not exists idx_air_stations_station_created
  on air_stations (station_id, created_at desc);

create index if not exists idx_air_stations_station_recorded
  on air_stations (station_id, recorded_at);

analyze air_stations;
