create table if not exists earthquake_events (
  id bigint generated always as identity primary key,
  datetime_utc text not null,
  datetime_thai text not null,
  magnitude numeric,
  depth_km int,
  lat numeric,
  lon numeric,
  title_th text,
  tambon_th text,
  amphoe_th text,
  province_th text,
  location_en text,
  is_domestic boolean not null default false,
  created_at timestamptz not null default now(),
  unique (datetime_utc, lat, lon)
);

create index if not exists idx_earthquake_events_datetime_utc
  on earthquake_events (datetime_utc desc);

create index if not exists idx_earthquake_events_province_th
  on earthquake_events (province_th);
