create table if not exists earthquake_reports_raw (
  id bigint generated always as identity primary key,
  fetched_at text not null,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_earthquake_reports_raw_fetched_at
  on earthquake_reports_raw (fetched_at desc);
