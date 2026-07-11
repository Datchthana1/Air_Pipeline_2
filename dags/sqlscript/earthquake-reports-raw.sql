-- Raw staging table for PL0_ingestion_earthquake. Stores each fetch of the
-- TMD DailySeismicEvent API as-is (JSONB), so PL1 can reprocess a batch
-- without re-hitting the API. Run this once against Supabase before the DAG
-- pushes to it.

create table if not exists earthquake_reports_raw (
  id bigint generated always as identity primary key,
  fetched_at text not null,
  payload jsonb not null,
  created_at timestamptz not null default now()
);

create index if not exists idx_earthquake_reports_raw_fetched_at
  on earthquake_reports_raw (fetched_at desc);
