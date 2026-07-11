-- Indexes backing the queries used by get_distinct_stations() / transform_station().
-- Without these, every transform (per-station or all-at-once) does a
-- sequential scan of air_stations, and that scan gets slower every hour as
-- PL0 ingests more rows -- the second half of why statement_timeout gets hit
-- more often over time even after batching is fixed.
--
-- air_stations.created_at and .recorded_at are stored as TEXT
-- ("YYYY-MM-DD HH:MM:SS" from ingest.py), which sorts identically to a
-- proper timestamp because the format is fixed-width/zero-padded, so plain
-- btree indexes on the text columns work correctly for both equality and
-- range/BETWEEN filters and for MAX(created_at).

-- Supports: get_distinct_stations() "latest" branch — MAX(created_at) over
-- the whole table, and station_id lookups scoped to the newest batch.
create index if not exists idx_air_stations_created_at
  on air_stations (created_at desc);

-- Supports: transform_station() "latest" branch — MAX(created_at) scoped to
-- one station — and any WHERE station_id = X AND created_at = Y.
create index if not exists idx_air_stations_station_created
  on air_stations (station_id, created_at desc);

-- Supports: "day"/"range" modes — WHERE station_id = X AND recorded_at::date
-- BETWEEN ... — and get_distinct_stations()'s day/range branch.
create index if not exists idx_air_stations_station_recorded
  on air_stations (station_id, recorded_at);

analyze air_stations;
