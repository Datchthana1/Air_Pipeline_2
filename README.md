# Air_Pipeline_2

## Tests & data quality

- Unit tests (no Airflow, database or network needed): `python -m pip install -r requirements-dev.txt`, then `python -m pytest -q`.
- Data-quality checks, read-only, run against the live database: `psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f dags/sqlscript/dq-checks.sql`. Every `error` check should return 0 rows.
- Cleaning rules, bug fixes and known gaps are listed in [docs/DATA_CLEANING.md](docs/DATA_CLEANING.md).
