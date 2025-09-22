# Deprecated migration directory

The canonical set of database migrations now lives under `db/migrations/` (Alembic).
This folder remains only to avoid breaking legacy tooling references. Please add new
revisions via Alembic in `db/migrations/versions/`. Existing SQL-based scripts were
ported to Alembic revision `20250101_06_hardened_scenario_constraints`.
