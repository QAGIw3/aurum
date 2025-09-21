# Database migrations

This directory contains Alembic migrations for the scenario store and PPA contract tables.

Run migrations with:

```bash
alembic upgrade head
```

Set `AURUM_APP_DB_DSN` to point at the target PostgreSQL instance or edit `alembic.ini`.
