### Feature Flags (Configuration)

Standard flags exposed via environment variables:

- `AURUM_USE_SIMPLIFIED_SETTINGS`: enable simplified, centralized settings.
- `AURUM_ENABLE_MIGRATION_MONITORING`: record settings migration metrics.
- `AURUM_SETTINGS_MIGRATION_PHASE`: `legacy|hybrid|simplified` for rollout control.

Database-related flags live under Trino client (`src/aurum/api/database/trino_client.py`).

Flags accept `true/false`, `1/0`, `yes/no`. Lowercase variants are accepted with a deprecation warning.


