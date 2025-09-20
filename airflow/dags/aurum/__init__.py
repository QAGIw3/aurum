"""Lightweight stubs for DAG-time imports in Airflow pods.

These stubs allow DAG modules to import `aurum.db`, `aurum.dq`, and
`aurum.reference` during parsing without requiring the full Aurum package
to be installed inside the Airflow image. At runtime we mount the real
`src/aurum` into `/opt/airflow/src` and prepend it to `PYTHONPATH` from
the DAG bash commands, so task code still uses the real modules.
"""

# Provide a minimal submodule for `aurum.reference` so `from aurum.reference.eia_catalog import ...`
# doesn't break DAG parsing when the full package isn't installed.
try:  # pragma: no cover - best effort stub wiring
    import types
    import sys

    reference_pkg = types.ModuleType("aurum.reference")
    sys.modules.setdefault("aurum.reference", reference_pkg)
except Exception:
    pass


