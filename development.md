## Maintenance and Cleanup

- maintenance-cleanup: runs ruff autofix for unused imports/vars, isort, black, vulture (dead code), and dead fixtures report.
- maintenance-update-deps: rebuilds constraints (if pip-tools installed), installs deps, runs pip-audit and safety, generates SBOM.
- maintenance-report: prints JSON summary of latest reports.

### Commands

```bash
make maintenance-cleanup
make maintenance-update-deps
make maintenance-report
make constraints-compile
```

### CI

- CI Full publishes vulture report and CycloneDX SBOM.
- Weekly maintenance: `.github/workflows/maintenance-weekly.yml`.
- Pre-commit autoupdate: `.github/workflows/pre-commit-autoupdate.yml`.
- CodeQL analysis: `.github/workflows/codeql.yml`.

## Development quickstart

Prereqs:
- Python 3.11
- Optional: `uv` for faster installs (`pip install uv`)

Bootstrap (≤5 minutes on typical laptop):
```bash
make dev
```

Daily commands:
- `make unit-fast` – fast lane unit tests (<30s target)
- `make unit-watch` – re-run affected unit tests on change
- `make unit-changed` – only tests impacted by recent edits
- `make ci-unit` – lint + unit tests like CI

Integration:
- `make integration-up` → `make integration-test` → `make integration-down`

Formatting & lint:
- Pre-commit runs automatically on commit; run all hooks: `make git-pre-commit`

Conventional commits:
- Use `feat:`, `fix:`, `chore:`, etc. CI enforces via Commitizen.

Troubleshooting:
- venv issues: `rm -rf .venv && make dev`
- Pre-commit hook versions: `pre-commit autoupdate`

