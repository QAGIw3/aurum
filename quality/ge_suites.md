# Great Expectations Suites Refactor Guide

This repository maintains Great Expectations (GE) suites with a single source of truth and a generated working copy used by hooks and tests.

- Canonical source: `libs/contracts/ge/expectations/`
- Generated target: `ge/expectations/`
- Catalog index: `ge/index.json`

Use the Makefile helpers:

- `make ge-sync` — Copy canonical suites → `ge/expectations/` and build `ge/index.json` (domain + tier classification)
- `make ge-lint` — Lint for drift (canonical vs. target), suite-name consistency, and Tier A compatibility with the minimal validator
- `make ge-generate-curves` — Rebuild curves-domain suites from fragments (canonical → libs/contracts/ge/expectations)
- `make ge-generate-external` — Rebuild external-domain suites from fragments
- `make ge-generate-iso` — Rebuild ISO-domain suites from fragments

Tiers:

- Tier A (minimal validator compatible): only uses expectation types supported by `src/aurum/dq/validator.py`
- Tier B (GE-only): JSON suites that include GE expectation types not supported by the minimal validator
- Tier C (YAML/Multi-table): YAML suites or multi-table style definitions; run with GE only

Notes:

- Do not hand-edit files in `ge/expectations/`. Make changes under `libs/contracts/ge/expectations/` and run `make ge-sync`.
- The lakeFS pre-commit hook continues to validate the curve landing suite at `ge/expectations/curve_landing.json`.
- Environment overrides in suites (e.g., scenario thresholds) should be declared in `.env.example` if you want the linter to recognize them.

Future work (optional):

- Fragments live under `libs/contracts/ge/fragments/curves`. The curves generator at `scripts/contracts/generate_curves_suites.py` composes canonical suites from these fragments.
  - External fragments live under `libs/contracts/ge/fragments/external` and are composed by `scripts/contracts/generate_external_suites.py`.
  - ISO fragments live under `libs/contracts/ge/fragments/iso` and are composed by `scripts/contracts/generate_iso_suites.py`.
- Introduce templates (optional) in `libs/contracts/ge/templates/` if a more general composition layer is needed across domains.
- Extend the minimal validator with YAML support and additional expectation types if needed.
