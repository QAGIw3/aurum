This folder can host ad-hoc analyses and reference SQL used in docs.

Guidelines
- Prefer `ref()` and `source()` over hard-coded relations.
- Keep analyses idempotent and light; move recurring logic into models.
- Co-locate analysis with its domain in subfolders if it grows.

