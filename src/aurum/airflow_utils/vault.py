"""Small helpers for composing Vault env pull commands for BashOperator.

These return shell snippets that safely attempt to populate environment
variables from Vault using scripts/secrets/pull_vault_env.py. They are
designed to be embedded in BashOperator bash_command strings or passed as
pre_lines to utilities like create_seatunnel_ingest_chain.
"""
from __future__ import annotations

import os
from typing import Iterable


def build_pull_env_command(mappings: Iterable[str]) -> str:
    """Return a shell line that pulls Vault secrets into env vars.

    Args:
        mappings: Iterable of mapping specs like
            "secret/data/aurum/pjm:token=PJM_API_KEY".
    """

    flags = " ".join(f"--mapping {m}" for m in mappings)

    # Resolve defaults but allow overrides at execution time via ${AURUM_*}
    vault_addr_default = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
    vault_token_default = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
    pythonpath_entry = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
    venv_python = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")

    line = (
        "eval \"$(VAULT_ADDR=${AURUM_VAULT_ADDR:-"
        + vault_addr_default
        + "} VAULT_TOKEN=${AURUM_VAULT_TOKEN:-"
        + vault_token_default
        + "} PYTHONPATH=${PYTHONPATH:-}:"
        + pythonpath_entry
        + " "
        + venv_python
        + " scripts/secrets/pull_vault_env.py "
        + flags
        + " --format shell)\" || true"
    )

    return line


__all__ = ["build_pull_env_command"]

