"""Configuration for contract tests."""

import pytest


def pytest_configure(config):
    """Configure pytest for contract tests."""
    config.addinivalue_line(
        "markers", "contract: mark test as contract test validating API against OpenAPI spec"
    )


def pytest_collection_modifyitems(config, items):
    """Skip contract tests by default unless explicitly requested."""
    skip_contract = pytest.mark.skip(reason="contract tests require --contract flag")

    for item in items:
        if "contract" in item.keywords:
            item.add_marker(skip_contract)
