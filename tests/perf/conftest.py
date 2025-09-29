"""Configuration for performance tests."""

import pytest


def pytest_configure(config):
    """Configure pytest for performance tests."""
    config.addinivalue_line(
        "markers", "perf: mark test as performance regression test"
    )


def pytest_collection_modifyitems(config, items):
    """Skip performance tests by default unless explicitly requested."""
    skip_perf = pytest.mark.skip(reason="performance tests require --perf flag")

    for item in items:
        if "perf" in item.keywords:
            item.add_marker(skip_perf)
