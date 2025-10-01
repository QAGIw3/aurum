from __future__ import annotations

import pytest

from src.aurum.api.handlers.external_support import (
    providers_cache_components,
    series_cache_components,
    observations_cache_components,
    metadata_cache_components,
    dao_call_with_metrics,
)


def test_cache_key_component_helpers():
    # Providers
    assert providers_cache_components(10, None, None) == {"limit": 10, "offset": None, "cursor": ""}

    # Series
    class _P:
        provider = "px"
        frequency = "daily"
        asof = "2024-01-01"
        limit = 50
        offset = 0
        cursor = None

    s = series_cache_components(_P)
    assert s["provider"] == "px" and s["frequency"] == "daily" and s["limit"] == 50

    # Observations
    class _O:
        start_date = "2024-01-01"
        end_date = "2024-01-31"
        frequency = None
        asof = None
        limit = 100
        offset = 0

    o = observations_cache_components("sid", _O)
    assert o["series_id"] == "sid" and o["start_date"] == "2024-01-01" and o["limit"] == 100

    # Metadata
    m = metadata_cache_components("prov", True)
    assert m == {"provider": "prov", "include_counts": True}


@pytest.mark.asyncio
async def test_dao_call_with_metrics_success_and_error():
    async def _ok():
        return 123

    val = await dao_call_with_metrics("op", _ok)
    assert val == 123

    async def _err():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await dao_call_with_metrics("op", _err)

