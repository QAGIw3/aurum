import asyncio
import os
from unittest.mock import AsyncMock, Mock

import pytest

os.environ.setdefault("AURUM_API_LIGHT_INIT", "1")

from aurum.data.external_provider_adapters import (
    EiaProviderAdapter,
    ExternalProviderAdapter,
    FredProviderAdapter,
    IsoProviderAdapter,
    NoaaProviderAdapter,
)


def test_list_series_filters_by_provider() -> None:
    dao = Mock()
    dao.get_series = AsyncMock(return_value=[{"id": "FRED:GDP", "provider_id": "fred"}])

    adapter = FredProviderAdapter(dao=dao)
    series = asyncio.run(adapter.list_series(limit=5))

    dao.get_series.assert_awaited_once_with(
        provider="fred",
        frequency=None,
        asof=None,
        limit=5,
        offset=0,
        cursor=None,
    )
    assert len(series) == 1
    assert series[0]["provider_id"] == "fred"
    assert series[0]["id"] == "FRED:GDP"


def test_fetch_observations_normalizes_provider() -> None:
    dao = Mock()
    dao.get_observations = AsyncMock(return_value=[{"series_id": "FRED:GDP", "value": 1.23}])

    adapter = FredProviderAdapter(dao=dao)
    obs = asyncio.run(adapter.fetch_observations("FRED:GDP", limit=2))

    dao.get_observations.assert_awaited_once_with(
        "FRED:GDP",
        start_date=None,
        end_date=None,
        frequency=None,
        asof=None,
        limit=2,
        offset=0,
        cursor=None,
    )
    assert len(obs) == 1
    assert obs[0]["provider_id"] == "fred"
    assert obs[0]["series_id"] == "FRED:GDP"


def test_fetch_metadata_filters_other_providers() -> None:
    dao = Mock()
    dao.get_metadata = AsyncMock(
        return_value={
            "providers": [
                {"id": "fred", "name": "FRED"},
                {"id": "eia", "name": "EIA"},
            ],
            "total_series": 10,
        }
    )

    adapter = FredProviderAdapter(dao=dao)
    metadata = asyncio.run(adapter.fetch_metadata(include_counts=True))

    dao.get_metadata.assert_awaited_once_with(provider="fred", include_counts=True)
    assert metadata["providers"] == [{"id": "fred", "name": "FRED"}]


def test_describe_provider_returns_first_entry() -> None:
    dao = Mock()
    dao.get_metadata = AsyncMock(return_value={"providers": [{"id": "noaa", "name": "NOAA"}]})

    adapter = NoaaProviderAdapter(dao=dao)
    provider = asyncio.run(adapter.describe_provider())

    assert provider == {"id": "noaa", "name": "NOAA"}


def test_iso_adapter_uses_custom_slug() -> None:
    dao = Mock()
    dao.get_series = AsyncMock(return_value=[])

    adapter = IsoProviderAdapter(iso="miso", dao=dao)
    asyncio.run(adapter.list_series())

    dao.get_series.assert_awaited_once_with(
        provider="miso",
        frequency=None,
        asof=None,
        limit=100,
        offset=0,
        cursor=None,
    )
    assert adapter.provider == "miso"


def test_adapter_requires_provider() -> None:
    with pytest.raises(ValueError):
        ExternalProviderAdapter("")


def test_eia_adapter_reuses_base_behavior() -> None:
    dao = Mock()
    dao.get_series = AsyncMock(return_value=[{"series_id": "EIA:SERIES"}])

    adapter = EiaProviderAdapter(dao=dao)
    series = asyncio.run(adapter.list_series())

    assert series[0]["provider_id"] == "eia"
    assert series[0]["id"] == "EIA:SERIES"
