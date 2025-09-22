"""Provider-specific adapters on top of ExternalDAO with uniform semantics."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from .external_dao import ExternalDAO


class ExternalProviderAdapter:
    """Thin wrapper around ExternalDAO enforcing provider-specific semantics."""

    def __init__(self, provider: str, *, dao: Optional[ExternalDAO] = None) -> None:
        provider_slug = (provider or "").strip().lower()
        if not provider_slug:
            raise ValueError("provider must be provided")
        self._provider = provider_slug
        self._dao = dao or ExternalDAO()

    @property
    def provider(self) -> str:
        return self._provider

    async def list_series(
        self,
        *,
        frequency: Optional[str] = None,
        asof: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        cursor: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        records = await self._dao.get_series(
            provider=self._provider,
            frequency=frequency,
            asof=asof,
            limit=limit,
            offset=offset,
            cursor=cursor,
        )
        return [self._normalize_series(record) for record in records]

    async def fetch_observations(
        self,
        series_id: str,
        *,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        frequency: Optional[str] = None,
        asof: Optional[str] = None,
        limit: int = 500,
        offset: int = 0,
        cursor: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        records = await self._dao.get_observations(
            series_id,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency,
            asof=asof,
            limit=limit,
            offset=offset,
            cursor=cursor,
        )
        return [self._normalize_observation(record) for record in records]

    async def fetch_metadata(self, *, include_counts: bool = False) -> Dict[str, Any]:
        metadata = await self._dao.get_metadata(provider=self._provider, include_counts=include_counts)
        filtered = deepcopy(metadata)
        providers = filtered.get("providers", [])
        if isinstance(providers, list):
            filtered["providers"] = [p for p in providers if p.get("id", "").lower() == self._provider]
        return filtered

    async def describe_provider(self) -> Optional[Dict[str, Any]]:
        metadata = await self.fetch_metadata(include_counts=False)
        providers = metadata.get("providers") or []
        return providers[0] if providers else None

    def _normalize_series(self, record: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(record)
        normalized["provider_id"] = self._provider
        if "id" not in normalized and "series_id" in normalized:
            normalized["id"] = normalized["series_id"]
        return normalized

    def _normalize_observation(self, record: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(record)
        normalized.setdefault("provider_id", self._provider)
        return normalized


class FredProviderAdapter(ExternalProviderAdapter):
    def __init__(self, *, dao: Optional[ExternalDAO] = None) -> None:
        super().__init__("fred", dao=dao)


class EiaProviderAdapter(ExternalProviderAdapter):
    def __init__(self, *, dao: Optional[ExternalDAO] = None) -> None:
        super().__init__("eia", dao=dao)


class NoaaProviderAdapter(ExternalProviderAdapter):
    def __init__(self, *, dao: Optional[ExternalDAO] = None) -> None:
        super().__init__("noaa", dao=dao)


class IsoProviderAdapter(ExternalProviderAdapter):
    def __init__(self, iso: str = "isone", *, dao: Optional[ExternalDAO] = None) -> None:
        super().__init__(iso, dao=dao)
