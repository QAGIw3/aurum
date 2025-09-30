"""Convenience wrappers for emitting external contract events to Kafka topics."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Iterable, Sequence

from aurum.external.runner import run_once

logger = logging.getLogger(__name__)

DEFAULT_PROVIDERS: tuple[str, ...] = ("eia", "fred", "noaa", "worldbank")


@dataclass
class PublishResult:
    provider: str
    status: str
    error: str | None = None


class ExternalContractsPublisher:
    """High-level façade over :mod:`aurum.external.runner` for canonical contracts."""

    def __init__(self, providers: Sequence[str] | None = None) -> None:
        self._default_providers: tuple[str, ...] = tuple(p.lower() for p in (providers or DEFAULT_PROVIDERS))

    async def publish(
        self,
        providers: Sequence[str] | None = None,
        *,
        catalog: bool = True,
        observations: bool = True,
    ) -> list[PublishResult]:
        """Publish catalog/observation events for each provider sequentially."""
        target = tuple(p.lower() for p in (providers or self._default_providers))
        results: list[PublishResult] = []
        for provider in target:
            try:
                await run_once([provider], catalog=catalog, observations=observations)
                results.append(PublishResult(provider=provider, status="success"))
            except Exception as exc:  # pragma: no cover - surfaced to caller
                logger.exception("Publisher run failed", extra={"provider": provider})
                results.append(PublishResult(provider=provider, status="error", error=str(exc)))
        return results

    def publish_sync(
        self,
        providers: Sequence[str] | None = None,
        *,
        catalog: bool = True,
        observations: bool = True,
    ) -> list[PublishResult]:
        """Synchronous wrapper suitable for Airflow operators or CLIs."""
        return asyncio.run(self.publish(providers, catalog=catalog, observations=observations))

    async def publish_provider(
        self,
        provider: str,
        *,
        catalog: bool = True,
        observations: bool = True,
    ) -> PublishResult:
        """Publish for a single provider and return the execution status."""
        result = await self.publish([provider], catalog=catalog, observations=observations)
        return result[0]

    def publish_provider_sync(
        self,
        provider: str,
        *,
        catalog: bool = True,
        observations: bool = True,
    ) -> PublishResult:
        return asyncio.run(self.publish_provider(provider, catalog=catalog, observations=observations))


__all__ = ["ExternalContractsPublisher", "PublishResult", "DEFAULT_PROVIDERS"]
