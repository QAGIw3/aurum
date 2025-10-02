"""Aurum core domain package providing shared models and configuration."""
from .enums import CurrencyCode, IsoCode, IsoMarket, PriceBlock, UnitOfMeasure
from .models import AurumBaseModel, CurveKey, PaginationMeta, PriceObservation, UnitNormalization
from .pagination import CursorPage, OffsetPage, Paginator

# Unified configuration re-exports (single source of truth)
try:
    from aurum.core.settings import AurumSettings, get_settings
except ImportError:
    # Fallback for when libs is not available
    class MockAurumSettings:
        pass
    AurumSettings = MockAurumSettings

    def get_settings():
        return AurumSettings()

__all__ = [
    "AurumSettings",
    "get_settings",
    "AurumBaseModel",
    "UnitNormalization",
    "CurveKey",
    "PriceObservation",
    "PaginationMeta",
    "CurrencyCode",
    "UnitOfMeasure",
    "IsoCode",
    "IsoMarket",
    "PriceBlock",
    "Paginator",
    "OffsetPage",
    "CursorPage",
]
