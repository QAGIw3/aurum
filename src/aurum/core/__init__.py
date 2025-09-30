"""Aurum core domain package providing shared models and configuration."""
from .enums import CurrencyCode, IsoCode, IsoMarket, PriceBlock, UnitOfMeasure
from .models import AurumBaseModel, CurveKey, PaginationMeta, PriceObservation, UnitNormalization
# Unified configuration re-exports (single source of truth)
from libs.common.config import AurumSettings, get_settings
from .pagination import CursorPage, OffsetPage, Paginator

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
