"""Aurum core domain package providing shared models and configuration."""
from .enums import CurrencyCode, IsoCode, IsoMarket, PriceBlock, UnitOfMeasure
from .models import AurumBaseModel, CurveKey, PaginationMeta, PriceObservation, UnitNormalization
from .settings import AurumSettings
from .pagination import CursorPage, OffsetPage, Paginator

__all__ = [
    "AurumSettings",
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
