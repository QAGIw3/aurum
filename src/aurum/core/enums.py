"""Core domain enumerations shared across Aurum services."""
from __future__ import annotations

from enum import Enum
from typing import ClassVar


def _normalise_token(token: str) -> str:
    """Normalise enumeration tokens for flexible parsing."""
    return "".join(ch for ch in token.upper() if ch.isalnum())


class CaseInsensitiveStrEnum(str, Enum):
    """``Enum`` base that accepts case-insensitive tokens and relaxed separators."""

    _DEFAULT_: ClassVar["CaseInsensitiveStrEnum" | None] = None

    def __str__(self) -> str:  # pragma: no cover - simple proxy
        return str(self.value)

    @classmethod
    def _missing_(cls, value: object):
        if isinstance(value, str):
            normalised = _normalise_token(value)
            for member in cls:  # pragma: no branch - bounded iteration
                if normalised == _normalise_token(member.value) or normalised == _normalise_token(member.name):
                    return member
            default = getattr(cls, "_DEFAULT_", None)
            if isinstance(default, cls):
                return default
        return None


class IsoCode(CaseInsensitiveStrEnum):
    PJM = "PJM"
    CAISO = "CAISO"
    ERCOT = "ERCOT"
    NYISO = "NYISO"
    MISO = "MISO"
    ISONE = "ISONE"
    SPP = "SPP"
    AESO = "AESO"
    OTHER = "OTHER"

    _DEFAULT_: ClassVar["IsoCode"] = OTHER


class IsoMarket(CaseInsensitiveStrEnum):
    DAY_AHEAD = "DAY_AHEAD"
    REAL_TIME = "REAL_TIME"
    FIFTEEN_MINUTE = "FIFTEEN_MINUTE"
    FIVE_MINUTE = "FIVE_MINUTE"
    HOUR_AHEAD = "HOUR_AHEAD"
    SETTLEMENT = "SETTLEMENT"
    DAY_AHEAD_EXANTE = "DAY_AHEAD_EXANTE"
    DAY_AHEAD_EXPOST = "DAY_AHEAD_EXPOST"
    REAL_TIME_EXANTE = "REAL_TIME_EXANTE"
    REAL_TIME_EXPOST = "REAL_TIME_EXPOST"
    UNKNOWN = "UNKNOWN"

    _DEFAULT_: ClassVar["IsoMarket"] = UNKNOWN


class PriceBlock(CaseInsensitiveStrEnum):
    ON_PEAK = "ON_PEAK"
    OFF_PEAK = "OFF_PEAK"
    PEAK_2X16 = "2X16"
    SUPER_OFF_PEAK = "SUPER_OFF_PEAK"
    CUSTOM = "CUSTOM"

    _DEFAULT_: ClassVar["PriceBlock"] = CUSTOM


class UnitOfMeasure(CaseInsensitiveStrEnum):
    MWH = "MWh"
    KWH = "kWh"
    THERM = "THERM"
    MMBTU = "MMBtu"
    BBL = "BBL"
    GAL = "GAL"
    SHORT_TON = "SHORT_TON"
    METRIC_TON = "METRIC_TON"
    LB = "LB"
    KG = "KG"
    OTHER = "OTHER"

    _DEFAULT_: ClassVar["UnitOfMeasure"] = OTHER


class CurrencyCode(CaseInsensitiveStrEnum):
    USD = "USD"
    CAD = "CAD"
    EUR = "EUR"
    GBP = "GBP"
    MXN = "MXN"
    AUD = "AUD"
    NZD = "NZD"
    NOK = "NOK"
    SEK = "SEK"
    CHF = "CHF"
    JPY = "JPY"
    DKK = "DKK"
    OTHER = "OTHER"

    _DEFAULT_: ClassVar["CurrencyCode"] = OTHER


__all__ = [
    "IsoCode",
    "IsoMarket",
    "PriceBlock",
    "UnitOfMeasure",
    "CurrencyCode",
    "CaseInsensitiveStrEnum",
]
