"""Parser utilities and vendor adapters for Aurum."""

from .vendor_curves import PARSERS, parse, parse_with_diagnostics, register

__all__ = ["parse", "parse_with_diagnostics", "register", "PARSERS"]
