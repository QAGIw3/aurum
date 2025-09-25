from __future__ import annotations

"""Shared Pydantic base model for Aurum API schemas."""

from pydantic import BaseModel, ConfigDict


class AurumBaseModel(BaseModel):
    """Base model enforcing strict validation and predictable serialization."""

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        use_enum_values=True,
        populate_by_name=True,
    )
