"""Domain layer - Pure business logic with no framework dependencies.

This layer contains the core business logic of the Aurum platform, organized
into bounded contexts following Domain-Driven Design principles.

Architecture Rules:
- Domain layer has NO dependencies on other layers
- No framework imports (FastAPI, SQLAlchemy, etc.)
- Only pure Python and domain-specific logic
- All external dependencies must be abstracted through interfaces
"""

