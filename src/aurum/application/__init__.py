"""Application layer - Use cases and orchestration logic.

This layer contains application services that orchestrate domain operations.
Application services are the entry points for external requests and handle:
- Transaction management
- Validation
- Authorization
- Orchestrating domain objects
- Publishing domain events

Architecture Rules:
- Application layer depends ONLY on domain layer
- No framework-specific code (use abstractions)
- No business logic (that belongs in domain)
- Coordinates but doesn't make business decisions
"""

