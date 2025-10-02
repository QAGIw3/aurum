"""Infrastructure layer - Framework and external integrations.

This layer contains implementations of infrastructure concerns:
- Database access and persistence
- External API clients
- Message brokers
- Caching
- Logging

Architecture Rules:
- Infrastructure depends on domain and application layers
- Implements interfaces defined in domain layer
- Contains all framework-specific code
- No business logic
"""

