# Aurum Architecture Documentation

## Overview

The Aurum platform follows **Clean Architecture** principles with **Domain-Driven Design** (DDD) to create a maintainable, testable, and scalable energy trading platform.

## Quick Links

- **[Clean Architecture Migration Guide](CLEAN_ARCHITECTURE_MIGRATION_GUIDE.md)** - How to migrate existing code
- **[Architecture Overview](../architecture-overview.md)** - High-level system design
- **[Refactoring Plan](../../AURUM_REFACTOR_PLAN_2025.md)** - Current refactoring progress

## Architecture Layers

### 1. Domain Layer (`src/aurum/domain/`)

The **heart of the application** - pure business logic with zero dependencies.

```
domain/
├── shared_kernel/          # Common domain concepts
│   ├── entities.py         # Entity & AggregateRoot base classes
│   ├── value_objects.py    # Value objects (Money, TimeRange, etc.)
│   ├── repositories.py     # Repository interfaces
│   ├── specifications.py   # Specification pattern
│   └── exceptions.py       # Domain exceptions
│
└── energy/                 # Energy bounded context
    ├── models/             # Domain entities
    │   ├── curve.py        # Curve aggregate root
    │   ├── iso.py          # ISO market aggregate
    │   └── ppa.py          # PPA contract aggregate
    └── services/           # Domain services (complex business logic)
```

**Key Principles:**
- ✅ No framework dependencies
- ✅ Pure Python business logic
- ✅ Defines interfaces for infrastructure
- ✅ Rich domain models with behavior

**Example:**
```python
from aurum.domain.energy.models.curve import Curve, CurvePoint

curve = Curve(...)
curve.add_point(CurvePoint(tenor=Decimal('1'), value=Decimal('100')))
# Business rules enforced in domain
```

### 2. Application Layer (`src/aurum/application/`)

**Use cases and orchestration** - coordinates domain objects to fulfill business operations.

```
application/
├── common/                 # CQRS infrastructure
│   ├── commands.py         # Command pattern
│   ├── queries.py          # Query pattern
│   ├── results.py          # Result type for error handling
│   └── unit_of_work.py     # Transaction management
│
└── energy/                 # Energy use cases
    ├── curve_service.py    # Curve application service
    ├── iso_service.py      # ISO application service
    └── ppa_service.py      # PPA application service
```

**Key Principles:**
- ✅ Depends ONLY on domain
- ✅ Orchestrates domain objects
- ✅ No business logic (that's in domain)
- ✅ Transaction management

**Example:**
```python
from aurum.application.energy.curve_service import CurveApplicationService

service = CurveApplicationService(curve_repository, unit_of_work)
result = await service.create_curve(CreateCurveCommand(...))

if result.is_success():
    curve_dto = result.value
```

### 3. Infrastructure Layer (`src/aurum/infrastructure/`)

**Technical implementation** - databases, external APIs, message brokers, etc.

```
infrastructure/
├── persistence/            # Database implementations
│   ├── unit_of_work.py     # SQLAlchemy UoW implementation
│   ├── curve_repository.py # Curve repository implementation
│   ├── iso_repository.py   # ISO repository implementation
│   └── ppa_repository.py   # PPA repository implementation
│
├── messaging/              # Event bus, Kafka, etc.
├── caching/                # Redis, in-memory cache
└── external/               # External API clients
```

**Key Principles:**
- ✅ Implements domain interfaces
- ✅ Contains all framework code
- ✅ Adapters to external systems
- ✅ Can depend on domain & application

### 4. Presentation Layer (`src/aurum/api/` or `src/aurum/presentation/`)

**User interface** - REST APIs, GraphQL, WebSocket, etc.

```
api/
├── v2/                     # New endpoints using clean architecture
│   ├── curves.py           # Curve API endpoints
│   ├── iso.py              # ISO API endpoints
│   └── ppa.py              # PPA API endpoints
│
└── v1/                     # Legacy endpoints (being migrated)
```

**Key Principles:**
- ✅ Thin layer - just translates requests
- ✅ Depends on application services
- ✅ Handles HTTP/API concerns
- ✅ Input validation & serialization

## Architectural Patterns

### Domain-Driven Design (DDD)

**Bounded Contexts:**
- `energy`: Curves, ISO markets, PPA contracts
- `scenarios`: Scenario modeling and forecasting
- `external_data`: External data providers

**Tactical Patterns:**
- **Entities**: Objects with identity (e.g., `Curve`, `IsoMarket`)
- **Value Objects**: Immutable objects without identity (e.g., `Money`, `CurvePoint`)
- **Aggregates**: Consistency boundaries (e.g., `Curve` aggregate)
- **Domain Events**: Things that happened (e.g., `CurvePointAddedEvent`)
- **Repositories**: Collection-like interfaces for aggregates
- **Specifications**: Reusable business rules

### CQRS (Command Query Responsibility Segregation)

Separate read and write operations:

**Commands** (Write):
```python
@dataclass(frozen=True)
class CreateCurveCommand(Command):
    tenant_id: str
    curve_key: str
    points: List[tuple[Decimal, Decimal]]

# Handled by CurveApplicationService
result = await command_bus.dispatch(CreateCurveCommand(...))
```

**Queries** (Read):
```python
@dataclass(frozen=True)
class GetCurveQuery(Query):
    curve_id: str

# Handled by optimized read model
result = await query_bus.dispatch(GetCurveQuery(...))
```

### Event-Driven Architecture

Domain events enable loose coupling:

```python
# Domain raises events
class Curve(AggregateRoot):
    def add_point(self, point: CurvePoint):
        self.points.append(point)
        self.record_event(CurvePointAddedEvent(...))

# Handlers react to events
class UpdateAnalyticsHandler:
    async def handle(self, event: CurvePointAddedEvent):
        await self.analytics.recalculate(event.curve_key)
```

## Dependency Rules

### The Dependency Rule

**Dependencies flow inward** - outer layers depend on inner layers, never the reverse.

```
┌─────────────────────────────────────┐
│     Presentation (API/UI)           │  Depends on Application
├─────────────────────────────────────┤
│     Infrastructure (DB, Kafka)      │  Depends on Application & Domain
├─────────────────────────────────────┤
│     Application (Use Cases)         │  Depends on Domain ONLY
├─────────────────────────────────────┤
│     Domain (Business Logic)         │  Depends on NOTHING
└─────────────────────────────────────┘
```

### Enforcement

We enforce these rules automatically using:

**1. Import Linter**
```bash
# Check architectural boundaries
lint-imports

# Configuration in .importlinter
```

**2. Architectural Tests**
```bash
# Run architecture fitness tests
pytest tests/architecture/test_clean_architecture.py
```

**3. Pre-commit Hooks**
```bash
# Install pre-commit hooks
pre-commit install

# Runs lint-imports on every commit
```

## Migration Status

### ✅ Completed

- Domain layer structure
- Shared kernel (entities, value objects, repositories)
- Energy domain models (Curve, ISO, PPA)
- Application layer infrastructure (commands, queries, UoW)
- Infrastructure persistence layer skeleton
- Architectural boundary enforcement (import-linter)
- Architectural fitness tests
- Migration guide documentation

### 🚧 In Progress

- Complete repository implementations
- Domain services for complex business logic
- Additional application services (ISO, PPA)
- Event bus implementation
- Read model optimizations

### 📋 Planned

- Migrate existing features to new architecture
- Event sourcing for critical domains
- CQRS read models
- GraphQL integration
- Complete legacy code removal

## Development Workflow

### Creating a New Feature

1. **Start with Domain**
   ```python
   # Define domain model in src/aurum/domain/
   @dataclass
   class MyAggregate(AggregateRoot):
       ...
   ```

2. **Define Application Service**
   ```python
   # Create use case in src/aurum/application/
   class MyApplicationService:
       async def do_something(self, command):
           ...
   ```

3. **Implement Infrastructure**
   ```python
   # Create repository in src/aurum/infrastructure/
   class MyRepository(Repository[MyAggregate]):
       ...
   ```

4. **Add API Endpoint**
   ```python
   # Add route in src/aurum/api/v2/
   @router.post("/my-resource")
   async def create(request, service: MyApplicationService = Depends(...)):
       ...
   ```

5. **Write Tests**
   ```python
   # tests/unit/domain/
   # tests/integration/application/
   # tests/e2e/api/
   ```

### Code Review Checklist

- [ ] No framework imports in domain layer
- [ ] Business logic in domain, not application
- [ ] Application service orchestrates, doesn't decide
- [ ] Repository interfaces in domain, implementations in infrastructure
- [ ] Domain events for important state changes
- [ ] Unit tests for domain logic
- [ ] Integration tests for application services
- [ ] Architectural tests pass
- [ ] `lint-imports` passes

## Resources

### Documentation
- [Clean Architecture Migration Guide](CLEAN_ARCHITECTURE_MIGRATION_GUIDE.md)
- [Platform Roadmap](../../ROADMAP.md)
- [Contributing Guidelines](../../CONTRIBUTING.md)

### Code Examples
- Domain Models: `src/aurum/domain/energy/models/`
- Application Services: `src/aurum/application/energy/`
- Infrastructure: `src/aurum/infrastructure/persistence/`
- API Examples: `src/aurum/api/v2/`

### Testing
- Unit Tests: `tests/unit/domain/`
- Integration Tests: `tests/integration/`
- E2E Tests: `tests/e2e/`
- Architecture Tests: `tests/architecture/`

## Questions & Support

For questions about the architecture:

1. **Read the migration guide** - [CLEAN_ARCHITECTURE_MIGRATION_GUIDE.md](CLEAN_ARCHITECTURE_MIGRATION_GUIDE.md)
2. **Check existing patterns** - Look at `src/aurum/domain/energy/` for examples
3. **Run the tests** - `pytest tests/architecture/` shows enforcement in action
4. **Ask the team** - Discuss in architecture channel

## Contributing

When contributing to Aurum:

1. **Follow the architecture** - Use clean architecture patterns
2. **Write tests first** - TDD for domain logic
3. **Check boundaries** - Run `lint-imports` before committing
4. **Document decisions** - Update ADRs for significant changes
5. **Review patterns** - Ensure consistency with existing code

---

**Last Updated**: October 2025  
**Status**: Phase 1 Complete - Energy Domain Migrated  
**Next Phase**: Phase 2 - Infrastructure Modernization

