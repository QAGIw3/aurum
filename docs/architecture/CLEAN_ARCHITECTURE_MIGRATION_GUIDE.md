# Clean Architecture Migration Guide

## Overview

This guide documents the new clean architecture implementation for the Aurum platform and provides patterns for migrating existing code to the new structure.

## Architecture Layers

### Layer Structure

```
src/aurum/
├── domain/              # Pure business logic (no dependencies)
│   ├── shared_kernel/   # Common domain concepts
│   │   ├── entities.py         # Entity & AggregateRoot base classes
│   │   ├── value_objects.py    # Value objects (Money, TimeRange, etc.)
│   │   ├── repositories.py     # Repository interfaces
│   │   ├── specifications.py   # Specification pattern
│   │   └── exceptions.py       # Domain exceptions
│   └── energy/          # Energy bounded context
│       ├── models/      # Domain entities
│       │   ├── curve.py        # Curve aggregate
│       │   ├── iso.py          # ISO market aggregate
│       │   └── ppa.py          # PPA aggregate
│       └── services/    # Domain services
│
├── application/         # Use cases & orchestration
│   ├── common/          # CQRS infrastructure
│   │   ├── commands.py         # Command pattern
│   │   ├── queries.py          # Query pattern
│   │   ├── results.py          # Result type
│   │   └── unit_of_work.py     # UoW pattern
│   └── energy/          # Energy use cases
│       └── curve_service.py    # Curve application service
│
├── infrastructure/      # Framework & external integrations
│   └── persistence/
│       ├── unit_of_work.py      # SQLAlchemy UoW
│       ├── curve_repository.py  # Curve repository impl
│       ├── iso_repository.py    # ISO repository impl
│       └── ppa_repository.py    # PPA repository impl
│
└── presentation/        # API layer (or keep in api/)
    └── rest/            # REST API endpoints
```

## Dependency Rules

### Clean Architecture Principles

1. **Domain Layer** (innermost)
   - NO dependencies on other layers
   - NO framework imports (FastAPI, SQLAlchemy, etc.)
   - Pure Python with business logic only
   - Defines interfaces that infrastructure implements

2. **Application Layer**
   - Depends ONLY on domain layer
   - Orchestrates domain objects
   - Implements use cases
   - No framework-specific code

3. **Infrastructure Layer**
   - Depends on domain and application
   - Implements interfaces defined in domain
   - Contains all framework code
   - Adapters to external services

4. **Presentation Layer**
   - Depends on all layers through abstractions
   - FastAPI routes, GraphQL resolvers, etc.
   - Thin layer - just translates requests

### Enforcing Boundaries

We use `import-linter` to enforce these rules automatically:

```bash
# Run import linter
lint-imports

# In CI/CD
pip install import-linter
lint-imports
```

Configuration is in `.importlinter` file.

## Migration Patterns

### Pattern 1: Migrating Existing Services

**Before (old structure):**
```python
# src/aurum/api/services/curve_service.py
from fastapi import Depends
from sqlalchemy.orm import Session

class CurveService:
    def __init__(self, db: Session):
        self.db = db
    
    async def create_curve(self, data: dict):
        # Mix of validation, business logic, and persistence
        curve = Curve(**data)
        self.db.add(curve)
        self.db.commit()
        return curve
```

**After (clean architecture):**

```python
# 1. Domain model (src/aurum/domain/energy/models/curve.py)
@dataclass
class Curve(AggregateRoot):
    id: CurveId
    metadata: CurveMetadata
    points: List[CurvePoint]
    
    def add_point(self, point: CurvePoint) -> None:
        # Pure business logic
        if any(p.tenor == point.tenor for p in self.points):
            raise BusinessRuleViolation("Duplicate tenor")
        self.points.append(point)
        self.record_event(CurvePointAddedEvent(...))

# 2. Application service (src/aurum/application/energy/curve_service.py)
class CurveApplicationService:
    def __init__(self, repository: Repository[Curve], uow: UnitOfWork):
        self.repository = repository
        self.uow = uow
    
    async def create_curve(self, command: CreateCurveCommand) -> Result[CurveDTO]:
        try:
            # Create domain entity
            curve = Curve(...)
            
            # Persist through repository
            async with self.uow:
                await self.repository.save(curve)
                await self.uow.commit()
            
            return success(self._to_dto(curve))
        except DomainException as e:
            return failure("DOMAIN_ERROR", str(e))

# 3. Infrastructure (src/aurum/infrastructure/persistence/curve_repository.py)
class CurveRepository(Repository[Curve]):
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def save(self, aggregate: Curve) -> None:
        # Convert domain model to ORM model
        # Handle persistence
        pass

# 4. Presentation (src/aurum/api/v2/curves.py)
@router.post("/curves")
async def create_curve(
    request: CreateCurveRequest,
    service: CurveApplicationService = Depends(get_curve_service)
) -> CreateCurveResponse:
    # Translate HTTP request to command
    command = CreateCurveCommand(...)
    
    # Execute use case
    result = await service.create_curve(command)
    
    # Translate result to HTTP response
    if result.is_success():
        return CreateCurveResponse.from_dto(result.value)
    else:
        raise HTTPException(status_code=400, detail=result.message)
```

### Pattern 2: Domain Events

**Publishing events:**

```python
# Domain entity records events
class Curve(AggregateRoot):
    def add_point(self, point: CurvePoint) -> None:
        self.points.append(point)
        self.record_event(CurvePointAddedEvent(
            aggregate_id=self.id,
            curve_key=self.metadata.curve_key,
            tenor=point.tenor,
            value=point.value
        ))

# Repository publishes events after save
class CurveRepository:
    async def save(self, aggregate: Curve) -> None:
        # Save to database
        ...
        
        # Publish domain events
        for event in aggregate.clear_events():
            await self.event_bus.publish(event)
```

### Pattern 3: CQRS Implementation

**Commands (write operations):**

```python
@dataclass(frozen=True)
class CreateCurveCommand(Command):
    tenant_id: str
    curve_key: str
    points: List[tuple[Decimal, Decimal]]

class CreateCurveHandler(CommandHandler[CreateCurveCommand, CurveDTO]):
    async def handle(self, command: CreateCurveCommand) -> Result[CurveDTO]:
        # Implement use case
        ...
```

**Queries (read operations):**

```python
@dataclass(frozen=True)
class GetCurveQuery(Query):
    curve_id: str
    tenant_id: str

class GetCurveHandler(QueryHandler[GetCurveQuery, CurveDTO]):
    async def handle(self, query: GetCurveQuery) -> Result[CurveDTO]:
        # Read from optimized read model
        ...
```

### Pattern 4: Value Objects

**Use value objects for domain concepts:**

```python
@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: str = "USD"
    
    def __add__(self, other: Money) -> Money:
        if self.currency != other.currency:
            raise ValueError("Currency mismatch")
        return Money(self.amount + other.amount, self.currency)

# Usage in domain
class PowerPurchaseAgreement(AggregateRoot):
    fixed_price: Money  # Not just Decimal!
```

## Migration Strategy

### Phase 1: New Features (Immediate)
- All new features MUST use clean architecture
- Use existing features as reference
- No changes to existing code yet

### Phase 2: Gradual Migration (Ongoing)
1. Start with least-coupled features
2. Extract domain logic from services
3. Create domain models
4. Create application services
5. Update API layer to use new services
6. Run in parallel with feature flags
7. Switch over when validated

### Phase 3: Legacy Cleanup (Later)
- Remove old implementations
- Consolidate duplicate code
- Update tests

## Testing Strategy

### Unit Tests (Domain Layer)

```python
def test_curve_add_point():
    """Test adding a point to a curve (pure domain logic)."""
    curve = Curve(
        id=CurveId.generate(),
        tenant_id=TenantId.generate(),
        metadata=CurveMetadata(...),
        points=[CurvePoint(Decimal('1'), Decimal('100'))]
    )
    
    curve.add_point(CurvePoint(Decimal('2'), Decimal('105')))
    
    assert len(curve.points) == 2
    assert curve.points[1].tenor == Decimal('2')
```

### Integration Tests (Application + Infrastructure)

```python
@pytest.mark.asyncio
async def test_create_curve_integration(session, uow):
    """Test creating a curve end-to-end."""
    repository = CurveRepository(session)
    service = CurveApplicationService(repository, uow)
    
    command = CreateCurveCommand(...)
    result = await service.create_curve(command)
    
    assert result.is_success()
    
    # Verify persistence
    curve = await repository.get_by_id(result.value.id)
    assert curve is not None
```

### API Tests (E2E)

```python
@pytest.mark.asyncio
async def test_create_curve_api(client):
    """Test curve creation through API."""
    response = await client.post("/v2/curves", json={...})
    
    assert response.status_code == 201
    assert response.json()["curve_key"] == "test_curve"
```

## Common Pitfalls

### ❌ Don't: Put business logic in application service

```python
# BAD
class CurveApplicationService:
    async def add_point(self, command):
        curve = await self.repository.get_by_id(command.curve_id)
        # Business logic in application layer!
        if any(p.tenor == command.tenor for p in curve.points):
            raise BusinessRuleViolation("Duplicate")
        curve.points.append(...)
```

### ✅ Do: Put business logic in domain

```python
# GOOD
class Curve(AggregateRoot):
    def add_point(self, point: CurvePoint) -> None:
        # Business logic in domain
        if any(p.tenor == point.tenor for p in self.points):
            raise BusinessRuleViolation("Duplicate")
        self.points.append(point)

class CurveApplicationService:
    async def add_point(self, command):
        curve = await self.repository.get_by_id(command.curve_id)
        curve.add_point(CurvePoint(...))  # Just orchestrate
        await self.repository.save(curve)
```

### ❌ Don't: Import frameworks in domain

```python
# BAD
from sqlalchemy import Column, Integer
from pydantic import BaseModel

@dataclass
class Curve(BaseModel):  # Framework in domain!
    ...
```

### ✅ Do: Keep domain pure

```python
# GOOD
from dataclasses import dataclass

@dataclass
class Curve(AggregateRoot):  # Pure Python
    ...
```

## Tools and Automation

### Pre-commit Hooks

Add to `.pre-commit-config.yaml`:

```yaml
  - repo: local
    hooks:
      - id: import-linter
        name: Check architectural boundaries
        entry: lint-imports
        language: system
        pass_filenames: false
```

### CI/CD Integration

```yaml
# .github/workflows/architecture.yml
name: Architecture Tests

on: [push, pull_request]

jobs:
  architecture:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
      - name: Install dependencies
        run: |
          pip install import-linter pytest
      - name: Check architectural boundaries
        run: lint-imports
      - name: Run architecture tests
        run: pytest tests/architecture/
```

## Resources

- **Architecture Documentation**: `docs/architecture-overview.md`
- **Domain Models**: `src/aurum/domain/`
- **Application Services**: `src/aurum/application/`
- **Examples**: `examples/clean_architecture_examples.py`

## Questions?

For questions or discussions about clean architecture migration:
1. Check existing patterns in `src/aurum/domain/energy/`
2. Review application services in `src/aurum/application/energy/`
3. Run architectural tests: `pytest tests/architecture/`
4. Consult the team in architecture discussions

## Next Steps

1. **Familiarize** yourself with the domain models in `src/aurum/domain/energy/`
2. **Study** the application service patterns in `src/aurum/application/energy/`
3. **Run** the architectural tests to see enforcement in action
4. **Migrate** your feature using the patterns documented here
5. **Validate** with `lint-imports` and `pytest tests/architecture/`

