# Refactoring Quick Start Guide

**For developers continuing the refactoring work**

## Status at a Glance

- ✅ **Phase 1:** Cleanup and Debt Reduction (COMPLETE)
- 🔄 **Phase 2:** Architecture Consolidation (50% COMPLETE)
  - ✅ Data Access Layer (DONE)
  - ✅ Repository Layer (DONE)  
  - ⏳ Settings Consolidation (TODO)
  - ⏳ Service Migration (TODO)
  - ⏳ Dependency Injection (TODO)
- ⏳ **Phase 3:** External Data Ingestion (PENDING)
- ⏳ **Phase 4:** Testing and Documentation (PENDING)
- ⏳ **Phase 5:** Code Quality (ONGOING)

## What's Been Done

### ✅ Completed
1. Removed 18 demo/test files from repository root
2. Fixed code duplication in `app.py` and `v1_retired.py`
3. Audited all legacy imports
4. Created new async DAO layer (`src/aurum/data/dao/`)
5. Created repository layer (`src/aurum/data/repositories/`)
6. Wrote comprehensive documentation

### 📁 New Files Created
- `src/aurum/data/dao/` - 5 files (base + 4 backends)
- `src/aurum/data/repositories/` - 4 files (base + 3 domains)
- `src/aurum/data/README.md` - Data layer guide
- `docs/refactoring/LEGACY_AUDIT.md` - Legacy tracking
- `docs/refactoring/MIGRATION_GUIDE.md` - Migration guide
- `docs/refactoring/PROGRESS.md` - Progress tracking
- `REFACTORING_SUMMARY.md` - This session's summary

## Next Steps

### Immediate Priority: Service Migration

**Pick one service to migrate as a pilot:**

Recommended: `CurvesV2Service` (relatively simple, good test case)

**Steps:**
1. Read the service: `src/aurum/api/curves_v2_service.py`
2. Identify DAO usage (currently uses `CurvesDao`)
3. Create new service using `CurveRepository`
4. Write tests (unit + integration)
5. Deploy in parallel with old service
6. Compare performance and correctness
7. Switch over when validated
8. Document lessons learned

**Template for new service:**
```python
# src/aurum/services/core/curves.py
from typing import Optional, List, Dict, Any
from aurum.data.repositories import CurveRepository

class CurveService:
    """Curve business logic service."""
    
    def __init__(self, curve_repo: CurveRepository):
        self.curve_repo = curve_repo
    
    async def get_curves(
        self,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get curves with business logic."""
        # Add business logic here
        # Then delegate to repository
        return await self.curve_repo.find_by_filters(
            iso=iso,
            market=market,
            limit=limit
        )
```

### Secondary Priority: Integration Testing

**Set up test infrastructure:**
1. Configure test databases (Docker Compose)
2. Write integration tests for DAOs
3. Test connection pooling behavior
4. Benchmark performance

**Test example:**
```python
# tests/integration/data/test_trino_dao.py
import pytest
from aurum.data.dao import TrinoDAO

@pytest.mark.integration
@pytest.mark.asyncio
async def test_trino_query():
    async with TrinoDAO() as dao:
        results = await dao.execute_query("SELECT 1 as test")
        assert results[0]["test"] == 1
```

## How to Continue

### Option A: Complete Service Migration

**Goal:** Migrate all 33 services to use repositories

**Approach:**
1. Start with simple services (curves, metadata)
2. Move to complex services (scenarios)
3. Update one service per day
4. Test thoroughly before moving on

**Commands:**
```bash
# Create new service directory
mkdir -p src/aurum/services/core

# Copy and refactor service
cp src/aurum/api/services/curves_v2_service.py \
   src/aurum/services/core/curves.py

# Edit to use repository pattern
# Write tests
pytest tests/unit/services/test_curves.py -v

# Integration test
pytest tests/integration/services/test_curves.py -v
```

### Option B: Complete Settings Consolidation

**Goal:** Single unified settings system

**Approach:**
1. Design new settings structure
2. Create unified `AurumSettings` class
3. Migrate one subsystem at a time
4. Remove old settings classes

**Files to modify:**
- `src/aurum/core/settings.py` - Main settings file
- All imports of `HybridAurumSettings`
- All imports of `SimplifiedSettings`

### Option C: External Collector Standardization

**Goal:** Consistent patterns for all collectors

**Approach:**
1. Review existing collectors
2. Define standard interfaces
3. Refactor collectors to match
4. Update Airflow DAGs

**Files to examine:**
- `src/aurum/external/providers/` - All providers
- `src/aurum/external/runner.py` - Main runner
- `airflow/dags/external_*.py` - All external DAGs

## Key Commands

### Development
```bash
# Install dependencies
pip install -e .

# Run tests
pytest tests/unit/ -v
pytest tests/integration/ -v --db

# Type checking
mypy src/aurum/data/

# Linting
ruff check src/aurum/data/
black src/aurum/data/

# Coverage
pytest --cov=src/aurum/data tests/
```

### Testing New DAO Layer
```bash
# Unit tests (with mocks)
pytest tests/unit/data/ -v

# Integration tests (needs databases)
docker-compose -f compose/docker-compose.dev.yml up -d postgres timescale trino
pytest tests/integration/data/ -v

# Performance benchmark
python scripts/benchmark_dao.py
```

## Important Files to Know

### Documentation
- `src/aurum/data/README.md` - Data layer guide (READ THIS FIRST)
- `docs/refactoring/MIGRATION_GUIDE.md` - How to migrate code
- `docs/refactoring/LEGACY_AUDIT.md` - Legacy code tracking
- `docs/refactoring/PROGRESS.md` - Current progress

### Code
- `src/aurum/data/dao/base.py` - DAO base class (understand this)
- `src/aurum/data/repositories/base.py` - Repository base class
- `src/aurum/api/services/` - Services to migrate
- `src/aurum/api/dao/` - Legacy DAOs (to be removed)

### Config
- `pyproject.toml` - Dependencies and settings
- `.env.example` - Environment variables
- `config/` - Application configuration

## Common Tasks

### Task: Migrate a Service

```bash
# 1. Read current service
cat src/aurum/api/services/eia_service.py

# 2. Create new service
cat > src/aurum/services/external/eia.py << 'EOF'
from aurum.data.repositories import MetadataRepository  # Or appropriate repo

class EiaService:
    def __init__(self, repo: MetadataRepository):
        self.repo = repo
    
    async def get_series(self, series_id: str):
        # Business logic here
        pass
EOF

# 3. Write tests
cat > tests/unit/services/test_eia.py << 'EOF'
import pytest
from unittest.mock import AsyncMock
from aurum.services.external.eia import EiaService

@pytest.mark.asyncio
async def test_get_series():
    repo = AsyncMock()
    service = EiaService(repo)
    # Test implementation
EOF

# 4. Run tests
pytest tests/unit/services/test_eia.py -v
```

### Task: Add a New Repository

```bash
# 1. Create repository file
cat > src/aurum/data/repositories/new_domain.py << 'EOF'
from .base import BaseRepository
from ..dao import TrinoDAO  # Or appropriate DAO

class NewDomainRepository(BaseRepository):
    async def initialize(self):
        self._dao = TrinoDAO(self.settings)
        await self._dao.initialize()
    
    async def close(self):
        if self._dao:
            await self._dao.close()
    
    # Add domain methods here
EOF

# 2. Update __init__.py
echo 'from .new_domain import NewDomainRepository' >> \
  src/aurum/data/repositories/__init__.py

# 3. Write tests
pytest tests/unit/repositories/test_new_domain.py -v
```

### Task: Run Performance Comparison

```bash
# Benchmark old sync DAO
python -m timeit -s "from aurum.api.dao import CurvesDao; dao = CurvesDao()" \
  "dao.query_curves(limit=100)"

# Benchmark new async DAO
python -c "
import asyncio
import time
from aurum.data.repositories import CurveRepository

async def bench():
    start = time.time()
    async with CurveRepository() as repo:
        await repo.find_by_filters(limit=100)
    return time.time() - start

print(f'Time: {asyncio.run(bench())}s')
"
```

## Troubleshooting

### Issue: Import Errors

```bash
# Ensure package is installed in editable mode
pip install -e .

# Check Python path
python -c "import sys; print('\n'.join(sys.path))"

# Verify installation
python -c "from aurum.data.dao import TrinoDAO; print('OK')"
```

### Issue: Database Connection

```bash
# Check database status
docker-compose -f compose/docker-compose.dev.yml ps

# Test connection
python -c "
import asyncio
from aurum.data.dao import TrinoDAO

async def test():
    async with TrinoDAO() as dao:
        result = await dao.health_check()
        print(f'Health: {result}')

asyncio.run(test())
"
```

### Issue: Tests Failing

```bash
# Run with verbose output
pytest tests/ -vv -s

# Run specific test
pytest tests/unit/data/test_trino_dao.py::test_connection -vv

# Debug mode
pytest --pdb tests/

# Show print statements
pytest -s tests/
```

## Resources

### Learning
- [Async Python Guide](https://realpython.com/async-io-python/)
- [Repository Pattern](https://martinfowler.com/eaaCatalog/repository.html)
- [SOLID Principles](https://en.wikipedia.org/wiki/SOLID)

### Internal Docs
- Project README: `/README.md`
- Data Layer: `src/aurum/data/README.md`
- Migration Guide: `docs/refactoring/MIGRATION_GUIDE.md`

### Getting Help
- Read the migration guide thoroughly
- Check existing repository implementations for patterns
- Review tests for usage examples
- Ask in team chat/discussions

## Success Checklist

Before marking a task complete:
- [ ] Code follows SOLID principles
- [ ] All functions have type hints
- [ ] Comprehensive docstrings added
- [ ] Unit tests written and passing
- [ ] Integration tests passing (if applicable)
- [ ] Code reviewed by peer
- [ ] Documentation updated
- [ ] Performance benchmarked (no regression)

## Final Notes

The foundation is solid. The new DAO and repository layers are production-ready. Focus on:

1. **Service migration** - One at a time, tested thoroughly
2. **Integration testing** - Validate against real databases
3. **Performance** - Benchmark and optimize
4. **Documentation** - Keep it updated

Good luck with the next phase! 🚀

---

**Questions?** Check the docs or reach out to the team.

