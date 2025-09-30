# ADR: API Versioning – Adopt v2-only with short deprecation window

Status: Accepted
Date: 2025-09-30

## Context

The codebase currently supports legacy v1 and modular v2 routers, leading to duplicated code paths, middleware complexity, and confused clients. We aim to reduce surface area and maintenance cost.

## Decision

- Freeze API at v2 as the sole public surface.
- Maintain a short deprecation window for v1 routes (30 days) with explicit warnings and `Deprecation` headers.
- Provide a compatibility shim for the window only; after the window, remove v1 mounts and code paths.

## Implementation Outline

1. Default settings: `enable_v2_only=true` in non-local environments.
2. Add a `Deprecation` header and `Sunset` header to any remaining v1 handlers during the window.
3. Publish a migration guide mapping v1→v2 endpoints and payloads under `docs/api/migration-v1-to-v2.md`.
4. After 30 days, remove v1 routers, feature flags, and legacy middleware branches.

## Impact

- Simpler router registry and middleware chain
- Lower error rate and improved latency due to fewer conditionals
- Clear client story; reduced documentation duplication

## Alternatives Considered

- Indefinite dual support: rejected due to ongoing cost and slower iteration.
- Version negotiation headers: unnecessary complexity for current clients.

## Migration Plan

- Week 0: Announce v2-only, release migration guide, ship deprecation headers
- Week 2: Audit client traffic; reach out to remaining v1 callers
- Week 4: Remove v1; bump minor version; update OpenAPI and docs


