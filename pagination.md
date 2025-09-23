# Pagination and Cursors

The API supports both offset/limit and signed cursor-based pagination for consistent, resumable iteration.

See tests `tests/api/test_cursor_semantics.py` for the definitive behavior.

## Response Metadata

List endpoints return `meta` with:

- `next_cursor`: opaque cursor to fetch the next page (alias: `since_cursor`)
- `prev_cursor`: opaque cursor to fetch the previous page
- `offset`, `limit`, and optional `count`/`total` depending on route

## Using Cursors

- Forward:
  - First page: `GET /v1/curves?limit=10`
  - Next page: `GET /v1/curves?limit=10&cursor=<next_cursor>`
  - Alias: `GET /v1/curves?limit=10&since_cursor=<next_cursor>`

- Backward:
  - `GET /v1/curves?limit=10&prev_cursor=<prev_cursor>`

## Guarantees

- Cursors are signed; tampering is detected (`signature mismatch`).
- Cursors encode filters (e.g., `asof`, `iso`); they must match the subsequent request filters.
- Cursors can expire (server-configured window); clients should handle expiration and restart from a known position.
- Backward navigation provides the previous page relative to the issued cursor window.

## Edge Cases and Guidance

- Changing filters invalidates the cursor; the server rejects mismatched filter usage.
- Large filter payloads and unicode content are supported and preserved.
- Prefer cursor-based pagination for long-running exports to avoid duplicates as data evolves.

## V2 Implementation Notes

- The v2 endpoints share a lightweight helper (`src/aurum/api/v2/pagination.py`) that encodes only the offset, limit, and concrete filters. This keeps cursors stable and the surface easy to test.
- Cursors are Base64-encoded JSON. If decoding fails the API responds with a generic `Invalid cursor` error so clients can drop the token and restart safely.
- Link headers include a fully qualified `next` URL with an URL-encoded cursor. Parse the query parameters rather than performing direct string comparisons.

Quick examples are in `README.md:Curve API`. For programmatic iteration, propagate `next_cursor` until it is absent (end of stream).
