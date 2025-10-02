# Internal Token Issuer

The Aurum API now ships with an optional internal JWT issuer that mints access
and refresh tokens for service accounts and automation workloads. When
`AURUM_API_TOKEN_ISSUER_ENABLED=1`, the API exposes `/auth` endpoints guarded by
client credentials and refresh-token workflows.

## Endpoints

- `POST /auth/token` – exchange a registered client credential for access and
  refresh tokens.
- `POST /auth/refresh` – rotate refresh tokens and issue a new access token.
- `POST /auth/logout` – revoke a refresh token or session id.
- `GET /auth/keys` – expose the JWKS document for verifying issued tokens.

Successful responses set standard OAuth fields (`access_token`,
`refresh_token`, `expires_in`, `token_type`) and honour the optional cookie
configuration described below.

## Configuration

| Environment variable | Description |
| --- | --- |
| `AURUM_API_TOKEN_ISSUER_ENABLED` | Enable the internal issuer and REST endpoints |
| `AURUM_API_ACCESS_TOKEN_TTL_SECONDS` | Access-token lifetime in seconds (default 900) |
| `AURUM_API_REFRESH_TOKEN_TTL_SECONDS` | Refresh-token lifetime in seconds (default 1209600) |
| `AURUM_API_AUTH_CLIENTS` | JSON map or list of `{client_id, client_secret, scopes, tenant, claims}` records |
| `AURUM_API_AUTH_COOKIE_ENABLED` | Set to `1` to mirror access tokens into an HTTP-only cookie |
| `AURUM_API_AUTH_COOKIE_NAME` | Cookie name when mirroring access tokens |
| `AURUM_API_AUTH_COOKIE_SECURE` | Force the cookie to require HTTPS (default: true) |
| `AURUM_API_AUTH_COOKIE_HTTP_ONLY` | Prevent JavaScript access to the cookie (default: true) |
| `AURUM_API_AUTH_COOKIE_SAMESITE` | SameSite policy (`lax`, `strict`, `none`) |
| `AURUM_API_REFRESH_TOKEN_STORE_REDIS_URL` | Optional Redis URL for refresh-token persistence (defaults to `AURUM_REDIS_URL`) |
| `AURUM_API_REFRESH_TOKEN_STORE_NAMESPACE` | Redis key namespace for refresh tokens (default `aurum:auth:refresh_tokens`) |

The issuer reuses the OIDC issuer and audience values when provided. If none are
configured, it falls back to `urn:aurum:issuer:<service-name>` and exposes its
public keys via `/auth/keys`.

## Security Notes

- Refresh tokens are hashed before storage and rotated on every refresh.
- Sessions can be revoked explicitly via `/auth/logout` and are checked on every
  authenticated request.
- Access-token `jti` and `sid` claims are included in audit logs to simplify
  incident response.
