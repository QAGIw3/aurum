# Traefik Forward-Auth with OIDC

This guide shows how to protect Aurum services with [Traefik](https://doc.traefik.io/traefik/) using
[oauth2-proxy](https://oauth2-proxy.github.io/oauth2-proxy/) as an OIDC forward-auth gateway.
The configuration works for both the local Docker compose stack and the `k8s/base`
Kubernetes deployment.

## Prerequisites

1. An OIDC provider (Azure AD, Okta, Auth0, Keycloak, …) with:
   - Client ID / secret
   - Issuer discovery URL (e.g. `https://login.microsoftonline.com/<tenant>/v2.0`)
   - Redirect URI that Traefik/oauth2-proxy will call back (see below)
2. A 32 byte random cookie secret (base64 encoded) for oauth2-proxy – run `python -c "import secrets; print(secrets.token_urlsafe(32))"` to generate one.
3. For production you should use TLS everywhere. The examples below keep certificates out of scope.

## Docker Compose

1. Populate these values in `.env` (examples assume issuer `https://id.example.com`):

   ```bash
   OAUTH2_PROXY_PROVIDER=oidc
   OAUTH2_PROXY_ISSUER_URL=https://id.example.com/.well-known/openid-configuration
   OAUTH2_PROXY_CLIENT_ID=<client id>
   OAUTH2_PROXY_CLIENT_SECRET=<client secret>
   OAUTH2_PROXY_COOKIE_SECRET=<32-byte-secret>
   OAUTH2_PROXY_REDIRECT_URL=http://api.aurum.localhost/oauth2/callback
   OAUTH2_PROXY_EMAIL_DOMAINS=*
   AURUM_API_AUTH_DISABLED=1  # rely on oauth2-proxy to authenticate requests
   ```

2. Add a hosts entry so Traefik can match the router rule:

   ```text
   127.0.0.1   api.aurum.localhost
   ```

3. Start the stack with the optional `auth` profile:

   ```bash
   COMPOSE_PROFILES=core,auth docker compose -f compose/docker-compose.dev.yml up traefik oauth2-proxy
   ```

4. Traefik listens on `http://localhost:8082`. Visiting `http://api.aurum.localhost:8082/v1/curves`
   should redirect you to the OIDC provider for login. After authenticating the request is
   forwarded to the API with the user headers set (`X-Auth-Request-User`, `X-Auth-Request-Email`, …).

> **Tip:** if you also want the Aurum API to verify JWTs, set `AURUM_API_AUTH_DISABLED=0` and
> configure `AURUM_API_OIDC_{ISSUER,AUDIENCE,JWKS_URL}`. oauth2-proxy adds an `Authorization` header
> containing the ID token when `OAUTH2_PROXY_SET_AUTHORIZATION_HEADER=true` (the default in our compose profile).

## Kubernetes (k8s/base)

1. Update `k8s/base/configmap-env.yaml` (or create an overlay) with your issuer, client ID, redirect URL, etc.
2. Create the oauth2-proxy secret containing the client secret and cookie secret:

   ```bash
   kubectl -n aurum-dev create secret generic oauth2-proxy-secret \
     --from-literal=client-secret=<client secret> \
     --from-literal=cookie-secret=<32-byte-secret>
   ```

   The placeholder file `k8s/base/oauth2-proxy-secret.yaml` is provided as a template if you manage
   secrets via Gitops.

3. Apply the base manifests (or your overlay):

   ```bash
   kubectl apply -k k8s/base
   ```

4. The API `IngressRoute` now references the `oidc-forward-auth` middleware. Any request routed
   through Traefik must satisfy oauth2-proxy before reaching the Aurum API.

5. Expose Traefik with TLS and DNS appropriate for your cluster. The middleware is configured to pass
   `X-Auth-Request-*` headers which can be consumed by downstream services and the Aurum API.

## Customisation

- To protect additional services (Superset, OpenMetadata, …), add the `oidc-forward-auth` middleware
  to their respective `IngressRoute` definitions (Kubernetes) or Traefik routers (docker compose `auth.yml`).
- Adjust `OAUTH2_PROXY_EMAIL_DOMAINS`, `OAUTH2_PROXY_ALLOWED_GROUPS`, etc., to enforce tighter access control.
- For sensitive deployments ensure cookies are marked `secure` and Traefik terminates TLS.

With forward-auth enabled you get single-sign-on at the edge while the Aurum API can still enforce
fine-grained authorization using the decoded claims.
