# Management-plane security

How the Talon coordinator's administration surface (`/api/v1`, the UI, and the
operational `/healthz` `/readyz` `/metrics` endpoints) is secured for enterprise
deployment (#85).

## Route classes

| Class | Paths | Auth |
|-------|-------|------|
| Public operational | `/healthz`, `/readyz`, `/metrics` | never required |
| Protected management | `/api/v1/**`, `/`, `/ui/**` | required when auth is enabled |

Liveness and scrape endpoints stay public so orchestrators and Prometheus work
without credentials; they expose no sensitive cluster data. To restrict them,
bind the admin server to an internal interface or filter at the proxy.

## Authentication

Set via environment:

- `TALON_COORDINATOR_AUTH_TOKEN` — a shared secret (>= 16 chars). When set,
  protected routes require `Authorization: Bearer <token>` and **fail closed**
  (`401` with a `WWW-Authenticate` challenge) otherwise. The token is compared in
  constant time and is never logged, echoed in status, or exported in metrics.
- Unset — authentication is **disabled**. This is intended for development or
  deployments where a trusted reverse proxy terminates authentication in front
  of the coordinator. The coordinator logs a warning at startup so this is never
  a silent production mistake.

`TALON_COORDINATOR_TRUST_FORWARDED=1` honors `X-Forwarded-For` for audit
attribution when behind a trusted proxy.

## TLS

TLS is **reverse-proxy terminated** in v1: the coordinator serves plain HTTP and
is expected to sit behind an ingress/proxy (nginx, Envoy, a cloud LB) that
terminates TLS and, optionally, mutual TLS. This keeps certificate loading and
rotation in the proxy's well-trodden path. Direct in-process TLS is deliberately
out of scope; the trade-off is that operators must run a proxy for encrypted
transport.

Example nginx front:

```nginx
server {
  listen 443 ssl;
  ssl_certificate     /etc/tls/talon.crt;
  ssl_certificate_key /etc/tls/talon.key;
  location / {
    proxy_pass http://coordinator:8000;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  }
}
```

## HTTP hardening

Every management response carries:

- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY` (plus the UI's `frame-ancestors 'none'` CSP)
- `Referrer-Policy: no-referrer`
- `Cache-Control: no-store` on protected data (the UI asset layer sets its own
  long-lived caching for static files only)

Request bodies are capped at 64 KiB (the API is read-only), and each request is
handled under the coordinator's bounded state-store timeout.

## Secret redaction

`SecurityConfig`'s `Debug` renders the token as `<redacted>`; the etcd/Kubernetes
backend configs likewise redact passwords and key paths. No credential appears
in logs, the management API, metrics, or error bodies.

## Rate limiting & audit

Brute-force protection against the bearer token is bounded by the constant-time
comparison plus the request-size/timeout limits; for internet-facing
deployments, configure connection/request rate limiting at the proxy (e.g.
nginx `limit_req`). Audit fields for a protected request are: timestamp, method,
path, response status, and — when `trust_forwarded_headers` is on — the
forwarded client address. The token value is never part of an audit record.
