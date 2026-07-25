# Admin API — OpenCollection

`admin-api.yaml` is an [OpenCollection](https://opencollection.dev) covering the
Admin API endpoints **currently implemented** by `admin-api-svc` (`internal/adminapi`)
— 37 requests across Customers, Sender IDs, SMPP Accounts, Credentials, Connectors,
Routes, and Inbound Numbers, grouped by tag. Import it into Bruno (or any
OpenCollection-compatible client).
As more operations from `api/openapi-admin.yaml` are implemented, add them here.

## Run the service

```bash
HTTP_ADMIN_TOKENS='dev-operator-token:admin:read|admin:write' \
  make run SVC=admin-api-svc
```

The service listens on `HTTP_PORT` (default **8081**) and mounts the API at `/v1`, so
the base URL is `http://localhost:8081/v1` (the `Local` environment's `baseUrl`).

## Auth

`HTTP_ADMIN_TOKENS` entries are `token:scope|scope`. The client sends **only the token
(subject) part** as `Authorization: Bearer <token>` — bearer auth is set once at the
collection level (`request.auth`) as `{{operatorToken}}`, which the `Local` environment
resolves to `dev-operator-token`. Reads need `admin:read`, mutations `admin:write`.

## Variables

Path IDs (`customerId`, `accountId`, `connectorId`, …) are collection variables seeded
with the nil UUID — paste real IDs (from the corresponding `list-*` / `create-*`
responses) before running mutating or by-id requests. Optional query filters are present
but disabled by default; enable the ones you need per request.
