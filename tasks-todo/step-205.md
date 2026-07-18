# step-205 — TLS / SMPP-TLS / mTLS sur les transports

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-206

## But
Chiffrer et authentifier les transports : TLS sur les APIs HTTP, SMPP-TLS sur les binds, mTLS entre
services internes (dont l'Admin API et le gRPC billing).

## Périmètre (ce que fait CETTE PR)
- HTTP : TLS sur `rest-api-svc` (public) et mTLS sur `admin-api-svc` (interne).
- SMPP : option SMPP-TLS pour `smpp-server-svc` (entrant) et `connector-pool-svc` (sortant).
- gRPC : mTLS pour `billing-svc` (et futurs services gRPC).
- Config TLS (certs/clés/CA) via `internal/config`, jamais de secret en dur.

## Points d'implémentation clés
- L'Admin API est **interne** et déjà pensée derrière un ingress mTLS (`internal/adminapi/api.go` :
  scheme « mTLS + operator bearer ») — matérialiser le mTLS ici.
- **`ctx7`** avant toute API `crypto/tls` avancée / config TLS de `grpc` (credentials) / `coder/websocket` TLS.
- Certs/clés/CA fournis par config ou secrets, jamais commités ; rotation possible.
- Ne pas casser les tests d'intégration : TLS activable par config (off en test unitaire, on en prod).

## Tests (écrits dans la même PR)
- Handshake TLS/mTLS réussi ; un client sans cert client est rejeté sur les endpoints mTLS.
- SMPP-TLS : bind chiffré établi (faux SMSC/simulateur).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] TLS/SMPP-TLS/mTLS activables par config ; aucun secret en dur

## Hors périmètre
Auth opérateur réelle (OIDC) → step-206. Manifests k8s → step-207.
