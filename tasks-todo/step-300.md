# step-300 — TLS / SMPP-TLS / mTLS sur les transports

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — (step-193c livrée) · **Bloque :** step-310

## But
Chiffrer et authentifier les transports : TLS sur les APIs HTTP, SMPP-TLS sur les binds, mTLS entre
services internes (dont l'Admin API et le gRPC billing).

## Périmètre (ce que fait CETTE PR)
- HTTP : TLS sur `rest-api-svc` (public) et mTLS sur `admin-api-svc` (interne).
- SMPP : option SMPP-TLS pour `smpp-server-svc` (entrant) et `connector-pool-svc` (sortant).
- gRPC : mTLS pour `billing-svc`, **`content-key-svc` et `session-manager-svc`** — nommés, pas
  « et futurs services » : les sept clients gRPC du dépôt sont aujourd'hui construits avec
  `insecure.NewCredentials()`.
- Config TLS (certs/clés/CA) via `internal/config`, jamais de secret en dur.

## Points d'implémentation clés
- L'Admin API est **interne** et déjà pensée derrière un ingress mTLS (`internal/adminapi/api.go` :
  scheme « mTLS + operator bearer ») — matérialiser le mTLS ici.
- **Prérequis levé (step-193c).** Les dix services ont désormais leur `wiring.go`/`wiring_test.go` :
  `billing-svc` (cible du mTLS gRPC) et `rest-api-svc` (cible du TLS public) inclus. Un handshake TLS
  qui échoue au boot doit donc remonter **en valeur** depuis `newXxxApp`, jamais en `log.Fatal`, et son
  test de câblage l'exige déjà — c'est le point d'accroche à utiliser, pas à réinventer.
- **`ctx7`** avant toute API `crypto/tls` avancée / config TLS de `grpc` (credentials) / `coder/websocket` TLS.
- Certs/clés/CA fournis par config ou secrets, jamais commités ; rotation possible.
- Ne pas casser les tests d'intégration : TLS activable par config (off en test unitaire, on en prod).

## Ajouté par step-290d — la DEK circule en clair, sans authentification

ADR-0011 fait de `content-key-svc` le **seul détenteur de la KMS**, avec une surface volontairement
minimale pour que le dépositaire de la clé reste auditable. Cette surface est un gRPC que
`admin-api-svc` et `router-svc` appellent avec `insecure.NewCredentials()`
(`cmd/admin-api-svc/wiring.go`, `cmd/router-svc/wiring.go`).

Conséquence : la **clé de données** d'un client voyage en clair sur le réseau, et le service ne vérifie
pas qui la demande. Le chiffrement du contenu au repos (step-162) protège contre un vol de base ; il ne
protège de rien contre qui écoute ce lien ou sait l'appeler. C'est le lien le plus sensible du dépôt, et
c'est celui que le périmètre de cette step ne nommait pas.

**Ce que cette step doit donc faire :** mTLS sur `content-key-svc` avec une **liste d'appelants
autorisés** (le certificat client identifie le service), pas seulement un tunnel chiffré. Un tunnel sans
autorisation laisse n'importe quel pod du cluster demander n'importe quelle clé.

## Tests (écrits dans la même PR)
- Handshake TLS/mTLS réussi ; un client sans cert client est rejeté sur les endpoints mTLS.
- SMPP-TLS : bind chiffré établi (faux SMSC/simulateur).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] TLS/SMPP-TLS/mTLS activables par config ; aucun secret en dur

## Hors périmètre
Auth opérateur réelle (OIDC) → step-310. Manifests k8s → step-270.
