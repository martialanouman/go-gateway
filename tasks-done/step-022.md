# step-022 — session-manager-svc : serveur gRPC SessionRegistry (:7000)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-020, step-021 · **Bloque :** step-024, step-046

## But
Exposer le registre Redis (step-021) comme service gRPC inter-pods `SessionRegistry` sur `:7000`, avec le port ops commun `:9090`.

## Périmètre (ce que fait CETTE PR)
- Créer `cmd/session-manager-svc/main.go` : serveur gRPC `:7000` (§1.4), port ops `:9090` (`/metrics`, `/healthz`, `/readyz`).
- Implémenter `SessionRegistry` (`internal/session/grpc` ou `internal/session`) : `Bind`/`Unbind`/`Lookup` délèguent au `Registry` ; `Deliver` route le PDU vers le pod détenteur (résolu par `Lookup`).
- Config via `caarlos0/env/v11` (adresse Redis, ports).
- `readyz` = Redis joignable (dépendance vitale de ce service, §1.5).

## Points d'implémentation clés
- **`ctx7` avant** d'utiliser l'API serveur `google.golang.org/grpc` (interceptors, `grpc.NewServer`, arrêt gracieux).
- `Deliver` : le manager ne parle pas SMPP ; il relaie le `bytes pdu` au pod hôte du `bind_id`. En mono-pod (tests M3), l'hôte est local ; le routage inter-pods réel s'appuie sur `pod_id` (round-robin des binds vivants côté appelant, arrive à step-048).
- Arrêt propre : `context.Context` en 1er paramètre, aucune goroutine sans condition d'arrêt (règle d'or).
- Erreurs gRPC portant le `code` partagé (`max_sessions_exceeded`) via metadata/status pour que l'appelant SMPP le retraduise en `ESME_RBINDFAIL`.

## Tests (écrits dans la même PR)
- Intégration : client gRPC `Bind`/`Lookup`/`Unbind` de bout en bout contre un Redis testcontainers.
- `Bind` au-delà de `max_sessions` → status gRPC portant `max_sessions_exceeded`.
- `readyz` rouge si Redis coupé.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] service démarrable via `make run SVC=session-manager-svc`

## Hors périmètre
Consommateur SMPP du registre (step-024) ; remise MO via `Deliver` (step-046, step-048).
