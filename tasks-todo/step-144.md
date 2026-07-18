# step-144 — Câbler billing-svc (gRPC :7001) + port ops

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-142, step-143 · **Bloque :** step-145, step-146, step-147

## But
Exposer le cœur billing derrière le serveur gRPC `billing-svc` sur le port métier 7001, avec le port
ops commun (`/metrics`, `/healthz`, `/readyz`) et une readiness reflétant les dépendances vitales.

## Périmètre (ce que fait CETTE PR)
- `cmd/billing-svc/main.go` : serveur gRPC (port 7001, §1.4) implémentant le service `Billing`
  (step-140) sur le cœur `internal/billing` (step-142/143).
- Handlers : `Reserve`, `Capture`, `Release`, `GetBalances`, `RecordMO`.
- `OpsServer` (port 9090) avec `ReadinessCheck` Redis + Postgres (via `observability.PingCheck`).
- Câblage config (`internal/config`), supervision (`internal/platform/supervisor`), tracing/logging.

## Points d'implémentation clés
- `context.Context` en 1er paramètre partout ; aucune goroutine sans condition d'arrêt (CLAUDE.md).
- Readiness = politiques de panne (§1.5) : Redis **et** Postgres sont vitaux pour billing (le solde ne
  peut être ni servi ni réhydraté sans eux) → `/readyz` échoue si l'un tombe.
- **`ctx7`** avant l'API serveur `google.golang.org/grpc` (enregistrement, interceptors, graceful stop).
- Erreurs gRPC mappées sur le modèle plat partagé (`code`) — cohérent avec `internal/platform/errors`.
- Le corps ne circule pas ici (invariant a) : billing ne voit que des identifiants et des crédits.

## Tests (écrits dans la même PR)
- Intégration : démarrer le serveur, appeler `Reserve`/`Capture` en client gRPC, vérifier soldes/ledger.
- `/readyz` : Redis coupé → not ready ; Postgres coupé → not ready.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] readiness reflète Redis+Postgres ; graceful stop du serveur gRPC

## Hors périmètre
Intégration router (step-145), connector-pool (step-146), adaptateur externe (step-147), Admin (step-148/149).
