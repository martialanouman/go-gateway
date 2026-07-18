# step-100 — Repo `exact_routes` (numéros exacts, portabilité)

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-101, step-102, step-103

## But
Poser la couche de persistance des routes par numéro exact (numéros portés / MNP), socle du court-circuit L0 de résolution.

## Périmètre (ce que fait CETTE PR)
- `internal/storage/postgres/queries/exact_routes.sql` (sqlc) : upsert par `msisdn`, get, delete, list paginé, bulk-insert (import).
- `internal/storage/postgres/exact_routes.go` : repo (`Get/List/Upsert/Delete/BulkUpsert`).
- `internal/routing/exact/` (naissance du paquet) : type `Target{Type, ID}` (`connector|route`) mappant `db/schema_passerelle_sms.sql` §19.

## Points d'implémentation clés
- Clé primaire `msisdn` E.164 ; `target_type ∈ {connector,route}`, `target_id` polymorphe (pas de FK unique).
- SQL paramétré, schéma-qualifier `control_plane.exact_routes` (mémoire projet sqlc).
- **API pgx v5 / sqlc via `ctx7`** si besoin (bulk insert `CopyFrom`).

## Tests (écrits dans la même PR)
- Intégration Postgres (`testcontainers-go`) : upsert idempotent par `msisdn`, list paginé, bulk-upsert de N lignes.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] upsert par `msisdn` idempotent

## Hors périmètre
Bloom + Redis + court-circuit L0 → step-101. Admin exact-routes → step-102/103.
