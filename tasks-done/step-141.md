# step-141 — Repos Postgres billing : balances, config, grand livre partitionné

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-140 · **Bloque :** step-142, step-144

## But
Exposer l'autorité durable des soldes : lecture/écriture de `balances`, lecture de la config
`billing_customers`, et append idempotent dans le grand livre `billing_ledger` (partitionné par jour).

## Périmètre (ce que fait CETTE PR)
- `internal/storage/postgres/billing.go` + requêtes sqlc sous `internal/storage/postgres/queries/`.
- Lecture `balances` par `(owner_type, owner_id, direction)` ; lecture `billing_customers`.
- Append `billing_ledger` (`entry_type` reserve/capture/release/refund/topup/adjustment), signé,
  `balance_after` calculé, `message_id` porté.
- Vérification d'existence pré-capture dans le grand livre (garde d'idempotence cross-partition, §6.9).

## Points d'implémentation clés
- **SQL toujours paramétré** (sqlc), jamais de concaténation. Tables déjà créées par `migrations/0001_init`
  (aucune migration nouvelle ici) — voir `db/schema_passerelle_sms.sql` §22–24.
- Rappel MEMORY : sqlc doit **schéma-qualifier** (`control_plane.balances`).
- `billing_ledger` est **PARTITIONED BY RANGE (created_at)** : l'index unique
  `billing_ledger_idem_idx` est un *backstop même-jour*, PAS l'autorité cross-partition. L'autorité
  d'idempotence est Redis + le check d'existence ici (documenté §24 du schéma). Ne jamais s'y fier seul
  à une frontière de jour.
- Grand livre **append-only** : jamais d'UPDATE d'une ligne existante.

## Tests (écrits dans la même PR)
- Intégration (`testcontainers` Postgres) : append reserve→capture, lecture solde, existence pré-capture.
- Rappel MEMORY : `DOCKER_HOST` OrbStack sinon les tests d'intégration *skippent*.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] append-only vérifié ; check d'existence pré-capture testé

## Hors périmètre
Logique atomique reserve/capture/release (Lua Redis) → step-142. Cache/réhydratation → step-142.
