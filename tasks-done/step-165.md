# step-165 — Rétention & tiering par drop de partition (§6.14)

> **Jalon :** M10 (§14 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-162 · **Bloque :** —

## But
Mettre en place la rétention du CDR par partitions quotidiennes, TTL, et archivage froid, avec purge
**par drop de partition** (jamais `DELETE WHERE`), et un `content_retention_days` découplé de la
rétention CDR.

## Périmètre (ce que fait CETTE PR)
- `migrations/clickhouse/` : ajuster/confirmer le TTL et le partitionnement quotidien du CDR
  (`PARTITION BY toDate(submitted_at)`, §Appendice A). Rappel MEMORY : **une instruction par fichier**
  de migration ClickHouse.
- `internal/storage/clickhouse/` : purge par `DROP PARTITION` à l'échéance ; interface de tiering
  (archive Parquet) avec impl locale.
- `content_retention_days` découplé : le corps a un TTL plus court que le CDR (§1.10, §6.14).

## Points d'implémentation clés
- **Purge par drop de partition, PAS `DELETE WHERE`** (critère §14) : infaisable à 8000 msg/s autrement.
- Tiering : détacher/archiver les vieilles partitions vers du Parquet ; le bucket objet réel est
  **infra/hors périmètre** (§14) — l'impl du tiering est fournie, la destination est branchable.
- `billing_ledger` (Postgres) suit le même principe côté partitions jour (step-141 : détachement) — noté
  mais l'ordonnanceur pg_partman/cron reste opérationnel.
- **`ctx7`** avant toute API `clickhouse-go/v2` de gestion de partitions.

## Tests (écrits dans la même PR)
- Intégration ClickHouse : une partition échue est droppée ; les partitions actives restent.
- `content_retention_days` < rétention CDR : le corps expire avant la métadonnée.
- Tiering : une partition détachée est archivée (destination locale de test).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] purge par drop de partition (aucun `DELETE WHERE`) ; migration ClickHouse mono-instruction

## Hors périmètre
Bucket objet froid réel (infra). Effacement RGPD → step-166.
