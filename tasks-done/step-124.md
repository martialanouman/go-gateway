# step-124 — Pool de binds (`bind_pool_size > 1`) + partition par shard

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-128

## But
Monter le débit d'un connecteur avec plusieurs binds SMPP parallèles, en partitionnant `mt.routed` par `(connector_id, shard_index)` — tout en garantissant que les segments d'un même message empruntent **un seul bind**.

## Périmètre (ce que fait CETTE PR)
- `internal/connectorpool` : passer du bind unique (M2) à un **pool** de `bind_pool_size` binds (`db/schema_passerelle_sms.sql` §9, 1..32).
- Consommation partitionnée : `shard_index = hash(message_key) % bind_pool_size` (§1.6) → chaque shard servi par un bind dédié.
- Le producteur `mt.routed` (router) clé déjà `(connector_id, shard_index)` — vérifier/aligner la clé de partition.

## Points d'implémentation clés
- **Tous les segments d'un message partagent `message_key`** → même `shard_index` → même bind → **ordre préservé** (§7.3, critère d'acceptation M8).
- Chaque bind garde sa fenêtre `window_size` propre ; arrêt propre de chaque bind (`context`).
- `bind_pool_size` lu depuis le connecteur ; changement à chaud géré via l'Admin (step-128).

## Tests (écrits dans la même PR)
- `bind_pool_size=4` → débit agrégé supérieur au bind unique (test de débit).
- **Ordre** : les segments d'un message multipart arrivent sur un seul bind, dans l'ordre (test d'ordre, simulateur step-120).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] segments d'un message sur un seul bind ; débit accru à `bind_pool_size=4`

## Hors périmètre
Admin `set-connector-bind-pool` (rechargement) → step-128.
