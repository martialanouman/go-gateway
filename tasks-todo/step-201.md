# step-201 — Tuning de débit : partitions Kafka, batch ClickHouse, pool pgx

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-200 · **Bloque :** —

## But
Atteindre le débit soutenu (8000 SMS/s) en réglant les leviers de capacité : partitions Kafka, taille
de batch ClickHouse, dimensionnement du pool `pgx` — pilotés par config, mesurés par le harnais de charge.

## Périmètre (ce que fait CETTE PR)
- `internal/config` : exposer les leviers (partitions par topic, batch/flush ClickHouse, `pgxpool`
  max/min conns) via env, avec des valeurs par défaut de prod raisonnées.
- Ajustements côté `internal/storage/{kafka,clickhouse,postgres}` pour honorer ces leviers.
- Re-run du harnais (step-200) documentant les réglages tenant les NFR.

## Points d'implémentation clés
- `mt.routed` est shardé (`shard_index = hash(message_key) % bind_pool_size`, §1.6) : le nombre de
  partitions doit couvrir le parallélisme cible sans surdécoupage.
- ClickHouse : batch/flush pour éviter la mutation par message (§1.10) — équilibrer latence CDR vs débit.
- `pgxpool` : dimensionner selon la charge billing/contrôle ; éviter l'épuisement sous pic.
- **`ctx7`** avant d'ajuster une API `franz-go` / `clickhouse-go/v2` / `pgxpool`.
- Aucun réglage ne doit affaiblir un invariant (idempotence, ordre, non-fuite).

## Tests (écrits dans la même PR)
- Config : les leviers se parsent et s'appliquent (test unitaire config).
- Le harnais (step-200) tient le débit soutenu avec les réglages (run de référence documenté).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] débit soutenu tenu, budgets de latence respectés (disjoncteur fermé)

## Hors périmètre
Chaos → step-202/203. Sécurité → step-204+.
