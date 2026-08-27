# step-260 — Chaos : drain gracieux + PDB + binds préservés ; failover Postgres

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-250 · **Bloque :** —

## But
Prouver qu'un redémarrage de pods se fait **sans coupure des binds** (drain gracieux + PDB) et qu'un
failover Postgres se solde par une **réhydratation correcte des soldes**, sans perte de message.

## Périmètre (ce que fait CETTE PR)
- Drain gracieux : arrêt propre des services (unbind SMPP après cancel, offsets Kafka commités) —
  s'appuie sur `internal/platform/supervisor`.
- Chaos redémarrage de pods : binds SMPP préservés/rétablis, aucun message perdu.
- Failover Postgres : billing réhydrate les soldes depuis le grand livre (step-142), fail-closed pendant
  la fenêtre.

## Points d'implémentation clés
- Le drain SMPP existe déjà côté connector (`connectorpool.Run` détache l'unbind après cancel) — vérifier
  qu'il tient sous redémarrage orchestré et documenter le lien au PDB.
- **Binds préservés** (§16 critère) : rolling deploy sans coupure — s'assurer que le retrait LB
  (`/readyz`) précède l'arrêt.
- **Réhydratation solde** après failover : le cache Redis se recharge depuis Postgres ; aucune double
  facturation (invariant c) ni crédit fantôme.
- Aucune goroutine sans arrêt ; `go test -race`.

## Tests (écrits dans la même PR)
- Redémarrage orchestré : binds rétablis, zéro perte.
- Failover Postgres simulé : soldes réhydratés corrects ; fail-closed pendant la fenêtre.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (c)** tenu après failover
- [ ] rolling deploy sans coupure des binds (drain + PDB) prouvé

## Hors périmètre
Manifests k8s (PDB déclaré) → step-270. Sécurité → step-290+.
