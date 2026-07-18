# step-128 — Admin connecteurs : rebind / status / reconnect-policy / bind-pool

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-124, step-127 · **Bloque :** —

## But
Exposer le pilotage opérationnel des connecteurs dans l'Admin API : forcer un rebind, lire l'état vivant, changer la politique de reconnexion et la taille du pool de binds.

## Périmètre (ce que fait CETTE PR)
- `internal/adminapi/connectors.go` : handlers `rebind-connector`, `get-connector-status`, `set-connector-reconnect-policy`, `set-connector-bind-pool` (`api/openapi-admin.yaml` L564-591, déjà déclarés).
- `get-connector-status` renvoie `link_status` **et** `breaker_state` **séparés** (step-121/127), plus le nombre de sous-binds vivants.
- `set-connector-bind-pool` et `set-connector-reconnect-policy` persistent + déclenchent la reconfiguration (config-sync / rechargement du pool step-124).
- Étendre la surface contrat + collection admin.

## Points d'implémentation clés
- **Implémente pour conformer** `api/openapi-admin.yaml`. Modèle d'erreur plat.
- `bind_pool_size` validé 1..32 (schéma) ; changement à chaud → drainage propre des binds retirés.
- Ne jamais conflater `link_status` et `breaker_state` dans la réponse (§12).

## Tests (écrits dans la même PR)
- Contrat : les 4 opérations conforment le schéma.
- `get-connector-status` expose les deux états distinctement.
- `set-connector-bind-pool` de 1→4 → 4 binds actifs (intégration).
- Collection admin re-synchronisée (test bloquant).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] surface contrat + collection admin à jour ; états distincts exposés

## Hors périmètre
Dead-letter + retraitement → step-129.
