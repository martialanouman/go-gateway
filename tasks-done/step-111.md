# step-111 — Admin routing-scripts : CRUD + list-versions

> **Jalon :** M7 (§11 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-107 · **Bloque :** step-112

## But
Exposer la gestion des scripts de routage dans l'Admin API (création brouillon, lecture, mise à jour, suppression, historique des versions).

## Périmètre (ce que fait CETTE PR)
- `internal/adminapi/routing_scripts.go` + DTO : handlers `create-routing-script`, `get-routing-script`, `update-routing-script`, `delete-routing-script`, `list-routing-scripts`, `list-routing-script-versions` (`api/openapi-admin.yaml` L722-808).
- Étendre la surface d'opérations (`contract_test.go`) + collection admin (`api/collections/admin-api.yaml`).

## Points d'implémentation clés
- **Implémente pour conformer** `api/openapi-admin.yaml`. Modèle d'erreur plat.
- Création = statut `draft` par défaut ; `language ∈ {js,lua}`, `checksum` recalculé serveur.
- Validation des bornes `timeout_ms ≤ 20` avant persistance (step-107).
- **huma v2.38 quirks** (mémoire projet).

## Tests (écrits dans la même PR)
- Contrat : les 6 opérations conforment le schéma.
- create draft → get → update → list-versions ; delete.
- Collection admin re-synchronisée (test bloquant).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] surface contrat + collection admin à jour

## Hors périmètre
assign / validate / test / publish (exécution) → step-112.
