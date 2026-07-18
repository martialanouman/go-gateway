# step-149 — Admin billing : grand livre, rate-plans, providers externes

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-148, step-147 · **Bloque :** —

## But
Compléter la surface Admin de facturation : consultation du grand livre, gestion des plans tarifaires
et des fournisseurs de facturation externes (dont un test de connectivité).

## Périmètre (ce que fait CETTE PR)
- `api/openapi-admin.yaml` : `get-billing-ledger`, `create/get/update/delete-rate-plan`,
  `create/get/update/delete-billing-provider`, `test-billing-provider`.
- `internal/adminapi/billing.go` (ou fichier dédié) : handlers conformant au contrat.
- Repos `rate_plans` / `external_billing_providers` (sqlc) si absents.
- `api/collections/admin-api.yaml` synchronisé.

## Points d'implémentation clés
- `get-billing-ledger` : lecture paginée (`billing_ledger_customer_idx`), filtrée par client/plage de
  dates ; jamais de corps de message (le ledger ne porte que `message_id` + crédits).
- `test-billing-provider` : appel de sonde vers le fournisseur (mode `balance_check`) sans consommer.
- Plans tarifaires : crédits **par segment**, jamais monétaire (§3.1 du schéma).
- Contrat d'abord, puis implémenter pour conformer.

## Tests (écrits dans la même PR)
- Contrat conforme ; `get-billing-ledger` paginé ; CRUD rate-plan/provider ; `test-billing-provider`.
- Sync collection Admin.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] collection synchronisée ; pagination du ledger testée

## Hors périmètre
Transport temps réel des alertes → M11. Fin de M9.
