# step-148 — Admin billing : config client, soldes, top-up/transfert, change-balance-scope

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-141 · **Bloque :** —

## But
Donner aux opérateurs le contrôle de la facturation d'un client : lire/mettre à jour la config,
consulter les soldes MT/MO, créditer et transférer, changer le périmètre de solde (avec garde).

## Périmètre (ce que fait CETTE PR)
- `api/openapi-admin.yaml` : déclarer d'abord `get/update-customer-billing`, `get-customer-balances`,
  `topup-balance`, `transfer-balance`, `change-balance-scope`.
- `internal/adminapi/billing.go` : handlers chi+huma conformant au contrat, sur les repos step-141.
- `api/collections/admin-api.yaml` synchronisé (rappel MEMORY : test bloquant de sync).

## Points d'implémentation clés
- **Déclarer dans le contrat d'abord**, implémenter pour conformer (recette CLAUDE.md « Ajouter un
  endpoint Admin »).
- `change-balance-scope` refusé (`409`) si un solde ≠ 0 (critère d'acceptation §13) — vérifier MT **et** MO.
- Top-up/transfert écrivent une entrée de grand livre (`topup`/`adjustment`) via step-141 ; jamais un
  UPDATE nu du solde hors Lua/ledger.
- Modèle d'erreur plat `{code,message,errors[]}` (huma) ; scopes opérateur requis via le middleware auth.
- Le corps de message n'apparaît nulle part ici (invariant a).

## Tests (écrits dans la même PR)
- Contrat : la spec générée conforme `api/openapi-admin.yaml` (test de contrat existant).
- `change-balance-scope` avec solde ≠ 0 → `409`.
- Top-up → solde crédité + entrée de grand livre.
- Sync collection Admin.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] `409` sur scope avec solde ≠ 0 ; collection synchronisée

## Hors périmètre
Grand livre, rate-plans, providers → step-149. WS `stream-billing-alerts` → step-184 (M11).
