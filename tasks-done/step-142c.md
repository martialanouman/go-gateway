# step-142c — Interdire overdraft/hard limit quand balance_scope=smpp_account

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-142b · **Bloque :** step-145

## But
Fermer le trou monétaire relevé à la revue de step-142b : un découvert/limite dure défini au niveau
CLIENT s'appliquerait à CHAQUE solde par-compte quand `balance_scope=smpp_account`, multipliant
l'exposition de crédit par le nombre de comptes. **Décision produit (utilisateur) : interdire
`overdraft`/`hard limit` quand `balance_scope=smpp_account`.** Un solde par-compte ne peut donc être
que prépayé strict (floor 0) ou postpayé soft (advisory, ne bloque pas) — tous deux sûrs par compte.

## Périmètre (ce que fait CETTE PR)
- **Garde-fou DB (autorité) :**
  - `customers` : `CHECK (balance_scope <> 'smpp_account' OR NOT overdraft_enabled)` — la config réellement
    éditée par l'admin (overdraft + balance_scope vivent dans cette table). Mappé 422 par `translate`.
  - `billing_customers` : trigger interdisant `overdraft_enabled` OU `credit_limit_is_hard` quand le client
    est account-scoped (RAISE `ERRCODE '23514'` → 422). Protège la table-source du floor (step-142b) et
    future-proof `credit_limit_is_hard` (seul endroit où il vit).
- **Retirer l'override money-safe de step-142b** dans `internal/billing/billing.go` : forcer toute balance
  par-compte à floor 0 est désormais REDONDANT (la DB garantit l'absence d'overdraft/hard-limit) et
  INCORRECT pour un postpayé soft par-compte (qui ne doit jamais bloquer). Le floor redevient piloté par
  la config.

## Points d'implémentation clés
- Le trigger lit `customers.balance_scope` via la FK `billing_customers.customer_id`. Lever avec
  `USING ERRCODE = '23514'` pour un 422 propre (P0001 par défaut → 500).
- Ne PAS toucher au changement de `balance_scope` (verrouillé à soldes=0, endpoint dédié à venir).

## Tests (écrits dans la même PR)
- `customers` : créer/mettre à jour un client `balance_scope=smpp_account` + `overdraft_enabled` → rejet 422.
- `billing_customers` : insérer overdraft/hard-limit pour un client account-scoped → rejet.
- Billing : un account-scoped **soft postpaid** ne bloque PAS (preuve que le retrait de l'override est correct).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] interdiction DB couverte par tests (customers + billing_customers) ; override retiré ; soft postpaid par-compte ne bloque pas
- [ ] migration up/down réversible ; schéma `db/schema_passerelle_sms.sql` à jour ; godoc/commentaires

## Hors périmètre
Le décrochage `customers` vs `billing_customers` (le floor lit une table jamais peuplée) → à trancher en
step-144/145 (câbler la source de config du floor). Compteur MO → step-143.
