# step-142d — Consolider la config de facturation sur `customers` (option A, ADR-0010)

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** FAIT
> **Dépend de :** step-142c · **Bloque :** step-145

## But
Résoudre le décrochage trouvé à la revue de step-142c : le floor de réservation (step-142b) lisait
`control_plane.billing_customers`, que **rien ne peuple**, tandis que l'admin écrit la config billing sur
`control_plane.customers`. Le floor lisait donc une table morte (tout client en prépayé strict).
**Décision produit : option A — `customers` devient la source unique de vérité, `billing_customers` est
supprimée.**

## Périmètre (ce que fait CETTE PR)
- Migration 0008 : ALTER `customers` + `credit_limit`, `credit_limit_is_hard`,
  `external_billing_provider_id` (+ CHECK hard-limit-needs-value) ; le ban account-scope devient **un
  CHECK mono-table** (overdraft ET hard-limit) ; suppression des 2 triggers cross-table de step-142c ;
  suppression de la table `billing_customers`. up/down réversibles.
- Floor : `GetBillingCustomer`/`ListBillingCustomers` lisent `customers` (les `billing_enabled` pour le
  snapshot). `billing_mode` NULL → prépayé strict.
- `cp.Customer`/`NewCustomer`/`CustomerPatch` + repos : nouvelles colonnes câblées (Create + Update).
- Admin + contrat : `credit_limit`/`credit_limit_is_hard` sur `Customer`/`CustomerCreate`/`CustomerUpdate` ;
  `api/openapi-admin.yaml` 1.1.0 → 1.2.0 (additif non-rupturant).
- ADR-0010 acte la divergence avec l'esquisse « table séparée » de la spec.

## Tests
- Ban account-scope via le CHECK mono-table : create overdraft, create hard-limit, flip `balance_scope`.
- Config chargée depuis `customers` + re-rebuild (invalidation) ; postpaid soft par-compte ne bloque pas.
- Lecture repo (`BillingCustomer`) depuis `customers`.

## Definition of Done
- [x] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck · `make contracts` verts
- [x] floor lit `customers` ; ban mono-table couvre toutes les directions ; migration up/down réversible
- [x] ADR-0010 ; mémoire `billing-customers-vs-customers-config-disconnect` → RÉSOLUE

## Hors périmètre
Endpoint admin `/admin/customers/{id}/billing` (non implémenté) — s'adossera aux colonnes de `customers`.
`external_billing_provider_id` non exposé à l'admin (step-148, adaptateur externe §6.10). Compteur MO →
step-143.
