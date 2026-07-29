# ADR-0010 : Config de facturation consolidée sur `customers` (suppression de `billing_customers`)

**Status:** Accepted
**Date:** 2026-07-29
**Deciders:** Équipe plateforme
**Réf spec:** §6.9 ; step-142d (suite de 142a/b/c)

## Context

La spec technique esquissait la config de facturation MT sur **deux tables** : `customers`
(`billing_mode`, `overdraft_enabled`, `overdraft_limit`, `balance_scope`, `billing_enabled`,
`mo_billing_floor`) et `billing_customers` (les mêmes + `credit_limit`, `credit_limit_is_hard`,
`external_billing_provider_id`), cette dernière « possédée par billing-svc » (spec §6.9, l.523).

À l'implémentation (step-141/142), le **floor de réservation** (step-142b) lisait `billing_customers`,
mais **aucun code n'écrit jamais cette table** : l'Admin API écrit la config sur `customers`. Résultat :
le floor lisait une table vide → tout client était en prépayé strict, et l'overdraft configuré par l'admin
n'atteignait jamais le moteur de réservation. `credit_limit`/`credit_limit_is_hard` (postpayé) n'avaient
même aucun chemin d'écriture. La redondance des colonnes entre les deux tables créait en plus un risque de
dérive permanent (le garde-fou account-scope de step-142c a dû poser des triggers cross-table uniquement à
cause de cette séparation).

## Decision

**`customers` est l'unique source de vérité de la config de facturation MT.**

- On déplace sur `customers` les seuls champs floor qui lui manquaient : `credit_limit`,
  `credit_limit_is_hard`, `external_billing_provider_id`.
- Le floor (`ListBillingCustomers`/`GetBillingCustomer`, `ConfigSnapshot` de step-142b) lit désormais
  `customers` (les clients `billing_enabled` pour le snapshot).
- On **supprime la table `billing_customers`** (migration 0008) et, avec elle, les deux triggers
  cross-table de step-142c : `credit_limit_is_hard` vivant désormais sur `customers`, le ban account-scope
  redevient **un seul CHECK mono-table** (`customers_account_scope_no_credit_ck`), re-validé à chaque écriture
  de `customers` (y compris un flip de `balance_scope`).
- L'Admin expose `credit_limit`/`credit_limit_is_hard` sur `Customer`/`CustomerCreate`/`CustomerUpdate`
  (contrat `api/openapi-admin.yaml` 1.1.0 → 1.2.0, additif non-rupturant).

## Options Considered

### Option A : source unique = `customers` (retenue)
**Pros :** utilise la table déjà peuplée et éditée par l'admin ; supprime tout risque de dérive (une seule
source) ; le ban redevient un CHECK mono-table (plus de triggers) ; churn minimal (2-3 colonnes + repointage).
**Cons :** dévie de l'esquisse « table séparée possédée par billing-svc » de la spec (d'où cet ADR).

### Option B : garder les deux tables, synchroniser `customers` → `billing_customers`
**Cons :** perpétue la duplication ET ajoute un mécanisme de sync qui peut lui-même dériver ; `credit_limit`
finirait sur les deux tables. Le pire des deux mondes.

### Option C : consolider sur `billing_customers`, vider les champs billing de `customers`
**Pros :** isolation conceptuelle billing-svc conforme à l'ownership spec.
**Cons :** churn bien plus lourd (déplacer overdraft/billing_mode hors de `customers`, nouveaux endpoints
admin `/billing`, migration de données) ; le ban reste cross-table. L'isolation gagnée est théorique (un
seul Postgres `control_plane`).

## Consequences

- **Plus facile :** le floor lit la config réellement éditée ; un seul CHECK mono-table pour le ban
  account-scope ; plus de triggers plpgsql à maintenir ; plus de risque de dérive à deux tables.
- **Plus difficile / limites :** l'endpoint admin `/admin/customers/{id}/billing` (schéma `BillingCustomer`,
  **non implémenté**) devra, s'il est un jour construit, lire/écrire les colonnes de `customers` (vue
  billing du client) et non une table dédiée. `external_billing_provider_id` est déplacé mais non exposé à
  l'admin (surface = step-148, adaptateur externe §6.10).
- **Traçabilité :** `tasks-done/step-142d.md`, migration `0008_billing_config_on_customers`, et la note
  mémoire `billing-customers-vs-customers-config-disconnect` (résolue) renvoient à cet ADR. La spec §6.9
  sera amendée pour refléter la table unique.
