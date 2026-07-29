# step-142b — Config de facturation (floor overdraft/postpaid) + TTL du cache de solde

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-142 · **Bloque :** step-145, step-146

## But
step-142 a livré le cœur MT reserve/capture/release en **prepaid strict (floor 0)**. Cette étape câble
la **configuration de facturation par client** (`control_plane.billing_customers`, step-141) au plancher
utilisé par `reserve.lua`, et **borne la durée de vie du cache de solde** pour que toute divergence
cache/durable s'auto-guérisse.

## Périmètre (ce que fait CETTE PR)
- **Mapping `BillingCustomer` → floor** : `prepaid` sans overdraft → floor 0 ; `prepaid` + `overdraft_enabled`
  → floor `-overdraft_limit` ; `postpaid` + `credit_limit_is_hard` → floor `-credit_limit` (limite dure) ;
  `postpaid` soft → pas de plancher (`has_floor=0`). Alimente les ARGV `has_floor`/`floor` déjà plombés
  dans `reserve.lua`.
- **Cache de config** : éviter une lecture Postgres de `billing_customers` par message à 8000/s (cache en
  mémoire avec invalidation, ou push via config-sync). Contrôle booléen « facturation activée » en cache
  (CLAUDE.md : désactivée = zéro appel réseau).
- **TTL borné sur le cache de solde** `billing:balance:{...}` : aujourd'hui écrit sans TTL
  (`SetNX(..., 0)`), donc toute divergence cache/durable est **permanente**. Poser un TTL borné pour
  auto-guérison (la réhydratation depuis le durable est cohérente car `RecordDurable(reserve)` est
  synchrone → le durable reflète déjà les holds). Envisager un `SET` conditionnel/versionné au lieu de
  `SetNX` si le TTL seul ne suffit pas. Voir la note mémoire `billing-cache-durable-divergence-142b` et
  les commentaires « step-142b » dans `internal/billing/billing.go`.

## Points d'implémentation clés
- Le trap `nil ≠ 0` (mémoire `ratelimit-token-bucket`) : un `overdraft_limit`/`credit_limit` NULL ≠ 0.
- Réutiliser `reserve.lua` tel quel (ARGV `has_floor`/`floor` déjà en place) — pas de nouveau script.
- **`ctx7`** avant toute API `go-redis/v9`/pgx utilisée.

## Tests (écrits dans la même PR)
- Overdraft : un `reserve` peut descendre le solde jusqu'à `-overdraft_limit`, refusé au-delà.
- Postpaid limite dure vs soft : plafond appliqué / absence de plancher.
- **Drift/expiry** : après expiration du TTL du cache de solde, une divergence artificielle cache/durable
  est corrigée par réhydratation ; le solde durable reste l'autorité.
- Cache de config : une mise à jour de `billing_customers` est prise en compte (invalidation).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] mapping floor couvert par tests (prepaid/overdraft/postpaid dur/soft)
- [ ] TTL du cache de solde posé + test de drift/expiry ; aucune divergence permanente possible
- [ ] godoc sur l'exporté ; aucun invariant violé

## Hors périmètre
Compteur MO → step-143. Serveur gRPC → step-144. Intégration pipeline → step-145/146.
