# step-142 — Réserve/capture/libère MT en Lua atomique, idempotent par message_id

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-141 · **Bloque :** step-144, step-145, step-146

## But
Le cœur de la facturation MT : réserver, capturer et libérer du crédit de façon **atomique en Lua**
sur Redis, **idempotent par `message_id`** (invariant c), avec réhydratation du cache depuis le grand
livre Postgres (autorité durable) et fail-closed strict pendant la fenêtre de chargement.

## Périmètre (ce que fait CETTE PR)
- `internal/billing/` : cœur MT (reserve/capture/release) + scripts Lua embarqués.
- Clés Redis (§Appendice B du schéma) : `billing:balance:{direction}:{owner_type}:{owner_id}` (cache de
  solde), `billing:reservation:{message_id}` (hold court-TTL, effacé à la capture/libération).
- Réhydratation du cache depuis `billing_ledger` (step-141) quand la clé de solde est absente.
- Écriture du grand livre après l'opération Redis (via step-141).

## Points d'implémentation clés
- **Opérations atomiques en Lua** (CLAUDE.md) : jamais de read-modify-write côté Go. Un seul script par
  opération, chargé via `EVALSHA`.
- **Idempotence (invariant c)** : reserve pose `billing:reservation:{message_id}` en `SET NX` ; un
  second reserve/capture du même `message_id` est un no-op (le script détecte le hold/l'entrée déjà
  posée). La capture consomme le hold ; une double capture ne débite qu'une fois.
- **Fail-closed** pendant la réhydratation : si le cache est froid et Postgres injoignable, refuser
  (ne jamais laisser passer un crédit non vérifié).
- Solde insuffisant → retour d'un code d'extension (mappé `402` côté REST) ; **aucune** entrée de grand
  livre dans ce cas. Ajouter le code `insufficient_balance` aux 3 endroits (recette CLAUDE.md :
  `internal/platform/errors`, champ `code` des deux `api/openapi-*.yaml`, §11.3 du guide).
- **`ctx7`** avant toute API `go-redis/v9` (chargement de script, `EVALSHA`).

## Tests (écrits dans la même PR)
- Unitaire/intégration (`testcontainers` Redis) : reserve→capture au succès ; reserve→release à l'échec.
- **Invariant (c)** : double `message_id` (reserve ×2, capture ×2) ne débite qu'une fois.
- Solde insuffisant : refus + zéro entrée de grand livre.
- Réhydratation : cache froid → chargé depuis le grand livre ; Postgres coupé → fail-closed.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (c)** couvert par un test bloquant
- [ ] tout Redis atomique en Lua ; fail-closed testé

## Hors périmètre
Compteur MO → step-143. Serveur gRPC → step-144. Intégration pipeline → step-145/146.
