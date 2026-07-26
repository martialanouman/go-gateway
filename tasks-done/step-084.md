# step-084 — Token-bucket Lua atomique + repo `rate_limits`

> **Jalon :** M6 (§10 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-080 · **Bloque :** step-085, step-086, step-087

## But
Fournir un limiteur de débit token-bucket **atomique en Lua** (`EVALSHA`) par entité (compte / connecteur / route) et le repo qui lit la config `rate_limits`. Cœur de la protection des connecteurs.

## Périmètre (ce que fait CETTE PR)
- `internal/pipeline/ratelimit/bucket.lua` + `internal/pipeline/ratelimit/ratelimit.go` : un `Script` (socle step-080) implémentant le token-bucket sur `ratelimit:{entity_type}:{entity_id}:{window}` (clé §Appendix B). Renvoie `allow|deny` + tokens restants.
- Repo `rate_limits` : `internal/storage/postgres/queries/rate_limits.sql` (sqlc) + `internal/storage/postgres/rate_limits.go` (List/Get par `(entity_type, entity_id)`).
- Type de résultat + code d'erreur de dépassement (réutiliser le code d'extension `429`/throttle du modèle d'erreur plat).

## Points d'implémentation clés
- **Atomicité obligatoire en Lua** : reflux/consommation de jetons dans le script, jamais de read-modify-write Go (règle d'or CLAUDE.md).
- **Fail-closed** : si Redis est injoignable, appliquer un **plafond technique statique local** (dérivé de `throughput_limit_per_sec`), jamais fail-open (§10). Distinguer erreur Redis vs `deny`.
- Coût = **nombre de segments** (fourni par step-082), pas 1 par message.
- Contrats : `rate_limits` (`db/schema_passerelle_sms.sql` §13), `throughput_limit_per_sec` (§9). Schéma-qualifier `control_plane.rate_limits` (sqlc, mémoire projet).
- **API sqlc/pgx v5 via `ctx7`** si signatures incertaines.

## Tests (écrits dans la même PR)
- **Concurrence `-race`** : N goroutines consomment ; la somme autorisée ≤ capacité (atomicité prouvée).
- Recharge des jetons dans le temps (horloge du script) ; burst respecté.
- Fail-closed : client Redis en panne simulée → plafond statique appliqué, pas d'ouverture.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] limite appliquée atomiquement sous concurrence ; fail-closed vérifié

## Hors périmètre
L'intégration dans l'ordre du pipeline + précédence connecteur ≥ route → step-085. L'AIMD → step-086.
