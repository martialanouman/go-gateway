# step-021 — Registre de sessions Redis (bind/unbind/lookup atomiques, max_sessions)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-022, step-024

## But
Implémenter le registre Redis faisant autorité pour les sessions SMPP : table `account → {pod_id, bind_id}[]`, avec application atomique de `max_sessions` au bind. C'est le socle de l'**invariant (d)**.

## Périmètre (ce que fait CETTE PR)
- Créer `internal/session` : type `Registry` sur `go-redis`, méthodes `Bind`, `Unbind`, `Lookup`, `Touch` (rafraîchit le TTL sur `enquire_link`).
- Scripts **Lua atomiques** (§règle d'or Redis) : `bind` vérifie le quota et insère en une opération (refus si `count >= max_sessions`) ; `unbind` retire le jeton ; TTL par session (expiration = perte de supervision `enquire_link`).
- Clés Redis : `sess:{account_id}` (set des `{pod_id}:{bind_id}`), TTL par membre via clé compagnon ou sorted-set horodaté.

## Points d'implémentation clés
- **`ctx7` avant d'utiliser** `github.com/redis/go-redis/v9` (§1.2) : API `EvalSha`/`ScriptLoad`, options de pipeline.
- **Jamais de read-modify-write côté Go** : le compteur + insertion + comparaison à `max_sessions` sont un seul script Lua (règle d'or).
- `max_sessions` est passé en argument au script (valeur lue depuis `control_plane.smpp_accounts` par l'appelant, step-024) ; le registre ne connaît pas PostgreSQL.
- Expiration : un bind dont l'`enquire_link` cesse doit libérer son jeton → TTL rafraîchi par `Touch`, balayage paresseux au `Bind`/`Lookup`.
- `Bind` renvoie une erreur sentinelle mappable sur `errs.ErrMaxSessionsExceeded` (`max_sessions_exceeded`) quand le quota est atteint.

## Tests (écrits dans la même PR)
- Intégration `testcontainers-go` Redis : bind jusqu'à `max_sessions` OK, le suivant refusé (**invariant (d)** au niveau registre).
- `unbind` libère un jeton → un nouveau bind repasse.
- Expiration TTL → jeton libéré (test avec TTL court).
- Concurrence : N binds parallèles sur `max_sessions=1` → exactement 1 succès (`go test -race`).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] atomicité du quota prouvée sous concurrence

## Hors périmètre
Exposition gRPC du registre (step-022) ; auth du bind et lecture `max_sessions` en base (step-024).
