# step-031 — En-tête Idempotency-Key (REST, fenêtre 24 h Redis)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** FAIT
> **Dépend de :** — · **Bloque :** —

## But
Rendre `POST /messages` idempotent : un rejeu avec la même `Idempotency-Key` (fenêtre 24 h) renvoie le résultat d'origine ; même clé + corps différent → `409 idempotency_conflict`.

## Périmètre (ce que fait CETTE PR)
- Ajouter le support de l'en-tête `Idempotency-Key` sur `submit-messages` (`internal/restapi/messages.go` + déclaration dans `api/openapi-public.yaml` si absente du paramètre).
- Store Redis : `idem:{account_id}:{key}` → `{body_hash, response, message_id}`, TTL 24 h, écriture atomique (`SET NX` / Lua) avant publication `mt.inbound`.
- Rejeu même clé + **même** corps (hash identique) → réponse d'origine, **un seul** message publié.
- Même clé + corps **différent** → `409 idempotency_conflict` (`errs.ErrIdempotencyConflict`, déjà défini).
- Course concurrente sur la même clé : seul le premier publie ; les autres attendent/renvoient le résultat.

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser `go-redis` (`SET` avec `NX`/`EX`, éventuel Lua pour la réservation + relecture atomiques — pas de read-modify-write, règle d'or).
- `body_hash` = hash déterministe du corps normalisé de la requête (SHA-256), comparé en temps constant si nécessaire.
- La réservation doit précéder l'ACK Kafka : garantir « un seul message publié » même sous double soumission simultanée.
- **Invariant (a)** : ne jamais stocker le texte en clair dans l'entrée d'idempotence ; stocker le hash et la réponse 202 (ids), pas le corps.

## Tests (écrits dans la même PR)
- Test de contrat public : paramètre `Idempotency-Key` conforme.
- Rejeu même clé+corps → même 202, `mt.inbound` publié une seule fois (vérifié via consumer de test).
- Même clé + corps différent → `409 idempotency_conflict`.
- Concurrence (`go test -race`) : N requêtes simultanées même clé → 1 publication.

## Definition of Done
- [x] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [x] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [x] unicité de publication sous concurrence prouvée

## Hors périmètre
Idempotence côté SMPP (le protocole n'a pas d'en-tête équivalent) ; persistance longue durée au-delà de 24 h.
