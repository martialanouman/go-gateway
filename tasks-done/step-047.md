# step-047 — Webhooks signés HMAC-SHA256 (retries, dead-letter)

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-048

## But
Fournir l'émetteur de webhooks pour la voie retour : requête HTTP signée HMAC-SHA256, retries avec backoff, dead-letter après épuisement.

## Périmètre (ce que fait CETTE PR)
- Créer `internal/webhook` : `Sender` qui POST un événement (MO/DLR) vers `control_plane.webhooks.url`, en-tête de signature HMAC-SHA256 (clé = `webhooks.secret`).
- Retries avec backoff exponentiel + jitter, borné par `retry_policy_json` ; après N tentatives → dead-letter (topic `mo.dead-letter`/`dlr` ou table).
- Repo lecture `control_plane.webhooks` (par `account_id`, `event_type`).

## Points d'implémentation clés
- HMAC/HTTP = **stdlib** (`crypto/hmac`, `net/http`) — aucune nouvelle dépendance (§8).
- Signature déterministe sur le corps brut (documenter l'algorithme d'en-tête, ex. `X-Signature: sha256=...` + timestamp anti-rejeu).
- `context.Context` propagé ; timeouts HTTP stricts ; backoff respectant l'annulation (pas de goroutine sans arrêt).
- **Invariant (a)** : le corps du message peut figurer dans le payload webhook (destinataire légitime), mais **jamais dans un log** de l'émetteur.
- Idempotence côté récepteur facilitée par un `id` d'événement stable.

## Tests (écrits dans la même PR)
- Serveur HTTP de test : signature HMAC vérifiable ; 5xx → retry avec backoff ; échec persistant → dead-letter.
- Le corps n'apparaît dans aucun log de l'émetteur.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] signature, retry et dead-letter prouvés

## Hors périmètre
Branchement dans le routeur MO/DLR (step-048) ; Admin des webhooks (déjà déclaré, hors ce step).
