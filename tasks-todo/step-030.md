# step-030 — cancel-message (REST) + parité cancel_sm (SMPP)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-025, step-029 · **Bloque :** —

## But
Annuler un message **pas encore envoyé** : `POST /messages/{id}/cancel` (REST) et `cancel_sm` (SMPP) partagent la même sémantique et les mêmes résultats.

## Périmètre (ce que fait CETTE PR)
- Créer `internal/restapi/cancel.go` : handler huma `cancel-message` (déjà déclaré `api/openapi-public.yaml`).
- Logique d'annulation partagée (ex. `internal/pipeline` ou `internal/messaging`) réutilisée par REST et par le `cancel_sm` du `smpp-server-svc` (step-025 en fournit le point d'accroche).
- Message en file (avant `enroute`) → annulation : ligne CDR `cancelled` (§1.10), `200`.
- Message déjà envoyé (`enroute`+) → `409 cancel_failed` (REST) / `ESME_RCANCELFAIL` (SMPP) via `errs.ErrCancelFailed`.
- Message inconnu / autre compte → `404`.

## Points d'implémentation clés
- La « file » avant envoi : marquer l'annulation dans Redis (`cancel:{message_id}`) que le `connector-pool-svc` consulte avant `submit_sm` ; sinon l'état CDR fait foi. Choisir le mécanisme le plus simple qui garantit « pas encore envoyé ».
- `code` partagé : `cancel_failed` mappe `409` et `ESME_RCANCELFAIL` (déjà dans `internal/platform/errors`).
- Scoping strict par `account_id`.

## Tests (écrits dans la même PR)
- Test de contrat public : `cancel-message` conforme.
- Annulation d'un message en file → `200` + CDR `cancelled` ; déjà envoyé → `409`/`ESME_RCANCELFAIL`.
- **Parité** : mêmes résultats via REST et `cancel_sm` (test dédié).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] parité REST/SMPP sur cancel prouvée

## Hors périmètre
`Idempotency-Key` (step-031) ; annulation d'un message multi-segment déjà partiellement parti (M6).
