# step-028 — API publique : get-account (projection lecture seule)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But
Compléter l'API publique avec `GET /account` : une projection lecture seule du compte du principal authentifié (`operationId: get-account`, déjà déclaré `api/openapi-public.yaml`).

## Périmètre (ce que fait CETTE PR)
- Créer `internal/restapi/account.go` : handler huma `get-account`, enregistré dans `internal/restapi/api.go`.
- Projection conforme au schéma de réponse du contrat : identité du compte, canaux (`smpp_enabled`/`rest_enabled`), `sender_id_policy`, `max_sessions` — **aucun secret** (pas de hash, pas de clé).
- Requête sqlc lecture seule sur `control_plane.smpp_accounts` (+ `customers` si le contrat l'exige), scoping par `principal.AccountID`.

## Points d'implémentation clés
- **Les contrats sont la source de vérité** : conformer la forme de réponse à `api/openapi-public.yaml` (`get-account`), ne pas l'inventer.
- Réutiliser le middleware d'auth existant (`principalFromContext`) ; 401 `unauthenticated` si absent.
- Modèle d'erreur plat `{code,message,errors[]}` via `humaerr`.

## Tests (écrits dans la même PR)
- Test de contrat public (`internal/restapi/conformance_test.go`) : `get-account` présent et conforme.
- Handler : compte du principal renvoyé, aucun champ secret sérialisé.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] réponse strictement conforme au contrat, sans secret

## Hors périmètre
`list-messages` (step-029), `cancel-message` (step-030), `Idempotency-Key` (step-031).
