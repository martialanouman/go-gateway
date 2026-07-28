# step-129 — Dead-letter (`mt.dead-letter` / `mo.dead-letter`) + retraitement

> **Jalon :** M8 (§12 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-130

## But
Ne jamais perdre silencieusement un message : router vers un topic dead-letter tout message échoué/expiré après épuisement des retries (y compris `fallback_chain` épuisée), avec un chemin de retraitement.

## Périmètre (ce que fait CETTE PR)
- `internal/storage/kafka/topics.go` : ajouter `mt.dead-letter` / `mo.dead-letter` (§1.6 / §Appendix B).
- `internal/connectorpool` + `mo-dlr-router-svc` : publier en dead-letter à l'épuisement (retries, `fallback_chain`, validité expirée) avec la cause ; ligne CDR `failed` finale (§1.10).
- Retraitement : consommateur/outil qui rejoue une dead-letter vers `mt.inbound`/`mt.routed`.

## Points d'implémentation clés
- Message dead-lettré = **compté + tracé** (métrique + span), jamais jeté en silence (cf. §1.11 pour les DLR orphelins, même principe).
- L'enveloppe conserve `message_id`/`trace_id` → un rejeu reste corrélé et idempotent (invariant c préservé quand M9 sera là).
- Le corps voyage dans la valeur du record, jamais en en-tête loggable (invariant a).

## Tests (écrits dans la même PR)
- Retries épuisés → message en `mt.dead-letter` avec la cause + CDR `failed`.
- Rejeu d'une dead-letter → repart dans le pipeline, corrélation conservée.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] aucun message perdu en silence ; retraitement corrélé

## Hors périmètre
Les scénarios d'acceptation end-to-end + dé-`Skip` → step-130.
