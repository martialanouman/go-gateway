# step-146 — Capture/libère dans connector-pool ; idempotent sous double livraison

> **Jalon :** M9 (§13 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-145 · **Bloque :** —

## But
Fermer la boucle de facturation MT : capturer le crédit réservé à l'envoi réussi, le libérer à
l'échec, de façon **idempotente sous double livraison d'un même `message_id`** (invariant c), et
renseigner `billed`/`credits_charged` dans le CDR.

## Périmètre (ce que fait CETTE PR)
- `internal/connectorpool/connectorpool.go` : après `submit_sm_resp`, `Capture` sur ESME_ROK,
  `Release` sur échec terminal — via le client billing.
- `cdrRow` : renseigner `Billed` et `credits_charged` (aujourd'hui `Billed:false` en dur).
- Court-circuit identique : facturation désactivée → aucun appel (booléen en cache).

## Points d'implémentation clés
- **Invariant (c)** : la livraison est at-least-once (`connectorpool` ne dédoublonne pas avant M3). Une
  double livraison du même `message_id` ne doit capturer qu'une fois → idempotence portée par le hold
  Redis `billing:reservation:{message_id}` + check d'existence ledger (step-141/142).
- Rejet transitoire (throttled/system error) : **ne pas** capturer ni libérer (le message est redélivré) —
  suivre le chemin `errTransientReject` existant.
- Rejet permanent (submit_fail, adresse invalide) : libérer la réserve + CDR `failed`.
- Corps jamais dans l'appel billing ni dans un log/span (invariant a).

## Tests (écrits dans la même PR)
- Succès → capture, `billed=1`, `credits_charged` correct.
- Échec permanent → release, réserve rendue.
- **Invariant (c)** : rejouer le même `message_id` deux fois → une seule capture (test bloquant).
- Rejet transitoire → ni capture ni release.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · **invariant (c)** re-testé bout-en-bout
- [ ] `billed`/`credits_charged` renseignés dans le CDR

## Hors périmètre
Adaptateur de facturation externe → step-147. Surface Admin → step-148/149.
