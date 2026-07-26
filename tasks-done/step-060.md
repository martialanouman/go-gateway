# step-060 — Étape pipeline : autorisation Sender ID (§6.19)

> **Jalon :** M5 (§9 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But
Activer l'étape STUB `pipeline.sender_id` : autoriser (ou rejeter) le `source_addr` d'un MT selon la politique du compte et les `sender_ids` actifs du client.

## Périmètre (ce que fait CETTE PR)
- Créer `internal/pipeline/senderid` : évaluation de la politique `sender_id_policy` du compte (`strict` / `allow_unregistered_numeric` / `disabled`) vs `From` du message.
- Remplacer le `stubStage(ctx, "pipeline.sender_id")` de `internal/pipeline/pipeline.go` par l'étape réelle (garder l'émission de span, l'ordre figé §6.1).
- Snapshot en mémoire des `control_plane.sender_ids` `active` par client (rechargé à froid ; hot reload M7).
- Rejet → `errs.ErrSenderIDNotAuthorized` (`sender_id_not_authorized`, `403`/`ESME_RINVSRCADR`, déjà défini) → CDR `rejected`.

## Points d'implémentation clés
- `strict` : `From` doit correspondre à un `sender_ids.address` actif du client. `allow_unregistered_numeric` : tolère un numérique non enregistré. `disabled` : passe.
- L'étape reste **bloquante** et **jamais court-circuitée** par une route exacte (invariant b, pleinement testable M7).
- **Invariant (a)** : le span porte le `code` de rejet, jamais le corps.
- Ordre du pipeline non réordonnable (CLAUDE.md).

## Tests (écrits dans la même PR)
- Table de tests par politique : autorisé / rejeté (`sender_id_not_authorized`, CDR `rejected`).
- Le rejet produit le bon `code` et le bon `command_status` SMPP.
- Test « ne logge pas le corps » sur l'étape.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] STUB `pipeline.sender_id` remplacé, span conservé

## Hors périmètre
Opt-out (step-061..064) ; anti-spam (step-065..067) ; hot reload des snapshots (M7).
