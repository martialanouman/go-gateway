# step-023 — Machine à états de session SMPP serveur (internal/smpp/session)

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-024

## But
Implémenter la machine à états d'une session SMPP côté serveur (framing sur le codec `internal/smpp` livré à M2) : cycle `open → bound → unbound`, `enquire_link`, fenêtre d'émission — sans logique métier ni auth (branchées à step-024).

## Périmètre (ce que fait CETTE PR)
- Créer `internal/smpp/session` : type `Session` lisant/écrivant des PDU via `internal/smpp`, transitions d'états, dispatch par `CommandID`.
- Gérer `bind_transmitter`/`receiver`/`transceiver` → `*_resp`, `enquire_link` → `enquire_link_resp`, `unbind` → `unbind_resp`, `generic_nack` sur PDU invalide.
- **Fenêtre** (`window_size`) : bornage des requêtes serveur→client en vol (utile pour la remise `deliver_sm` à step-046).
- Callbacks/hooks (`OnBind`, `OnSubmit`, `OnUnbind`) laissés à l'appelant : la session ne décide pas l'auth ni le routage.

## Points d'implémentation clés
- **Invariant (a)** : jamais le corps (`short_message`) dans un log/span. Utiliser le type `Body` masquant (`msg`) dès l'extraction.
- `context.Context` en 1er paramètre ; boucle de lecture avec condition d'arrêt claire (unbind, EOF, ctx annulé) — aucune goroutine fuyante (règle d'or).
- Rejet des PDU hors-séquence selon l'état (ex. `submit_sm` avant bind → `ESME_RINVBNDSTS`).
- `interface_version 0x34` (SMPP 3.4, déjà constante `InterfaceVersion34`).

## Tests (écrits dans la même PR)
- Unitaires sur pipe en mémoire (`net.Pipe`) : séquence bind→enquire_link→unbind ; PDU hors-séquence rejeté ; fenêtre saturée bloque/relâche.
- Test « ne logge pas le corps » (invariant a) sur le chemin `submit_sm`.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] transitions d'états couvertes (table de tests)

## Hors périmètre
Auth du bind, `max_sessions`, listener socket (step-024) ; `submit_sm` → pipeline (step-025) ; poussée `deliver_sm` (step-046).
