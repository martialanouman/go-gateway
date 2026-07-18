# step-046 — Remise deliver_sm côté smpp-server via SessionRegistry.Deliver

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-022, step-024 · **Bloque :** step-048

## But
Permettre la remise d'un MO/DLR à un ESME bindé : le `smpp-server-svc` reçoit `SessionRegistry.Deliver` (gRPC) et pousse un `deliver_sm` sur le bind détenteur (rx/trx).

## Périmètre (ce que fait CETTE PR)
- `smpp-server-svc` : registre **local** `bind_id → conn` (les binds vivants de ce pod), alimenté au bind/unbind (step-024).
- Implémenter le côté serveur de `SessionRegistry.Deliver` (ou un endpoint gRPC dédié du pod) : recevoir le `bytes pdu`, l'écrire sur le bind cible, gérer `deliver_sm_resp` et la fenêtre (`window_size`, step-023).
- Sélection du bind : seulement `rx`/`trx` (un `tx` ne reçoit pas) ; erreur si le `bind_id` n'est plus vivant (le caller re-résout).

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser le serveur gRPC côté pod / le client depuis session-manager.
- **Invariant (a)** : le `deliver_sm` transporte un corps ; jamais loggé.
- Respecter la fenêtre d'émission (backpressure) : ne pas dépasser `window_size` de `deliver_sm` en vol.
- `Deliver` échoue proprement (bind mort) → status gRPC exploitable pour round-robin/retry côté appelant (step-048).

## Tests (écrits dans la même PR)
- Intégration : un ESME (`fakesmsc` en mode client rx/trx ou client de test) bind ; un `Deliver` gRPC pousse un `deliver_sm` reçu par l'ESME, `deliver_sm_resp` renvoyé.
- `Deliver` vers un `bind_id` mort → erreur exploitable.
- Bind `tx` non éligible à la remise.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] remise `deliver_sm` de bout en bout via gRPC prouvée

## Hors périmètre
Décision remise-bind vs webhook (step-048) ; routage inter-pods réel à l'échelle (M12).
