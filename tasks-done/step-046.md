# step-046 — Remise deliver_sm côté smpp-server via SessionRegistry.Deliver

> **Jalon :** M4 (§8 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-022, step-024 · **Bloque :** step-048

## But
Permettre la remise d'un MO/DLR à un ESME bindé : le `smpp-server-svc` reçoit `SessionRegistry.Deliver` (gRPC) et pousse un `deliver_sm` sur le bind détenteur (rx/trx).

## Périmètre (ce que fait CETTE PR)
- `smpp-server-svc` : registre **local** `bind_id → conn` (les binds vivants de ce pod), alimenté au bind/unbind (step-024).
- Implémenter le côté serveur de `SessionRegistry.Deliver` (ou un endpoint gRPC dédié du pod) : recevoir le `bytes pdu`, l'écrire sur le bind cible, gérer `deliver_sm_resp` et la fenêtre (`window_size`, step-023).
- Sélection du bind : seulement `rx`/`trx` (un `tx` ne reçoit pas) ; erreur si le `bind_id` n'est plus vivant (le caller re-résout).
- **Rouvrir la readiness du `smpp-server-svc`** (dette de step-025) : ce service marque aujourd'hui Kafka **vital**. Or à partir de cette step un bind `rx`/`trx` délivre des `deliver_sm` **sans avoir besoin de Kafka** ; une panne Kafka sortirait le pod du LB et couperait la remise MO/DLR. Rendre **Kafka non vital** — un `submit_sm` qui ne peut plus produire échoue **par PDU** en `ESME_RSYSERR` (comme la dépendance `SessionRegistry` déjà), pendant que la remise `deliver_sm` continue.

## Points d'implémentation clés
- **`ctx7`** avant d'utiliser le serveur gRPC côté pod / le client depuis session-manager.
- **Invariant (a)** : le `deliver_sm` transporte un corps ; jamais loggé.
- Respecter la fenêtre d'émission (backpressure) : ne pas dépasser `window_size` de `deliver_sm` en vol.
- `Deliver` échoue proprement (bind mort) → status gRPC exploitable pour round-robin/retry côté appelant (step-048).
- **Readiness (§1.5)** : après cette step, seul **PostgreSQL** reste vital pour le `smpp-server-svc` (auth du bind) ; Kafka et ClickHouse sont dégradables (échec `submit_sm` par PDU, ligne `accepted` best-effort). Retirer Kafka des `ReadyCheck` de `cmd/smpp-server-svc/main.go`.

## Tests (écrits dans la même PR)
- Intégration : un ESME (`fakesmsc` en mode client rx/trx ou client de test) bind ; un `Deliver` gRPC pousse un `deliver_sm` reçu par l'ESME, `deliver_sm_resp` renvoyé.
- `Deliver` vers un `bind_id` mort → erreur exploitable.
- Bind `tx` non éligible à la remise.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] remise `deliver_sm` de bout en bout via gRPC prouvée
- [ ] Kafka retiré des dépendances vitales du `smpp-server-svc` : une panne Kafka ne coupe pas les binds `rx`/`trx` ni la remise MO/DLR

## Hors périmètre
Décision remise-bind vs webhook (step-048) ; routage inter-pods réel à l'échelle (M12).
