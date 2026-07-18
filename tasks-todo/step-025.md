# step-025 — submit_sm → mt.inbound (pipeline identique REST) + bascules query/cancel

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-024 · **Bloque :** step-030

## But
Un ESME bindé soumet un `submit_sm` : le message emprunte **exactement le même pipeline** que REST (`mt.inbound`) et reçoit un `submit_sm_resp`. Parité protocole vérifiée.

## Périmètre (ce que fait CETTE PR)
- Dans `smpp-server-svc` : handler `submit_sm` → construit `pipeline.InboundMT` (mêmes champs que `restapi.submit`), publie sur `mt.inbound` **après ACK durable Kafka**, renvoie `submit_sm_resp` avec le `message_id`.
- Écrire la ligne CDR `accepted` en asynchrone après l'ACK (§1.10), comme REST.
- Bascules d'ops (`control_plane.smpp_accounts`) : `query_sm_enabled=false` → `ESME_RINVCMDID` ; `cancel_sm_enabled=false` → `ESME_RINVCMDID`. `query_sm`/`cancel_sm` activés : squelette de handler (le comportement `cancel_sm` réel arrive à step-030 ; `query_sm` reste minimal, sa limite de débit dédiée est M6 — Hors périmètre).
- Mapper `esm_class`/`data_coding`/`registered_delivery` du PDU vers l'enveloppe.

## Points d'implémentation clés
- **Invariant (a)** : le corps passe en `Body` masquant, jamais dans un span/log.
- Réutiliser la logique d'ingestion partagée avec REST (extraire un helper commun si nécessaire, sans dupliquer l'ordre du pipeline).
- L'ordre du pipeline est figé (§ CLAUDE.md) : le serveur SMPP ne fait qu'**ingérer**, le `router-svc` applique les étapes.
- `code` → `command_status` via `errs.SMPPStatus` pour les rejets (`invalid_destination`→`ESME_RINVDSTADR`, etc.).

## Tests (écrits dans la même PR)
- **Parité protocole** : le même message soumis en REST et en SMPP produit une enveloppe `mt.inbound` équivalente et le même chemin CDR (test dédié).
- `query_sm`/`cancel_sm` désactivés → `ESME_RINVCMDID`.
- Le message suit le CDR (`accepted` puis `enroute` via connector-pool en e2e).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] parité REST/SMPP prouvée par test

## Hors périmètre
Sémantique réelle de `cancel_sm` (step-030) ; limite de débit `query_sm` (M6) ; MO/DLR entrants (M4).
