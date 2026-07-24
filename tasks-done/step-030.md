# step-030 — cancel_sm (SMPP) : annulation d'un message pas-encore-envoyé

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** FAIT
> **Dépend de :** step-025, step-029 · **Bloque :** —
> **Décision :** annulation **SMPP-only**, sans surface REST — voir [ADR-0009](../docs/adr/0009-annulation-reservee-smpp.md).

## But
Annuler un message **pas encore envoyé au SMSC** via `cancel_sm` (SMPP). L'annulation est une opération
**exclusivement SMPP** : les clients REST n'ont aucun moyen d'annuler (ADR-0009).

## Périmètre (ce que fait CETTE PR)
- Nouveau package `internal/cancel` : logique d'annulation partagée (`Canceller`) + store du flag Redis
  (`RedisFlags`).
- Remplacer le squelette `cancel_sm` de step-025 (`internal/smppserver/ops.go` `onCancel`) par la vraie
  sémantique, scopée strictement par `account_id` (le hook a déjà `st.accountID`/`st.customerID`).
- Message en file (`accepted`, avant `enroute`) → annulation : flag Redis `cancel:{message_id}` + ligne
  CDR `cancelled` (§1.10, rang 60) → `ESME_ROK`.
- Message déjà envoyé (`enroute`+) → `ESME_RCANCELFAIL` via `errs.ErrCancelFailed`.
- Message inconnu / autre compte → `ESME_RINVMSGID` via `errs.ErrMessageNotFound`.
- Double annulation (message déjà `cancelled`) → idempotent, `ESME_ROK`.
- `connector-pool-svc` consulte le flag `cancel:{message_id}` **avant** `submit_sm` : si annulé, saute
  l'envoi et committe l'offset (le `Canceller` a déjà écrit le CDR `cancelled`).
- **Retirer** l'opération `cancel-message` de `api/openapi-public.yaml` (plus de route REST) et l'entrée
  `deferred` correspondante du test de conformité.

## Points d'implémentation clés
- La « file » avant envoi : le flag Redis `cancel:{message_id}` (TTL 72 h) est le mécanisme le plus
  simple garantissant « pas encore envoyé » ; l'état CDR fait foi pour l'état affiché.
- Le flag est posé **avant** l'écriture du CDR `cancelled` (le flag prévient l'envoi ; le CDR est l'état
  visible qui suit).
- `code` → `command_status` via `errs.SMPPStatusForError` (mapping déjà dans `internal/platform/errors`,
  rien à ajouter). `ErrMessageNotFound` porte `ESME_RINVMSGID` (contrairement à `ErrNotFound`, sans
  surface SMPP).
- Câblage : `smpp-server-svc` a déjà Redis + ClickHouse ; `connector-pool-svc` gagne une dépendance
  Redis (non-vitale). `rest-api-svc` **inchangé**.

## Tests (écrits dans la même PR)
- `internal/cancel` : `accepted` → flag + CDR `cancelled` ; `enroute`/terminal → `ErrCancelFailed` ;
  inconnu → `ErrMessageNotFound` ; déjà `cancelled` → idempotent ; erreurs infra → `ErrInternal`. Store
  Redis réel (`redistest`).
- `internal/smppserver` : `cancel_sm` désactivé → `ESME_RINVCMDID` ; activé → `ESME_ROK` (annulable),
  `ESME_RCANCELFAIL` (déjà envoyé), `ESME_RINVMSGID` (inconnu / message_id non-uuid) ; scoping par
  `account_id` ; `Canceller` nil → `ESME_RCANCELFAIL`.
- `internal/connectorpool` : flag posé → pas de `submit_sm`, ligne CDR `cancelled` écrite par le
  connector (idempotent, ferme la fenêtre de crash), offset committé ; flag absent → envoi normal ;
  erreur Redis → **fail-open** (envoi quand même, l'annulation étant best-effort).

## Definition of Done
- [x] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [x] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [x] route REST `cancel-message` retirée du contrat public ; test de conformité vert
- [x] déviation tracée (ADR-0009)

## Hors périmètre
Surface REST d'annulation (ADR-0009) ; `Idempotency-Key` (step-031) ; annulation d'un message
multi-segment déjà partiellement parti (M6) ; la course intrinsèque annulation vs `submit_sm` déjà parti.
