# step-260i — `processOne` en trois temps, `connectorpool.go` en quatre fichiers

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** EN COURS (2026-09-05)
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

L'audit du 2026-09-03 a mesuré `internal/connectorpool/connectorpool.go` à 1 142 lignes, dont
`processOne` en fait 248 (`:852-1099`) : filtre connecteur, max-age, jeton d'annulation, reroute sur
disjoncteur ouvert, attente AIMD, `submit_sm`, disjoncteur, AIMD, reroute, retry, mapping DLR,
règlement, deux puits de métriques, publication — dans une seule fonction, avec trois `defer` sur un
retour nommé. Refactor iso-comportement : aucune signature exportée, aucun champ de `Deps`, aucune
sémantique de halte de shard ni de `defer` ne change.

## Ce que l'exploration a établi

- Six fichiers de test internes (`package connectorpool`) dépendent de `shardIndex`, `errShardHalted`,
  `newAIMD`, `dialAndBind`, `parseReceipt` : tout reste dans le paquet, rien n'est exporté.
- Les trois `defer` de `processOne` (`span.End`, `RecordSpanError(span, err)`, `retryFirstFail.Delete`
  si `err == nil`) portent sur le retour nommé `err` : ils doivent rester dans l'enveloppe, et les
  helpers ne font que *retourner* l'erreur — l'invariant « `healthRetry` insère, un retour nil supprime »
  (`:834-843`, `:862-866`) est intact.
- `e2e` est lu à l'instant de la réponse (`:975`), avant toute comptabilité : il reste dans
  l'enveloppe et se passe en paramètre.
- Le `span` doit être passé aux helpers : un code de retour perdrait la distinction « issue terminale
  marquée sur le span, mais `err == nil` » (`:900`, `:936`, `:1002`, `:1040`).
- `retryKey` fait un `Sprintf` par message (`:845`) pour une clé `sync.Map` : une struct `{partition,
  offset}` est comparable, zéro allocation ; la `sync.Map` reste (écrit une fois, lu par le même shard,
  supprimé — son cas d'usage).
- Le calcul `(status, code)` est fait deux fois pour les deux puits (`:1041-1071`).
- Branche morte : `pipeline.EncodeOutcome` en échec (`:1083-1085`) — `json.Marshal` d'`outcomeWire` ne
  peut pas échouer ; pas de test creux, noté.
- Baseline de couverture (`go test -race -coverprofile`, suite complète) : voir « Chaîne de preuves ».

## Design arrêté

**Fichiers** (mêmes corps, déplacés) :

| fichier | contenu |
|---|---|
| `connectorpool.go` | doc de paquet, sentinelles `err*`, `Service`, constantes, `New`, `BindReady`, `LinkStatus`, `setLink` |
| `deps.go` | interfaces consommateur, `noop*`, `Deps` (`:56-282`) |
| `lifecycle.go` | `Run` … `shardIndex` (`:404-765`), `breakerStates`, `severity` (`:1123-1142`) |
| `submit.go` | `ageBase` … `retryKey`, `processOne` et ses helpers, `recordDLRMapping`, `stream` (`:767-1121`) |
| `metrics.go` | `observeSubmit` |

**Helpers extraits de `processOne`** :

```go
// tout ce qui règle un record SANS le mettre sur le fil : filtre connecteur, max-age (peek, annulation,
// dead-letter), Claim du jeton, disjoncteur ouvert → reroute, attente AIMD. done=true : processOne
// retourne err tel quel.
func (s *Service) preDispatch(ctx context.Context, span trace.Span, bindIndex int, routed pipeline.RoutedMT) (done bool, err error)
// après submit_sm_resp : feedBreaker, AIMD, puis reroute | transitoire | issue terminale (mapping DLR,
// règlement, observeSubmit, publication mt.outcome).
func (s *Service) settleOutcome(ctx context.Context, span trace.Span, bindIndex int, rec kafka.Record, routed pipeline.RoutedMT, resp smpp.PDU, e2e time.Duration) error
// un seul calcul de (status, code) pour les deux puits (flux + Prometheus)
func (s *Service) observeSubmit(resp smpp.PDU, e2e time.Duration)
```

`processOne` après extraction (≈ 45 lignes) : span + les trois `defer` ; décodage ; `preDispatch` ;
`Submit` et son branchement d'erreur (consomme `err` de `Submit`, reste ici) ; `e2e` ; `settleOutcome`.
`preDispatch` ne reçoit pas `rec` : aucune de ses branches ne touche la clé de retry.

**`retryKey`** : `type retryKey struct{ partition int32; offset int64 }`, construite par `keyOf(rec)`.

**Trous à combler AVANT le refactor** (`submit_internal_test.go`) : `TestUndecodableRecordIsNotCommitted`
(record `Value: []byte("{")` via `oneBatchConsumer` ⇒ résultat non nil, le faux SMSC n'a rien reçu ;
mutation : `return nil` au décodage → tombe) et `TestRetryKeyDistinguishesPartitions` (même offset,
partitions différentes ⇒ clés différentes ; mutation : ignorer la partition → tombe). Tests de
caractérisation : verts avant le refactor, leur mutation vue tomber est ce qui les rend non creux.

## Protocole (chaque commit réversible seul, suite `-race` verte à chacun)

0. Baseline de couverture ; les deux tests de caractérisation.
A. **Déplacement pur** vers `deps.go`, `lifecycle.go`, `submit.go` : zéro édition de corps ;
   `git diff --color-moved=dimmed-zebra` ne montre que du déplacé.
B. `retryKey` struct.
C. `observeSubmit` (`metrics.go`). Mutations : intervertir `"ok"`/`"rejected"` → `e2elatency_test` tombe ;
   retirer `SubmitRejectedTotal` → tombe.
D. `preDispatch`. Mutations : `heldByACancellation` renvoie toujours `false` →
   `TestExpiredCancelledMessageIsNotDeadLettered` / `TestConnectorStillWritesCancelledRowDirectly`
   tombent ; retirer le reroute disjoncteur-ouvert → `sim_fallback` tombe.
E. `settleOutcome`. Mutations : retirer `recordDLRMapping` →
   `TestConnectorRecordsDLRMappingEvenWhenTheOutcomePublishFails` tombe ; retirer `Billing.Release` sur
   rejet → `billing_settle_test` tombe.
Puis `golangci-lint`, `make check`, fiche → done.

## Chaîne de preuves

À remplir commit par commit : la baseline, le diff « déplacé seulement », chaque mutation avec le test
qui tombe.

## Definition of Done

- [ ] `make check` vert
- [ ] le commit A ne montre que du code déplacé
- [ ] suite verte à chaque commit et les mutations listées tombent chacune sur un test nommé
- [ ] `processOne` ≤ 50 lignes, un seul `CodeFromSMPPStatus` par issue, aucun fichier du paquet > 600 lignes

## Hors périmètre

`bind.go`, `reroute.go`, `mapping.go`, `aimd.go`, `deliver.go`, `settle/` ; toute sémantique.
