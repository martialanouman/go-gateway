# step-260i — `processOne` en trois temps, `connectorpool.go` en cinq fichiers

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** LIVRÉE (2026-09-05)
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

L'audit du 2026-09-03 a mesuré `internal/connectorpool/connectorpool.go` à 1 142 lignes, dont
`processOne` en fait 239 (`:852-1090`) : filtre connecteur, max-age, jeton d'annulation, reroute sur
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
// un seul calcul de (status, code) pour les deux puits (flux + Prometheus) ; code est vide sur ESME_ROK
func (s *Service) observeSubmit(resp smpp.PDU, code errs.Code, e2e time.Duration)
```

`processOne` après extraction (40 lignes) : span + **deux** `defer` (`span.End`, et une closure qui marque
le span puis efface la clé de retry sur un retour nil — les deux effets sont exclusifs sur toute valeur
de `err`, l'ordre est sans effet) ; décodage ; `preDispatch` ; `Submit` et son branchement d'erreur
(consomme `err` de `Submit`, reste ici) ; `e2e := e2eLatency(routed)` (le pourquoi du clamp vit dans le
helper, à côté d'`ageBase`) ; `settleOutcome`. `preDispatch` ne reçoit pas `rec` : aucune de ses branches
ne touche la clé de retry. Dans `settleOutcome`, `code` est calculé **une fois, sur un rejet seulement**
(`CodeFromSMPPStatus` ne doit jamais voir ESME_ROK) et passé aux deux puits.

**`retryKey`** : `type retryKey struct{ partition int32; offset int64 }`, construite par `keyOf(rec)`.

**Trous à combler AVANT le refactor** (`submit_internal_test.go`, `e2elatency_test.go`) :
`TestUndecodableRecordIsNotCommitted` (record `Value: []byte("{")` via `oneBatchConsumer` ⇒ résultat non
nil, le faux SMSC n'a rien reçu), `TestRetryKeyDistinguishesPartitions`, `TestSubmitOutcomesReachTheStream`
(aucun test du paquet ne posait de `StreamEmitter` : la closure du flux était à 0 % de couverture) et
`TestSubmitCountersCarryTheOutcomeAndTheCode` (aucun test ne lisait `submits_total` ni
`submit_rejected_total` : la mutation « `SubmitRejectedTotal` retiré » du protocole C ne tombait sur
rien). Tests de caractérisation : verts avant le refactor, leur mutation vue tomber est ce qui les rend
non creux.

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

- Baseline : 88,9 % de couverture ; dans `processOne`, non couverts : le décodage raté (`869-871`),
  l'annulation du ctx pendant l'attente AIMD (`947-949`), le reroute après une erreur de `Submit`
  (`962`), la closure du flux (`1046-1053`), la branche morte `EncodeOutcome` (`1083-1085`).
- Caractérisation, mutations vues tomber : décodage → `return nil` (le SMSC reçoit un submit) ; partition
  ignorée dans la clé ; émission du flux retirée ; `SubmitRejectedTotal` retiré ; `connector_id` vidé.
- A : multiset des lignes hors `package`/`import`/blanches identique avant/après (script), confirmé par la
  revue avec `--color-moved` ; 4 fichiers, 1 021 insertions / 982 suppressions.
- B : clé struct ; suite verte.
- C : ok/rejected intervertis → `TestSubmitOutcomesReachTheStream`, `TestE2ELatencyObservedOnAcceptedSubmit`,
  `TestE2ELatencyObservedOnPermanentReject` tombent.
- D : `heldByACancellation` toujours faux → `TestConnectorReleasesOnCancel`,
  `TestConnectorDispatchesWhenTheCancelTokenStoreIsCut`, `TestExpiredCancellationIsMisfiledWhenTheCancelTokenStoreIsCut`,
  `TestConnectorSkipsCancelledMessage` ; reroute disjoncteur-ouvert retiré → `TestRerouteOnBreakerOpenSkipsSubmit`.
- E : `recordDLRMapping` retiré → `TestConnectorRecordsDLRMappingOnEnroute`,
  `TestConnectorRecordsDLRMappingEvenWhenTheOutcomePublishFails` ; `Release` sur rejet retirée →
  `TestConnectorReleasesOnPermanentFailure`.
- Suite `-race` verte à chacun des commits, `make check` vert à la fin (deux passes, avant et après revue).

## Commits

1. Fiche. 2. Tests de caractérisation (3). 3. A : déplacement pur. 4. B : `retryKey`. 5. C : `observeSubmit`.
6. Test des compteurs Prometheus. 7. D : `preDispatch`. 8. E : `settleOutcome`. 9. F : un seul
`CodeFromSMPPStatus`, `processOne` sous 50 lignes. 10. Revue. 11. Fiche → `tasks-done/`.

## Definition of Done

- [x] `make check` vert (86 paquets)
- [x] le commit A ne montre que du code déplacé
- [x] suite verte à chaque commit et les mutations listées tombent chacune sur un test nommé
- [x] `processOne` = 40 lignes, un seul `CodeFromSMPPStatus` par issue, aucun fichier **non test** du
      paquet > 404 lignes (`connectorpool_test.go` fait 963 lignes, hors périmètre)

## Revue

Un sous-agent en lecture seule a comparé `processOne` d'avant et les helpers d'après ligne par ligne :
ordre des effets identique, chaque `return` et chaque marquage de span au même endroit, ordre des `defer`
sans effet. Aucun bloquant. Required corrigés : un « deferred recorder above » devenu faux dans
`preDispatch`, trois « above » qui pointaient dans un autre fichier, la fiche en retard sur le code.
Nits retenus : retour nommé `err` de `preDispatch` que seul le `Claim` alimentait (un futur `return` nu
aurait fait fuir l'erreur d'un fail-open) ; godoc de `healthRetry` collé sur `heldByACancellation`
depuis M8 ; `connector_id` absent des deux tests de compteurs.

## Hors périmètre

`bind.go`, `reroute.go`, `mapping.go`, `aimd.go`, `deliver.go`, `settle/` ; toute sémantique.
