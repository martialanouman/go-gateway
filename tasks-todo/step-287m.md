# step-287m — ClickHouse sobre sous charge : le reaper lit par compte, les CDR s'insèrent en async

> **Jalon :** M12 · **Statut :** EN COURS
> **Dépend de :** step-190 (reaper), step-287b (CDR des DLR par lot de poll)
> Demande humaine du 10/10/2026, née du diagnostic ClickHouse après le run 12 de step-287.

## Pourquoi
ClickHouse est le premier consommateur de CPU de contabo75 (2,2 cœurs au run 12), l'hôte qu'il partage avec
Postgres et Redpanda, dont il allonge les allers-retours. Ses tables système, sur les 10 min du run 12 :
- **reaper** : `MessageStatus` → `ByMessageID` filtre sur `message_id` seul, hors du préfixe de la clé de tri ;
  chaque appel lit toute la table (11,7 M lignes, 181 ms). 437 appels en 10 min, 32 836 sur une journée,
  11 milliards de lignes lues ;
- **inserts** : 22 par seconde et par table (`cdr`, `cdr_events`), médiane 59 lignes ; 26 500 parts neuves et
  6 700 fusions en 10 min, ~860 s de travail.

## Design arrêté
**PR1 — le reaper lit par compte.**
- `OutcomeReader.MessageStatus(ctx, customerID, accountID *uuid.UUID, messageID)` : avec un compte, la lecture
  passe par `Current` (préfixe `(customer_id, account_id)` de la clé de tri) ; sans compte (compte supprimé,
  `account_id` mis à NULL), elle garde `ByMessageID`.
- Le reaper passe `o.CustomerID` et `o.AccountID` de la réservation orpheline.
- Un CDR dont le compte diffère de celui de la réservation n'est plus trouvé : la réservation reste intacte et
  compte comme `Unresolvable`, ce qui est déjà le sort d'un message sans CDR. Jamais de remboursement deviné.

**PR2 — les lots de CDR s'insèrent en asynchrone.** Au run 12, 98 % des insertions viennent des projections de
router-svc (`ingest/accepted.go`, `outcome/outcome.go`) : `InsertBatch` à chaque poll, ~10 par seconde et par
pod, médiane 51 lignes. `Insert` (une ligne) ne sert qu'aux chemins rares : rejet, annulation, reroutage.
- `InsertBatch` et le miroir `cdr_events` passent `async_insert=1` et **`wait_for_async_insert=1`** par
  `clickhouse.WithSettings` dans le contexte de l'appel : le serveur regroupe les lots de tous les écrivains,
  et l'appel ne rend la main qu'une fois les lignes écrites. L'offset Kafka reste commité après une écriture
  durable (D6/D8) ; les réglages sont fixés dans le writer, pas transmis depuis la config.
- `Insert` reste synchrone : une ligne sur un chemin rare n'a rien à regrouper, et n'attendrait que le délai
  du tampon serveur.
- Le coût : un lot attend que le tampon serveur se vide (adaptatif, 200 ms au plus en 24.8). Les projections
  sont des boucles de poll : un lot plus lent rend le suivant plus gros, sans toucher au chemin du message.
- Test d'intégration : les lignes d'un `InsertBatch` sont lisibles dès son retour, et le `query_log` montre
  l'insertion avec `async_insert=1` et `wait_for_async_insert=1` ; un `Insert` n'y apparaît pas en async.

## Definition of Done
- [x] PR1 : `MessageStatus` avec compte ne trouve pas le CDR d'un autre compte ni d'un autre client, et trouve
      le sien ; sans compte, il trouve par `message_id` (intégration ClickHouse, 3 mutations tuées)
- [x] PR1 : le reaper passe le client et le compte de la réservation (test unitaire, 2 mutations tuées)
- [ ] PR2 : design arrêté puis livré
- [ ] mesuré à un run : `ByMessageID` sort du `query_log` du reaper, parts neuves et fusions en baisse
