# step-390b — `query_sm` résout l'état du message au lieu de répondre UNKNOWN

> **Jalon :** Surfaces déclarées par la spec, jamais construites (§6.22 `docs/specification-technique-passerelle-sms.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## But

Tenir la promesse de §6.22 : « `query_sm` — état d'un message par son ID, **résolu contre le magasin de
statut/CDR** ». Aujourd'hui `internal/smppserver/ops.go` répond `ESME_ROK` + `MessageStateUnknown` à
toute requête autorisée et non throttlée (`ops.go:35-39`) ; `internal/smpp/smpp.go` l'avoue dans le godoc de
`MessageStateUnknown` (« while the real state lookup is unimplemented »). Répondre `UNKNOWN` est légal
SMPP : c'est une promesse non tenue, pas une panne, et cette fiche ne bloque pas le go-live. step-390 ne
porte que les réglages de compte (`query_sm_enabled`), et aucune fiche ne portait la résolution ;
step-260g a ouvert celle-ci pour que la dette ait un porteur.

## Ce qui existe déjà (vérifié le 2026-09-04)

- `smpp-server-svc` déclare `config.SectionClickHouse` (`cmd/smpp-server-svc/main.go:55`) et construit
  déjà `clickhouse.NewCDRReader(st.ch)` pour `cancel_sm` (`wiring.go:231`). **Aucun store neuf, aucune
  section neuve** : le reader passe au `Listener` par ses options, à côté de `QueryLimiter`
  (`internal/smppserver/smppserver.go:138`).
- Le throttle dédié §6.22 est posé (`wiring.go:250-257`, compteur `smpp_query_throttled_total`) et
  s'applique **avant** la résolution : le polling ne peut pas reporter sa charge sur ClickHouse au-delà
  du débit accordé.
- La lecture à utiliser est `CDRReader.Current(ctx, customerID, accountID, messageID)`
  (`internal/storage/clickhouse/cdr.go:422`) : scopée tenant par le préfixe de clé de tri, elle ne fait
  pas de full scan et ne peut pas révéler le message d'un autre compte. `ByMessageID` et
  `MessageStatus` (`cdr.go:409,440`) sont cross-tenant et « non-hot-path admin » : **interdits** ici.
- La session connaît son `customerID`/`accountID` (`st` dans `ops.go`) ; le `message_id` du `query_sm`
  est celui rendu par `submit_sm_resp`, un UUIDv7 en texte.

## Design arrêté

**Mapping statut CDR → `message_state` (SMPP §5.2.28)**, un seul endroit, testé par table :

| statut CDR (`schema:759`) | `message_state` | `final_date` |
|---|---|---|
| `accepted`, `enroute`, `rerouted` | `ENROUTE` (1) | vide |
| `delivered` | `DELIVERED` (2) | `delivered_at` |
| `expired` | `EXPIRED` (3) | `delivered_at` |
| `cancelled` | `DELETED` (4) | vide (voir ci-dessous) |
| `failed` | `UNDELIVERABLE` (5) | `delivered_at` si porté, sinon vide |
| `rejected` | `REJECTED` (8) | vide — aucun writer n'émet ce statut aujourd'hui (`StatusRejected` n'est que lu, `replay.go:213`) |
| inconnu du tenant, ou `message_id` non parsable | `ESME_RINVMSGID` | — |

`error_code` reste 0 (le CDR porte un `error_code` textuel du catalogue, pas un code réseau SMPP ;
le mapper serait inventer).

**`ACCEPTED` (6) n'est pas utilisé.** En SMPP 3.4 §5.2.28, ACCEPTED signifie « lu manuellement pour le
compte de l'abonné par le service client », pas « accepté par la passerelle ». Un message retenu avant
la soumission est `ENROUTE` pour un ESME 3.4 (SCHEDULED n'existe qu'en 5.0). La distinction
`accepted`/`enroute` reste visible par `get-message` REST, pas par `query_sm`.

**`final_date`** : `CDRRow` ne porte que `SubmittedAt` et `DeliveredAt` (`cdr.go:114-115`), et la table
n'a pas d'horodatage d'insertion (`version` est un rang). Les lignes `cancelled` (`mapping.go:104`) et
reroute/dead-letter (`reroute.go:183`) ne remplissent pas `delivered_at` : leur `final_date` est vide,
ce que SMPP tolère mal pour un état final. Arbitrage à la PR : soit le consigner comme écart, soit faire
remplir `delivered_at` par ces writers (une ligne chacun, mais un changement de sémantique de colonne à
nommer). À noter aussi : l'expiration max-age du pool passe par la dead-letter et écrit `failed`, pas
`expired` (`reroute.go:139-142`) — un tel message répond `UNDELIVERABLE`, non `EXPIRED`.

**Arbitrage — le lag de projection (ADR-0012).** Le statut est une projection asynchrone : un message
soumis il y a 200 ms peut n'avoir que sa ligne `accepted` alors qu'il est déjà sur le fil. Comme
`accepted` et `enroute` répondent tous deux `ENROUTE`, le lag ne change pas la réponse avant l'issue
terminale ; après, on répond le dernier état durablement connu, exactement ce que `get-message` REST
répond au même instant (parité protocole). On ne lit **pas** Redis ni le pool pour
« rattraper » la projection. Le lag se surveille par la métrique du guide §16.

**Erreur ClickHouse** : `ESME_RQUERYFAIL` (`0x67`), le code SMPP dédié, **n'existe pas** dans
`internal/platform/errors` (vérifié) : l'ajouter suit `.claude/rules/errors.md` (trois endroits). Jamais `ESME_ROK` + `UNKNOWN` sur une erreur : ce serait retomber dans le défaut que
cette fiche ferme.

**Retrait de l'aveu** : le godoc de `MessageStateUnknown` (`smpp.go:58-59`) perd sa seconde phrase dans
la même PR.

## Chaîne de preuves

1. Rouge lu dans `internal/smppserver` : `query_sm` d'un message `delivered` (fake reader) attend
   `MessageStateDelivered` ; aujourd'hui `MessageStateUnknown`.
2. Table du mapping ; `ESME_RINVMSGID` pour un ID inconnu ; `ESME_RINVMSGID` pour un ID d'un autre
   compte (fixture non creuse : le fake reader **doit** recevoir le `accountID` de la session, sinon
   le test passe sur un reader qui ignore le scope — mémoire `hollow-test-fixtures`).
3. Intégration `clickhouse` : `Current` sur une ligne insérée par le writer, scope respecté.
4. Test de câblage `cmd/smpp-server-svc/wiring_test.go` : le reader est branché (mutation : retirer
   l'option → `UNKNOWN` → tombe).
5. `make check` vert.

## Hors périmètre

`replace_sm`, `data_sm` (non supportés, spec §5.1). La bascule `query_sm_enabled` (step-025) et son
réglage Admin (step-390). Un cache des réponses `query_sm` : le throttle suffit tant que le p99 de
`Current` ne le contredit pas.
