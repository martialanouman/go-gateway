# step-390b — `query_sm` résout l'état du message au lieu de répondre UNKNOWN

> **Jalon :** Surfaces déclarées par la spec, jamais construites (§6.22 `docs/specification-technique-passerelle-sms.md`) · **Statut :** LIVRÉE
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
| `expired` | `EXPIRED` (3) | vide |
| `cancelled` | `DELETED` (4) | vide |
| `failed` | `UNDELIVERABLE` (5) | vide |
| `rejected` | `REJECTED` (8) | vide — aucun writer n'émet ce statut aujourd'hui (`StatusRejected` n'est que lu, `replay.go:213`) |
| inconnu du tenant, ou `message_id` non parsable | `ESME_RINVMSGID` | — |

`error_code` reste 0 (le CDR porte un `error_code` textuel du catalogue, pas un code réseau SMPP ;
le mapper serait inventer).

**`ACCEPTED` (6) n'est pas utilisé.** En SMPP 3.4 §5.2.28, ACCEPTED signifie « lu manuellement pour le
compte de l'abonné par le service client », pas « accepté par la passerelle ». Un message retenu avant
la soumission est `ENROUTE` pour un ESME 3.4 (SCHEDULED n'existe qu'en 5.0). La distinction
`accepted`/`enroute` reste visible par `get-message` REST, pas par `query_sm`.

**`final_date`** (arbitré par Fable le 2026-09-27) : `delivered_at` n'est rempli que pour
`delivered` (`modlrrouter.go:210`), et l'agrégat ne le remonte que si tous les segments sont livrés
(`cdr.go:366`). `final_date` vaut donc `delivered_at` pour DELIVERED et reste vide pour tout autre état,
final compris. Faire remplir `delivered_at` par les writers `expired`/`failed`/`cancelled` changerait la
sémantique d'une colonne publique (REST get-message, export et recherche Admin, `latency_ms`,
`cdr_events.at`) : écarté. Une colonne `finalized_at` (migration + tous les writers) est hors de
proportion pour un champ informatif d'une opération de polling. L'écart est consigné dans
`debts/query-sm-final-date-vide-hors-delivered.md`. Format : temps absolu SMPP §7.1.1 en UTC
(`YYMMDDhhmmsst00+`). À noter aussi : l'expiration max-age du pool passe par la dead-letter et écrit
`failed`, pas `expired` (`reroute.go:139-142`) — un tel message répond `UNDELIVERABLE`, non `EXPIRED`.

**Arbitrage — le lag de projection (ADR-0012).** Le statut est une projection asynchrone : un message
soumis il y a 200 ms peut n'avoir que sa ligne `accepted` alors qu'il est déjà sur le fil. Comme
`accepted` et `enroute` répondent tous deux `ENROUTE`, le lag ne change pas la réponse avant l'issue
terminale ; après, on répond le dernier état durablement connu, exactement ce que `get-message` REST
répond au même instant (parité protocole). On ne lit **pas** Redis ni le pool pour
« rattraper » la projection. Le lag se surveille par la métrique du guide §16.

**Erreur ClickHouse** (arbitré par Fable le 2026-09-27) : `ESME_RQUERYFAIL` (`0x67`), ajouté comme
constante SMPP brute `StatusQueryFail` à côté de `StatusInvalidBindStatus`, **pas** comme `Code` du
catalogue : il n'a ni surface HTTP ni `cdr.error_code`, donc `.claude/rules/errors.md` ne s'applique pas
(ni OpenAPI, ni §11.3, ni bump). Le `Code` métier de l'échec reste `internal_error`, ce que REST
get-message répond au même instant. Un `MessageReader` absent (build sans ClickHouse) répond aussi
`ESME_RQUERYFAIL`, comme un `Canceller` absent répond `ESME_RCANCELFAIL`. Jamais `ESME_ROK` + `UNKNOWN`
sur une erreur : ce serait retomber dans le défaut que cette fiche ferme.

**Ajouts de la revue** (arbitrés par Fable le 2026-09-27) : un statut CDR hors du mapping répond
`ESME_RQUERYFAIL`, pas `ESME_ROK` + `UNKNOWN`. La lecture ClickHouse de `query_sm` et le `Cancel` de
`cancel_sm` sont bornés par `cdrLookupTimeout` (5 s, à côté de `registryCallTimeout`) : les deux tournent
sur la goroutine de lecture de la session, et le `ReadTimeout` par défaut du client (300 s) gèlerait tout
le bind. Un dépassement répond `ESME_RQUERYFAIL` pour `query_sm` et `ESME_RSYSERR` pour `cancel_sm`.

**Retrait de l'aveu** : le godoc de `MessageStateUnknown` (`smpp.go:58-59`) perd sa seconde phrase dans
la même PR.

## Chaîne de preuves

1. Rouge lu dans `internal/smppserver` : `query_sm` d'un message `delivered` (fake reader) attend
   `MessageStateDelivered` ; aujourd'hui `MessageStateUnknown`.
2. Table du mapping ; `ESME_RINVMSGID` pour un ID inconnu ; `ESME_RINVMSGID` pour un ID d'un autre
   compte (fixture non creuse : le fake reader **doit** recevoir le `accountID` de la session, sinon
   le test passe sur un reader qui ignore le scope — mémoire `hollow-test-fixtures`).
3. Intégration `clickhouse` : déjà couverte par `TestCDRCurrentScopedToAccount`
   (`cdr_integration_test.go:151`), rien à ajouter.
4. Test de câblage `TestNewSMPPAppBuildsTheWholeGraph` : lit par `reflect` le champ non exporté
   `opts.MessageReader` du listener construit (aucune surface de production ajoutée) ; mutation :
   retirer l'option de `wiring.go` → tombe.
5. `make check` vert.

## Hors périmètre

`replace_sm`, `data_sm` (non supportés, spec §5.1). La bascule `query_sm_enabled` (step-025) et son
réglage Admin (step-390). Un cache des réponses `query_sm` : le throttle suffit tant que le p99 de
`Current` ne le contredit pas.
