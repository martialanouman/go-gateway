# step-260e — Le produce Kafka n'a aucune borne : `KAFKA_PRODUCE_TIMEOUT`, et l'ingest REST en hérite

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-280 (la campagne NFR mesure l'ingest)

## Pourquoi cette fiche existe

L'audit du 2026-09-03 a vérifié que `internal/storage/kafka/producer.go` ne pose que quatre options
kgo (`SeedBrokers`, `RequiredAcks AllISR`, `ProducerBatchMaxBytes`, `DialTimeout`) et que `Produce`
est un `ProduceSync(ctx)` dont **la seule borne est le contexte de l'appelant** — `options.go` le dit
noir sur blanc : « never a produce or fetch deadline ». Or, dans franz-go v1.21.5, `RecordRetries` est
illimité par défaut et `RecordDeliveryTimeout` vaut 0, c'est-à-dire désactivé : un broker injoignable
fait réessayer un record sans fin.

Le chemin REST propage un contexte de requête **nu** (`restapi/messages.go` → `ingest.Accept` →
`Produce`), et `http.Server` n'a que `ReadHeaderTimeout` : un broker qui n'acquitte pas retient un
handler, une goroutine et un slot de connexion tant que le client reste. Le chemin SMPP, lui, borne le
même appel à 15 s en dur (`internal/smpp/session/session.go:35-38`) et répond `ESME_RSUBMITFAIL`.

## Ce que l'exploration a établi

- kgo v1.21.5 (`pkg/kgo/config.go`) : `RecordDeliveryTimeout` (`:1437`) — défaut 0 = désactivé,
  **plancher 1 s** quand il est non nul (`:363-368`, le client refuse en dessous) ; à l'expiration,
  `ErrRecordTimeout` (`errors.go:255`), traversé par `errors.Is` ; `ProduceRequestTimeout` (`:1330`) —
  défaut 10 s, c'est le `TimeoutMillis` de la requête côté broker (attente des acks ISR) ;
  `RequestTimeoutOverhead` — défaut 10 s, client-wide, « tue la connexion ».
- `config.Kafka` n'a **aucun** champ producteur ; `kafkaCapacityProblems` valide les leviers de fetch
  avec le plancher kgo de `FetchMaxWait` comme modèle (`config.go:793+`).
- Un seul `kafka.NewProducer` sert six binaires : rest-api, smpp-server, router, connector-pool,
  mo-dlr-router, mt-replay. Toute option s'applique en bloc, y compris aux produces **fail-closed avant
  commit d'offset** (connectorpool `mt.outcome`, reroute, dead-letter, drainer, mo-dlr-router).
- Vérifié : une erreur de `Produce` devient `errs.ErrServiceUnavailable` (`ingest.go:60-62`) → **503
  `service_unavailable`** en REST, `ESME_RSYSERR` en SMPP, `Retryable: true`. Aucun code neuf.
- Patron de test d'option : `capacity_internal_test.go` (`assertOpt` via `cl.OptValue`, qui rend la valeur
  **résolue** — une option non passée rend le défaut de la lib et le test tombe).
- Outillage de panne : `tcpproxy.Cut` **ferme** une connexion, il n'avale pas ; pas de `kafkatest.Cuttable`
  (le broker Redpanda est partagé par package). Un broker muet (TCP accepté, jamais de réponse) n'est pas
  reproductible aujourd'hui.

## Design arrêté

**Une variable, deux options.** `config.Kafka.ProduceTimeout` (`KAFKA_PRODUCE_TIMEOUT`, défaut **5 s**),
câblée dans un nouveau `producerOpts(cfg)` (`options.go`, à côté de `consumerOpts`) sur :
- `kgo.RecordDeliveryTimeout(T)` — le temps total qu'un record peut passer dans le client : broker
  injoignable, métadonnées indisponibles, retries, élection de leader ;
- `kgo.ProduceRequestTimeout(T)` — l'attente broker des acks ISR. Sans lui, un ISR en retard retient la
  requête 10 s, répond `REQUEST_TIMED_OUT`, et seulement alors le record timeout est évalué : la borne
  effective serait T + 10 s > 15 s.
Pour un opérateur, les deux sont « le timeout du produce » ; une variable, deux options, documentées.
Pas `RecordRetries` (un compte, pas un temps ; laissé illimité, borné par T). Pas
`RequestTimeoutOverhead` : client-wide, sémantique « tue la connexion », non mesuré — résidu nommé.

**Défaut 5 s.** C'est une borne de panne, pas de nominal (le p99 d'un produce se compte en ms) : elle
couvre une élection de leader ordinaire ; elle reste sous les 15 s du chemin SMPP pour que celui-ci
réponde `RSYSERR` avec sa cause plutôt que par son propre timeout ; elle reste au-dessus du plancher kgo.

**REST : pas de timeout HTTP en plus.** Un `http.TimeoutHandler` répondrait 503 pendant qu'un produce
peut encore aboutir : message durablement accepté **et** client informé d'un échec → doublon au retry
client. La borne vit dans le produce ; le handler rend le 503 existant. Le chemin idempotent libère déjà
son slot sur erreur.

**Consommateurs fail-closed : une valeur longue, par déploiement.** Un record expiré → erreur → offset
non commité → redélivrance. Pour `mt.outcome` le produce est *après* `submit_sm` : une redélivrance est
un **SMS en double** (ADR-0012). Aujourd'hui (illimité) le pool attend et ne duplique pas ; avec T court,
une élection de leader fabriquerait des doublons. Donc **connector-pool-svc et mo-dlr-router-svc
déploient `KAFKA_PRODUCE_TIMEOUT` ≈ 30 s** : là, la borne ne sert qu'à ne pas épingler un shard et le
drain pour toujours. Écrit dans le godoc du champ ; step-270 (manifests) le lira là.

**Validation.** `kafkaCapacityProblems` : `ProduceTimeout < 1s` (zéro et négatif compris) → problème
qui nomme `KAFKA_PRODUCE_TIMEOUT` et le plancher kgo. `producerOpts` garde le contrat « zéro dans un
struct literal = défaut de la bibliothèque » (`options.go:9-16`).

**`StreamProducer`** (best-effort, `TryProduce`, tampon 256) : non touché ; suite éventuelle.

## Chaîne de preuves — le rouge d'abord, la mutation ensuite

1. `internal/config/config_test.go` — `TestKafkaProduceTimeoutFloor` sur le modèle de
   `TestKafkaFetchMaxWaitFloor` (1s accepté, 999ms / 0s / -1s refusés en nommant la variable) ;
   `knownVars` et les tableaux d'env complets gagnent `KAFKA_PRODUCE_TIMEOUT`. Rouge : champ inexistant.
2. `internal/storage/kafka/capacity_internal_test.go` — `TestProducerAppliesTheProduceTimeout` :
   `assertOpt(producer.cl, kgo.RecordDeliveryTimeout, …)` et `kgo.ProduceRequestTimeout` sur la valeur
   de `levers()` ; `TestAnUnsetLeverKeepsTheLibraryDefault` étendu (`0s` et `10s` sur un producteur nu).
3. `internal/storage/kafka/produce_timeout_internal_test.go` —
   `TestProduceFailsWithinTheProduceTimeoutWhenNoBrokerAnswers`, **sans conteneur** : seed = un port
   loopback qu'on vient de libérer (jamais `127.0.0.1:1`, rien ne garantit que rien n'y écoute) ;
   `Timeout 200 ms`, `ProduceTimeout 1 s` (le plancher : la suite ne ralentit que d'une seconde) ;
   contexte de 5 s ; assertions : erreur non nulle, `errors.Is(err, kgo.ErrRecordTimeout)`, durée < 3 s,
   `ctx.Err() == nil`. Mutation « retirer `RecordDeliveryTimeout` » : l'erreur devient
   `context.DeadlineExceeded` à 5 s — le test tombe pour deux raisons visibles. Mutation
   « `ProduceRequestTimeout` seul » : tombe aussi (ce test prouve le record timeout ; l'autre option est
   prouvée par `OptValue`).

## Documentation dans la même PR

`options.go:21-22` (« never a produce or fetch deadline » devient faux : réécrire) ; `config.go` godoc de
`Timeout` (« and, later, client operations » → « each broker dial ») ; godoc de `ProduceTimeout` : les
deux options, le plancher, la consigne par service, et le **résidu nommé** — un broker qui accepte le TCP
et ne répond jamais n'est détecté qu'après `3 × RequestTimeoutOverhead + T` ≈ 35 s sur la première
requête d'une connexion (`broker.go:1703`), et un record idempotent déjà parti n'est pas annulable par
le contexte avant la réponse ou la mort de la connexion (`producer.go:541-549`) : le SMPP répondrait
alors à 35 s, pas 15. Suite si le chaos le montre : `RequestTimeoutOverhead` sur le client producteur
seul (il a son propre `kgo.Client`).

## Commits

1. Cette fiche.
2. `config` : champ + plancher + tests.
3. `kafka` : `producerOpts` + tests `OptValue`.
4. `kafka` : test comportemental sans broker.
5. `docs` : godocs ; fiche → `tasks-done/`.

## Arbitrages que la spec ne tranche pas — écrits ici

- **Valeur par service** : 5 s pour l'ingress (rest-api, smpp-server, router), ≈ 30 s pour les
  consommateurs fail-closed (connector-pool, mo-dlr-router). La spec ne parle que du p99 nominal.
- **`RequestTimeoutOverhead`** pour le broker muet : sans reproduction possible, à mesurer en chaos.
- **`StreamProducer`** : purger les frames périmées avec le même levier — suite éventuelle.

## Definition of Done

- [ ] `make check` vert
- [ ] sans broker, `Produce` revient en ≈ T avec `kgo.ErrRecordTimeout`, sans expirer le ctx appelant
- [ ] `KAFKA_PRODUCE_TIMEOUT < 1s` est refusé au boot en nommant la variable
- [ ] `OptValue` confirme les deux options sur le producteur durable, et le défaut kgo quand le champ est zéro
- [ ] chaque rouge lu, chaque mutation vue tomber, citée dans la PR
- [ ] `options.go` ne dit plus « never a produce deadline » ; le godoc du champ porte la consigne par service

## Hors périmètre

Un timeout HTTP côté REST ; `RequestTimeoutOverhead` ; `RecordRetries` ; le `StreamProducer` ; la valeur
SMPP de 15 s en dur (`InboundSubmitTimeout`, sans variable d'environnement — constat de l'audit, fiche à
part si on veut l'exposer).
