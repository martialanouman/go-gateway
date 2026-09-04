# step-260e — Le produce Kafka n'a aucune borne : `KAFKA_PRODUCE_TIMEOUT`, et l'ingest REST en hérite

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** LIVRÉE (2026-09-04)
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

## Design arrêté (premier jet — révisé ci-dessous après revue)

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

## Design révisé après revue (2026-09-04) — ce qui remplace le premier jet

La revue de correction a vérifié trois mécanismes de kgo v1.21.5 que le premier jet ignorait, et un
arbitre à contexte neuf a tranché (spec muette ; ADR-0012 et ADR-0014 comme cadre) :

1. **Un lot idempotent en vol n'est pas bornable.** `RecordDeliveryTimeout` est évalué avant l'écriture
   d'une requête, après une réponse, et par un minuteur dédié dans `waitUnknownTopic` ; un lot dont la
   requête est partie sans réponse ne peut être échoué ni par le timeout ni par le contexte
   (`sink.go:1749`, `:2134`, `:1359`, `:303`). Une réponse `REQUEST_TIMED_OUT` /
   `NOT_ENOUGH_REPLICAS_AFTER_APPEND` pose `unsureIfProduced`, jamais remis à faux (`sink.go:1052`) :
   ce lot est retenté jusqu'à réponse définitive. C'est le prix de l'absence de doublon côté broker.
   → **Q1 : `RecordDeliveryTimeout` seul, sans `AllowIdempotentProduceCancellation`**, qui ouvrirait
   une troisième cause de duplication à l'ingress (un `mt.inbound` annulé puis atterri, re-soumis par le
   client avec un `message_id` neuf = deux SMS) que la spec §6.7 vient de fermer à deux. Le godoc de
   `Produce` dit ce que T borne vraiment : un record jamais émis ou dont la dernière tentative a reçu
   une réponse retriable définie. Une requête en vol est bornée par la read deadline kgo,
   `RequestTimeoutOverhead + TimeoutMillis` = 10 s + 10 s (`client.go:816-828`).
2. **Coupler `ProduceRequestTimeout` à T ne borne rien et aggrave.** À 5 s, un ISR en retard rend
   `REQUEST_TIMED_OUT` plus tôt → lot `unsureIfProduced` plus fréquent, sur un cluster sain (rolling
   upgrade broker : le follower traîne jusqu'à `replica.lag.time.max.ms`, 30 s). C'est une propriété du
   cluster, pas un SLA d'ingress. → **Q2 : découplé, laissé au défaut kgo 10 s, non exposé.** Le premier
   jet et son test `OptValue` sont corrigés : le test prouve désormais que l'option **reste** au défaut.
3. **Le routeur est fail-closed lui aussi** (`router.go:231` : produce `mt.routed` avant le commit de
   `mt.inbound`, cause n° 2 de duplication selon ADR-0014) : le classer « ingress à 5 s » était une
   erreur. Et un godoc n'est pas une garde. → **Q3 : une option de politique par binaire**,
   `kafka.ForFailClosedConsumer()`, passée à `NewProducer` dans le `wiring.go` de **router-svc,
   connector-pool-svc, mo-dlr-router-svc** et dans `cmd/mt-replay` : elle impose une constante
   `FailClosedProduceTimeout = 30 s` en ignorant `KAFKA_PRODUCE_TIMEOUT` — assez long pour une élection
   de leader et l'attente ISR. Plus long n'achète rien : les consommateurs ne bloquent pas le rebalance
   pendant un poll (`BlockRebalanceOnPoll` kgo non posé, `consumer.go:63-70`), donc un rebalance
   survenant pendant le blocage redélivre de toute façon — suite possible. Non surchargeable par
   l'env : un opérateur ne peut pas mal régler ce qu'il ne peut pas régler. `KAFKA_PRODUCE_TIMEOUT`
   ne gouverne plus que **rest-api-svc et smpp-server-svc**. Prouvé par `OptValue` (l'option gagne sur
   un `cfg.ProduceTimeout` de 1 s) et par une assertion dans chaque test de câblage concerné.

**Correction de la fiche step-270** : la consigne « ≈ 30 s par déploiement » disparaît (elle devient
fausse) ; il n'y a plus rien à porter dans les manifests pour ce levier.

**Résidu corrigé** : « 3 × RequestTimeoutOverhead + T ≈ 35 s » était le chemin acks=0
(`broker.go:1662-1703`) ; pour acks=all la read deadline d'un produce est 10 s + 10 s. Le broker muet
reste non reproductible ; `RequestTimeoutOverhead` sur le client producteur reste la suite si le chaos
le montre.

## Chaîne de preuves — le rouge d'abord, la mutation ensuite

1. `internal/config/config_test.go` — `TestKafkaProduceTimeoutFloor` sur le modèle de
   `TestKafkaFetchMaxWaitFloor` (1s accepté, 999ms / 0s / -1s refusés en nommant la variable) ;
   `knownVars` et les tableaux d'env complets gagnent `KAFKA_PRODUCE_TIMEOUT`. Rouge : champ inexistant.
2. `internal/storage/kafka/capacity_internal_test.go` — `TestProducerAppliesTheProduceTimeout` :
   `assertOpt(producer.cl, kgo.RecordDeliveryTimeout, …)` sur la valeur de `levers()`, et
   `ProduceRequestTimeout` **resté au défaut 10 s** (Q2) ; `TestAnUnsetLeverKeepsTheLibraryDefault`
   étendu (`0s` sur un producteur nu) ; `TestForFailClosedConsumerIgnoresTheEnvBound` : l'option
   impose 30 s sur un `cfg.ProduceTimeout` de 1 s (Q3). Puis une assertion dans
   `TestNew…BuildsTheWholeGraph` de router-svc, connector-pool-svc et mo-dlr-router-svc.
3. `internal/storage/kafka/produce_timeout_internal_test.go` —
   `TestProduceFailsWithinTheProduceTimeoutWhenTheBrokerIsUnreachable`, **sans conteneur** : seed = un port
   loopback qu'on vient de libérer (jamais `127.0.0.1:1`, rien ne garantit que rien n'y écoute) ;
   `Timeout 200 ms`, `ProduceTimeout 1 s` (le plancher : la suite ne ralentit que d'une seconde) ;
   contexte de 5 s ; assertions : erreur non nulle, `errors.Is(err, kgo.ErrRecordTimeout)`, durée < 3 s,
   `ctx.Err() == nil`. Mutation « retirer `RecordDeliveryTimeout` » : l'erreur devient
   `context.DeadlineExceeded` à 5 s — le test tombe pour trois raisons visibles. Il ne prouve que le
   chemin « record jamais émis » (minuteur dédié de `waitUnknownTopic`), et son nom le dit.

## Documentation dans la même PR

`options.go:21-22` (« never a produce or fetch deadline » devient faux : réécrire) ; `config.go` godoc de
`Timeout` (« and, later, client operations » → « each broker dial ») ; godoc de `ProduceTimeout` : une
option, ingress seulement, en quatre lignes ; godoc de `Produce` : ce que la borne couvre vraiment. La
fiche step-270 dit qu'il n'y a rien à porter dans les manifests pour ce levier.

**Résidu nommé — ici, pas dans le code.** Un broker qui accepte le TCP et ne répond jamais n'est
détecté qu'à la read deadline kgo, `RequestTimeoutOverhead + TimeoutMillis` = 20 s par tentative
(`client.go:816-828`), et un record idempotent déjà parti n'est pas annulable avant la réponse ou la
mort de la connexion. Non reproductible avec l'outillage actuel (`tcpproxy.Cut` ferme, il n'avale
pas). Suite si le chaos le montre : `RequestTimeoutOverhead` sur le client producteur seul.

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

- [x] `make check` vert (lint 0 issue, suite `-race` complète avec Docker et l'image du simulateur, govulncheck, contrats)
- [x] sans broker, `Produce` revient en ≈ T avec `kgo.ErrRecordTimeout`, sans expirer le ctx appelant —
      `TestProduceFailsWithinTheProduceTimeoutWhenTheBrokerIsUnreachable` (1,0 s mesuré sous `-race`)
- [x] `KAFKA_PRODUCE_TIMEOUT < 1s` est refusé au boot en nommant la variable — `TestKafkaProduceTimeoutFloor`,
      entrées de `TestLoadRejectsInvalid` ; défaut 5 s épinglé dans `TestCapacityLeverDefaults`
- [x] `OptValue` confirme le record timeout sur le producteur durable, `ProduceRequestTimeout` resté à 10 s, et le
      défaut kgo quand le champ est zéro — `TestProducerAppliesTheProduceTimeout`, `TestAnUnsetLeverKeepsTheLibraryDefault`
- [x] les quatre producteurs fail-closed portent la constante, hors de portée de l'env —
      `TestForFailClosedConsumerIgnoresTheEnvBound` + assertion dans `TestNew…BuildsTheWholeGraph` de router-svc,
      connector-pool-svc, mo-dlr-router-svc (mt-replay : sans test de câblage, câblé à vue)
- [x] chaque rouge lu, chaque mutation vue tomber :
      - rouges : `cfg.Kafka.ProduceTimeout undefined` ; `OptValue` = `0s` / `10s` (défauts lib) ;
        `ForFailClosedConsumer` / `app.producer` inexistants
      - mutation « plancher retiré » → `999ms`, `0s`, `-1s` acceptés (deux tests tombent)
      - mutation « `RecordDeliveryTimeout` retiré » → `context deadline exceeded` à 5,0 s, ctx expiré (trois assertions)
      - mutation « option `ForFailClosedConsumer` inerte » → `RecordDeliveryTimeout = 1s, want 30s` (kafka) et
        `producer delivery timeout = 0s, want 30s` (câblage router)
      - mutation « option retirée du câblage du pool » → `0s, want 30s`
      - mutation « défaut `50s` » → `Kafka.ProduceTimeout = 50s, want 5s`
- [x] `options.go` ne dit plus « never a produce deadline » ; le godoc du champ dit « ingress seulement » ; celui de
      `Produce` dit ce que la borne couvre et ne couvre pas

## Revue

Deux sous-agents en lecture seule (correction / tests et documents), puis un **arbitre à contexte neuf**
sur le fork de conception que la première revue a ouvert (trois mécanismes kgo vérifiés : lot en vol
non bornable, `unsureIfProduced` irréversible, read deadline = overhead + `TimeoutMillis`). Décisions
appliquées : record timeout seul, `ProduceRequestTimeout` découplé, option de politique par binaire,
le routeur reclassé fail-closed. Une seconde revue sur le delta des correctifs : aucun bloquant, quatre
Required documentaires appliqués (la constante ne promet plus une non-éviction que les consommateurs
n'ont pas ; `BlockRebalanceOnPoll` noté comme suite).

## Hors périmètre

Un timeout HTTP côté REST ; `RequestTimeoutOverhead` ; `RecordRetries` ; le `StreamProducer` ; la valeur
SMPP de 15 s en dur (`InboundSubmitTimeout`, sans variable d'environnement — constat de l'audit, fiche à
part si on veut l'exposer).
