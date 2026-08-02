# step-201 — Tuning de débit : partitions Kafka, batch ClickHouse, pool pgx

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-200 · **Bloque :** step-201b

## But
Régler les leviers de capacité — partitions Kafka, taille de batch ClickHouse, dimensionnement du pool
`pgx` — pilotés par config, et **construire les instruments qui rendent le réglage mesurable** : plafond
du pair de test, débit traversant, latence bout-en-bout.

Le **verdict NFR pleine échelle** (8 000 SMS/s soutenu, 15 000 en pic) n'est **pas** rendu ici : il
demande un environnement représentatif qui n'existe pas encore → `step-201b` (cf. `D1`).

## Périmètre (trois PRs — cf. `D0`)
**PR 1 — plafond du pair.** Mode injecteur `submit_sm` dans `test/load/bindgen` ; lecture du `/metrics`
du simulateur ; plafond mesuré et consigné.
**PR 2 — instruments de mesure.** Vérificateur de latence bout-en-bout par corrélation `message_id` ;
option `Idempotency-Key` dans le script k6 + stub observateur.
**PR 3 — les leviers.** `internal/config` : leviers de capacité via env, défauts de prod raisonnés ;
ajustements `internal/storage/{kafka,clickhouse,postgres}` ; provisionneur de topics ; run de référence
local documenté.

## Points d'implémentation clés
- **Établir le plafond du pair de test AVANT de régler quoi que ce soit.** Le run de référence se fait
  contre le simulateur SMSC (`internal/testutil/smscsim`, `make smsc-sim`) : c'est lui qui borne la
  mesure. Rien ne dit aujourd'hui qu'il tient 8 000 `submit_sm/s`, encore moins 15 000 — M8 l'a éprouvé
  sur l'injection de pannes, jamais sur le débit. Si son plafond est en dessous de la cible, chaque
  chiffre produit ici mesure le simulateur et non la passerelle, et le tuning vise une contrainte
  artificielle. Le dépôt n'a rien pour le vérifier : `smpp-bindgen` ouvre des binds mais **ne soumet
  rien**. Trancher comment on établit ce plafond (injecteur `submit_sm`, plusieurs instances du
  simulateur, ou lecture de la saturation) fait partie du design de cette step.
- **Mesurer aussi le chemin `Idempotency-Key`.** `internal/restapi/messages.go` bascule sur
  `submitIdempotent` quand l'en-tête est présent, ce qui ajoute deux allers-retours Redis (`Reserve`,
  `Finalize`) autour de la publication Kafka. Régler les leviers sans cet en-tête optimise un chemin que
  les clients qui retentent n'empruntent pas : les NFR seraient déclarés tenus sur le cas favorable.
  Le script k6 de step-200 ne l'émet pas encore — l'ajouter en option, désactivée par défaut.
  **Piège :** la clé doit être unique par itération (128 caractères max, cf. contrat). Une clé constante
  ferait retourner le résultat mémorisé de la première requête et mesurerait le cache d'idempotence, pas
  le chemin idempotent.
- `mt.routed` est shardé (`shard_index = hash(message_key) % bind_pool_size`, §1.6) : le nombre de
  partitions doit couvrir le parallélisme cible sans surdécoupage.
- ClickHouse : batch/flush pour éviter la mutation par message (§1.10) — équilibrer latence CDR vs débit.
- `pgxpool` : dimensionner selon la charge billing/contrôle ; éviter l'épuisement sous pic.
- **`ctx7`** avant d'ajuster une API `franz-go` / `clickhouse-go/v2` / `pgxpool`.
- Aucun réglage ne doit affaiblir un invariant (idempotence, ordre, non-fuite).

## Tests (écrits dans la même PR)
- Config : les leviers se parsent et s'appliquent (test unitaire config).
- Le plafond du pair de test est **mesuré et consigné**, et le run de référence se situe en dessous :
  un run de référence au niveau du plafond du simulateur ne prouve rien de la passerelle.
- Avec l'en-tête activé, deux itérations émettent deux clés d'idempotence différentes ; désactivé,
  aucun en-tête n'est émis.
- Le run de référence local tient l'**état stationnaire** au seuil de `D2`, et sait échouer en dessous.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] plafond du pair de test mesuré et consigné (`D3`)
- [ ] latence bout-en-bout p99 **mesurée par corrélation `message_id`** (`D4`), pas déduite de l'ingestion
- [ ] run de référence local à l'état stationnaire ≥ 1 000 msg/s traversants (`D2`), disjoncteur fermé
- [ ] verdict NFR pleine échelle **explicitement non rendu ici**, reporté sur `step-201b` (`D1`)

## Hors périmètre
Verdict NFR pleine échelle → step-201b. Chaos → step-202/203. Sécurité → step-204+.

---

## Design arrêté (2026-08-02)

### D0 — Trois PRs, pas une
PR1 plafond du pair · PR2 instruments de mesure · PR3 leviers + run de référence.

**Raison.** La step porte un injecteur SMPP, un vérificateur de latence, une option k6, un provisionneur
de topics et le remaniement de trois clients de storage. En une PR, aucun relecteur ne peut accepter ou
refuser une pièce isolément. L'ordre n'est pas cosmétique : PR3 règle des leviers qui **échangent de la
latence bout-en-bout contre du débit** — les régler sans les instruments de PR1/PR2, c'est optimiser à
l'aveugle la seule métrique que le réglage dégrade.

### D1 — Le verdict NFR pleine échelle n'est pas rendu ici
step-201 livre les leviers, les instruments et un run de référence local. Le verdict 8 000/s soutenu et
15 000 en pic part sur une fiche neuve **step-201b**, dépendant de step-201 **et** step-207, bloquant
step-208.

**Raison.** Le débit soutenu de la spec est **traversant**, pas un débit d'acceptation : §1.2 distingue
explicitement les deux *latences* (ingestion « soumission → mise en file », bout-en-bout « soumission →
tentative de remise SMSC ») et laisse le *débit* non qualifié ; §2.4 le compte « × 2 sens » ; §2.5
dimensionne sur « routage + anti-spam + encodage », au milieu du pipeline. Accepter 8 000/s dans
`mt.inbound` en n'en sortant que 500 n'est pas un débit — c'est une file qui grossit, ce que le mot
« soutenu » exclut.

Conséquence arithmétique : 8 000 SMS/s × 1,3 segment (§2.1) ≈ **10 400 `submit_sm/s`** à absorber en
sortie. À porter sur une machine de 14 cœurs qui tient déjà 9 services, 4 magasins (dont Redpanda bridé
à `--smp=1`), le simulateur et l'injecteur. La spec §2.5 dimensionne la cible à 8–16 vCPU de workers
*dédiés* plus un Kafka répliqué 3 : le matériel manque d'un ordre de grandeur. Un « 8 000/s tenu »
mesuré là ne validerait rien, et un échec ne condamnerait rien.

> **Amendé après la mesure de `D3`.** Ce paragraphe estimait ~200 `submit_sm/s` par bind, donc
> « ≥ 52 binds », et faisait du pair un facteur limitant plausible. La mesure dit autre chose :
> **136–171/s par bind** (le modèle était optimiste de 15 à 30 %), donc **≥ 80 binds** — et surtout, le
> pair **n'a jamais saturé**, jusqu'à 43 498 `submit_sm/s` à 320 binds sans une seule issue non-`success`.
> Le plafond du pair n'est donc **pas** le facteur limitant, et le tuning ne visera pas une contrainte
> artificielle. Ce qui manque reste le CPU de la passerelle elle-même : `D1` tient, mais pour cette
> seule raison.

step-201b dépend de step-207 parce que les manifests Kubernetes sont précisément ce qui rend un
environnement représentatif instanciable.

### D2 — Le run de référence local prouve l'état stationnaire, seuil 1 000 msg/s
Critère, tout en même temps sur un palier de **≥ 60 s** : débit traversant ≥ **1 000 msg/s** · débit de
sortie (`smsc_submit_sm_received_total`) égal au débit d'acceptation k6 à la marge de segmentation près
· **lag consumer Kafka plat**, pas croissant · p99 ingestion < 250 ms · 0 erreur · disjoncteur fermé ·
le tout **sous** le plafond mesuré en `D3`.

**Raison.** Le seuil n'est pas arbitraire : 1 000 msg/s est la **borne basse du modèle par-worker de la
spec** (§2.5, « ~1 vCPU soutient 1 000–2 000 msg/s »), donc le run vérifie l'hypothèse sur laquelle tout
le dimensionnement repose. S'il échoue, soit le modèle est faux, soit le code a un défaut — les deux
méritent d'être sus. Un run sans seuil produirait toujours un nombre et ne pourrait jamais échouer : la
DoD n'aurait plus de porte, seulement un rapport.

L'égalité entrée/sortie et le lag plat sont ce qui distingue un débit d'une file qui se remplit. Sans
eux, le critère retomberait sur l'acceptation — exactement l'erreur que `D1` écarte.

### D3 — Plafond du pair : injecteur `submit_sm` dans `bindgen`, mesuré sur le `/metrics` du simulateur
Mode optionnel du package existant `test/load/bindgen` (`Config.Submit *SubmitConfig`, `nil` = comportement
actuel strictement inchangé), démarrant sur la barrière `OnAllBound` déjà présente. Émission **fenêtrée**
(16–64 par session, pas de tour-par-tour), balayage du nombre de binds (10/20/40/80), paliers ≥ 60 s, au
**profil du run de référence** (`HealthyConfig`, 5 ms).

Le débit n'est **pas** compté côté injecteur : il se lit sur `GET :9000/metrics` du simulateur, par deux
scrapes espacés de `smsc_submit_sm_received_total`. `smsc_submit_sm_outcome_total` disqualifie tout
palier portant des erreurs.

> **Corrigé après mesure.** Ce paragraphe disait aussi que `smsc_served_latency_seconds` distinguerait
> « le pair sature » de « l'injecteur ne pousse pas ». **C'est faux** : le simulateur observe la latence
> que son scénario a *décidée*, pas une durée mesurée (`ObserveServedLatency` reçoit
> `decision.LatencyMS`). L'histogramme a lu 5 ms à plat de 10 à 320 binds. Les seuls signaux de
> saturation sont `smsc_submit_sm_outcome_total` et l'inflexion de la courbe. Le godoc de
> `smscmetrics` portait la même affirmation fausse — corrigé aussi.

Deux chiffres consignés, pas un : la **courbe plafond-vs-binds**, et le plafond **au nombre de binds du
run de référence** — c'est sous ce dernier que `D2` doit se situer.

**Raison.** Le compteur est côté pair : il mesure ce que le simulateur a réellement absorbé, pas ce que
l'injecteur croit avoir envoyé. Le tour-par-tour mesurerait la latence, pas un plafond. Un plafond
relevé sous un autre profil de latence ne bornerait pas le run de référence. Écartés : « lire la
saturation » (le calcul ~200/s/bind est un modèle, pas une mesure — la fiche exige « mesuré et
consigné ») ; plusieurs instances du simulateur (déplace le plafond sans le connaître, change la
topologie sous test, et charge davantage la ressource déjà rare).

Le mode réutilise les flags existants — il n'aggrave pas la dette `-password` sur `argv` consignée en
step-208.

### D4 — Latence bout-en-bout : dans cette step, PR séparée, corrélée sur les PDU du simulateur
Le vérificateur corrèle par `message_id` l'horodatage d'acceptation et l'horodatage de sortie lu dans le
**recorder du simulateur**, pas dans le CDR ClickHouse.

**Raison — pourquoi dans cette step.** `tasks-done/step-200.md` (`D6`) et `test/load/README.md`, tous
deux mergés, le nomment prérequis de step-201 ; le commit `4f7d764` triait les constats de revue de
step-200 et ne portait pas sur lui — son silence ne vaut pas révocation. Surtout, la DoD « budgets de
latence respectés » est **incochable** sans lui : sans corrélation de sortie, seul le budget d'ingestion
est mesurable, et cocher la case certifierait un budget jamais mesuré.

**Raison — pourquoi le recorder et pas le CDR.** La spec définit le span comme « soumission → tentative
de remise SMSC ». Le CDR `enroute_at` inclut le lag de projection ClickHouse : « < 2 s » ne certifierait
plus le même intervalle, et le seuil bougerait avec un levier de PR3 (`D6`) sans que la passerelle ait
changé. *(La phrase qui suivait — « `/metrics` ne peut pas servir ici : un compteur agrégé ne se
corrèle à aucun `message_id` » — est retirée : vraie mais sans objet, puisque l'amendement ci-dessous
supprime le besoin de corrélation.)*

> **Amendé (2026-08-02) — la corrélation n'est pas nécessaire, et le recorder ne la permet pas.**
>
> Trois faits vérifiés dans le code invalident la lettre de cette décision :
>
> 1. **`recorder.RecordedPDU` du simulateur ne porte AUCUN horodatage** — `Index`, `MessageID`,
>    adresses, `ShortMessage`, `PerBindClock` (un compteur logique, pas du temps). Corréler sur les PDU
>    du recorder ne donne donc pas d'instant de sortie. Il faudrait modifier le simulateur, qui est un
>    dépôt séparé.
> 2. **Le `message_id` de la passerelle ne traverse pas jusqu'au `submit_sm`** : `buildSubmit`
>    (`internal/connectorpool/mapping.go:25-59`) ne pose que les adresses, l'encodage et le corps ; le
>    seul TLV jamais posé est `message_payload`. Rien à corréler.
> 3. **La voie CDR est pire qu'imprécise, elle est fausse** : `cdrRow` pose `SubmittedAt` et jamais
>    `DeliveredAt`, et `appendEvents` (`internal/storage/clickhouse/cdr.go:194-197`) retombe donc sur
>    `SubmittedAt`. `cdr_events.at` de la ligne `enroute` **est** l'heure d'acceptation : le span y vaut
>    identiquement 0, avec un p99 rassurant et faux.
>
> **Ce qui remplace.** Les deux bouts du span sont dans le même processus — `env.SubmittedAt` est
> immuable et propagé jusqu'au `connectorpool`, et la tentative de remise a lieu là. Aucune corrélation
> n'est nécessaire : la passerelle peut mesurer le span elle-même.
>
> Et l'instrument existe déjà, mort : **`message_e2e_duration_seconds`** est déclarée dans
> `internal/observability/metrics/catalog.go:109`, enregistrée, exposée — et **observée nulle part
> hors de ses propres tests**. Son `Help` dit « Time from submission to the final SMSC outcome ». Une
> métrique déclarée et jamais alimentée est la pire des gardes mortes : un tableau de bord l'affiche
> comme « aucun problème ».
>
> Le vérificateur devient donc : **câbler cette métrique** au site de soumission, puis la lire sur le
> `/metrics` de la passerelle — même patron que `smscmetrics` pour le simulateur. Strictement meilleur
> sur tous les axes : pas de corrélation, pas de modification d'un dépôt tiers, pas de polling ni
> d'erreur systématique ajoutée à chaque échantillon, et une valeur **en production** et pas seulement
> pour le harnais. Le span mesuré reste celui de la spec.

### D5 — Les leviers exposés, et ceux qu'on écarte
Défaut de chaque variable = **comportement actuel effectif**, sauf `POSTGRES_MIN_CONNS`. La PR est neutre
par défaut ; les boutons existent pour la campagne. Défauts des bibliothèques vérifiés dans la source des
modules, pas de mémoire.

| Variable | Défaut | Raison |
|---|---|---|
| `KAFKA_FETCH_MIN_BYTES` | `1` | Le levier de taille de batch ClickHouse (`D8`). À 1, latence minimale à faible trafic ; on le monte en charge pour grossir les inserts CDR. |
| `KAFKA_FETCH_MAX_WAIT` | `5s` | Borne de latence du batch. Minimum franz-go : 10 ms — à valider. |
| `KAFKA_FETCH_MAX_BYTES` | `50MiB` | Plafond mémoire par poll et taille max d'un insert en drainage de backlog. |
| `KAFKA_TOPIC_PARTITIONS` | `12` | Consommé par le provisionneur (`D7`). Jusqu'à 12 pods actifs par groupe ; divisible par 1/2/3/4/6. |
| `KAFKA_TOPIC_PARTITIONS_OVERRIDES` | vide (`topic=n,…`) | Honore le « par topic » de la fiche sans N variables. |
| `KAFKA_TOPIC_REPLICATION_FACTOR` | `3` | Défaut de prod raisonné (spec §2.5) ; le local passe `1` explicitement. |
| `CLICKHOUSE_MAX_OPEN_CONNS` | `10` | Défaut lib (`MaxIdleConns+5`). Pas pour le writer CDR (une boucle) mais pour `admin-api-svc` : search-messages + export concurrents. |
| `CLICKHOUSE_MAX_IDLE_CONNS` | `5` | Défaut lib. |
| `POSTGRES_MIN_CONNS` | `2` | **Seule exception au « défaut = actuel ».** `MinConns` n'est jamais fixé aujourd'hui → 0 → aucune connexion pré-chauffée, donc rafale d'établissements au pic. C'est le « revisit » que `pool.go:14-16` réclame nommément. |

**Correctif, pas levier :** `KAFKA_TIMEOUT` est **déjà un bouton mort** — la variable est lue, validée,
et n'atteint aucun client. La câbler sur `kgo.DialTimeout`.

**Piège à valider :** `FetchMaxBytes` est **par broker** (le client bufferise `brokers × maxBytes`) et
doit rester **≤ `BrokerMaxReadBytes`** (100 MiB par défaut) — au-delà, franz-go **refuse de démarrer**
(`config.go:331`). La validation de config doit attraper ça, pas le boot en production.

**Écartés, avec la raison :**
- `ProducerLinger` — avec `ProduceSync` à la frontière d'ACK, le batching vient de la concurrence des
  appelants (franz-go coalesce pendant qu'une requête est en vol). Un linger > 0 n'ajouterait que de la
  latence au 202 et au `submit_sm_resp`. **Aucune variable producer dans cette step.**
- `ProducerBatchMaxBytes` (16 MiB) — plafond jamais atteint ; on ne tourne pas un cap qu'on ne touche pas.
- Compression producer — payloads SMS minuscules, gain non prouvé.
- `SessionTimeout` / `RebalanceTimeout` / `HeartbeatInterval` / `MaxConcurrentFetches` — leviers de
  stabilité, pas de capacité ; mal réglés ils fabriquent des tempêtes de rebalance.
- ClickHouse `BlockBufferSize` / compression / `ConnMaxLifetime` — micro-réglages sans mesure qui les
  justifie.
- ClickHouse `Settings` en passthrough — **interdit** : c'est la porte dérobée vers `async_insert` (`D8`).
- Redis `PoolSize` / `MinIdleConns` — pertinent pour le chemin `Idempotency-Key`, mais **hors périmètre
  de la fiche** (elle nomme kafka/clickhouse/postgres). À rouvrir seulement si le run de référence montre
  de l'attente de pool. Follow-up, pas contrebande.

`knownVars` dans `config_test.go` est la liste exhaustive des variables lues : la mettre à jour fait
partie de l'unité, pas d'un nettoyage ultérieur.

### D6 — Ce qui reste non configurable (invariants)
`RequiredAcks(AllISRAcks())` (durabilité) · `DisableAutoCommit()` (at-least-once) · `ProduceSync` (c'est
la **définition** de l'ACK durable qui autorise le 202 et le `submit_sm_resp`) · le producer idempotent
kgo (ne jamais exposer `DisableIdempotentWrite`) · **l'alignement poll = insert = commit** · pas
d'`async_insert` exposable.

**Raison.** Ces six ne sont pas des réglages conservateurs, ce sont des frontières de contrat. Les deux
derniers sont les moins évidents et les plus dangereux : `wait_for_async_insert=0` désaligne
silencieusement l'ACK d'insert et le commit Kafka — le CDR se perd sans qu'aucune erreur ne remonte.

### D7 — Partitions Kafka : un provisionneur en `cmd/`, jamais au boot d'un service
`cmd/kafka-provision` + `make kafka-topics` : `kadm.CreateTopics` / `CreatePartitions`, idempotent,
**extension seule, jamais de réduction** (Kafka ne sait pas rétrécir).

**Raison.** Sans lui, `KAFKA_TOPIC_PARTITIONS` serait un **bouton mort** : rien dans le dépôt ne crée de
topic hors des tests (`kafkatest.go:44`, 4 partitions), et l'auto-création Redpanda en local en donne
**1** — le run de référence mesurerait un parallélisme inter-pods de 1 et attribuerait le plafond à la
passerelle. Le provisionneur au boot d'un service est la mauvaise variante : courses entre réplicas,
échec partiel au démarrage, et personne ne sait quel service possède quel topic. Le dépôt a déjà le bon
patron pour du schéma d'infrastructure — `make migrate`, un acte opérateur délibéré.

À documenter : étendre `mt.routed` re-mappe clé→partition. Bénin ici (le sharding par bind est
applicatif, calculé après réception sur `rec.Key`), mais à faire hors pointe. `kafkatest` garde ses 4
partitions ; step-207 enveloppera l'outil dans un Job.

### D8 — Batch ClickHouse : aucun buffer client, piloté par le fetch Kafka
Le batch reste `poll Kafka = PrepareBatch/Send = commit`. Sa taille se règle par `KAFKA_FETCH_MIN_BYTES`
et `KAFKA_FETCH_MAX_WAIT`.

**Raison.** Un buffer/flush côté client désaligne commit Kafka et flush ClickHouse : il faudrait tracer
quels offsets sont couverts par quel flush — une machine à états neuve sur le chemin de durabilité du
CDR. Le couple fetch donne déjà les deux régimes : latence bornée à faible trafic, gros batches naturels
en charge. Les deux `Send()` par poll (`cdr` puis `cdr_events`) sont 2 RTT par batch, invisibles à
quelques batches/seconde.

**Critère de réouverture, à consigner :** si la campagne montre du « too many parts » côté ClickHouse, la
réponse sera `async_insert` **serveur** avec `wait_for_async_insert=1` (batching côté serveur, durable),
jamais un buffer client.

### D9 — pgxpool
`POSTGRES_MIN_CONNS` exposé (défaut 2). `MaxConnLifetime` / `MaxConnIdleTime` / `HealthCheckPeriod`
**restent en dur**. `POSTGRES_MAX_CONNS` reste **une seule variable**, défaut **10** inchangé.

**Raison.** Les trois durées sont de l'hygiène de pool, pas de la capacité : personne ne les tournera
pendant une campagne, et 30 min / 5 min / 1 min sont sains — les exposer serait décoratif. Le
`POSTGRES_MAX_CONNS` unique pour 7 services est **déjà résolu par l'isolation par processus** : chaque
déploiement pose sa valeur. Inventer `ROUTER_POSTGRES_MAX_CONNS` serait une régression de patron. Le
défaut 10 ne bouge pas : le chemin chaud touche peu Postgres (autorité en cache, billing Redis-first) et
le monter à l'aveugle risque d'épuiser `max_connections` (100 par défaut) une fois multiplié par
7 services × réplicas. Les valeurs par service se décident dans les manifests de step-207, mesures en
main.

### D10 — Option `Idempotency-Key` du script k6
`IDEMPOTENCY=on|off`, défaut `off`, **toute autre valeur lève à l'init** — calqué sur le patron `PROFILE`
(`messages.js:61-66`). Quand c'est `off`, la clé est **absente de l'objet `params`**, jamais présente et
vide.

Clé : `` `k6-${RUN_SEED}-${exec.scenario.iterationInTest}` `` où `RUN_SEED` = horodatage ms en base36 +
6 caractères aléatoires base36. ~25 caractères, très en deçà des 128 du contrat.

**Raison.** `scenario.iterationInTest` est unique **par construction** sur tout le run, y compris en
exécution distribuée — là où tout pliage arithmétique de `(__VU, __ITER)` (comme le `% 10000` du
`msisdn()`) peut collisionner. Le `RUN_SEED` couvre le piège que la fiche ne nomme pas : la **rétention
Redis est de 24 h**, donc sans composant par run, deux runs du même jour rejoueraient les mêmes clés et
le second mesurerait le cache d'idempotence — exactement le défaut que la fiche interdit, à l'échelle du
run entier. Le préfixe `k6-` n'est pas décoratif : il rend les clés du harnais balayables dans Redis.

Une émission vide serait pire qu'un bug muet : le serveur traite `""` comme « pas d'idempotence »,
**silencieusement** — le harnais passerait vert en exerçant le chemin non idempotent.

### D11 — Tester l'option k6 sans infrastructure JS : le stub est l'observateur
Le dépôt n'a ni node_modules, ni jest ; la CI n'installe que Go et le binaire k6. On ne teste donc pas le
JavaScript, on **observe ses effets** côté `test/load/stub/stub.go`, qui reçoit déjà chaque requête.

Le stub gagne `-idempotency=ignore|require-unique|forbid` (défaut `ignore`, comportement actuel intact) :
`require-unique` rejette en 422 un en-tête absent, **vide**, > 128 caractères ou **déjà vu** ; `forbid`
rejette toute présence. Tests Go unitaires dans `stub_test.go`, et **trois runs** ajoutés à
`scripts/load-smoke.sh` sur le patron positif/négatif existant :

| Run | Attendu |
|---|---|
| `IDEMPOTENCY=on` contre `require-unique` | exit **0** — ~500 clés toutes distinctes |
| `IDEMPOTENCY` absent contre `forbid` | exit **0** — aucun en-tête émis |
| `IDEMPOTENCY=on` contre `forbid` | exit **99** |

**Raison.** Le troisième run est le seul qui empêche de débrancher l'observateur sans bruit : sans lui,
les deux positifs passeraient trivialement avec un stub qui ignore tout. C'est la doctrine déjà établie
en step-200 (`D3`) — le run négatif est la vraie assertion. Le critère de la fiche (« deux itérations,
deux clés ») est couvert au-delà de sa lettre : 500 itérations, assertion mécanique.

### D12 — Pollution Redis du chemin idempotent : acceptée, documentée, non « corrigée »
Un run `peak` crée ~900 000 clés à 24 h de TTL, ~100–150 Mo. On ne touche à rien côté serveur.

**Raison.** C'est le réalisme, pas la pollution : en production, des clients qui retentent rempliront
`idem:{accountID}:*` au même rythme, et cette empreinte **fait partie du chemin que l'option existe pour
mesurer**. La purger entre runs fausserait la mesure. Régler la prod pour le confort du harnais (TTL
configurable, purge d'API) serait ajuster le système au test.

À écrire dans `test/load/README.md` : le chiffre, son **cumul** sur 24 h, et le dimensionnement
`maxmemory` du Redis cible — sous pression, une éviction expulserait aussi sessions, token-buckets et
cache de solde, et le run mesurerait une tempête d'éviction ; en `noeviction`, ce seraient des échecs de
réservation. Plus le balai, possible grâce au préfixe de `D10` :
`redis-cli --scan --pattern 'idem:{<accountID>}:k6-*' | xargs -L 500 redis-cli UNLINK`.

> **Corrigé (2026-08-02).** Ce pattern était écrit sans les accolades. Elles font **partie de la clé** —
> `key()` produit `"idem:{" + accountID + "}:" + idemKey` (`internal/idempotency/idempotency.go:122`),
> un hash tag Redis Cluster qui garde les entrées d'un compte sur un même slot. Sans elles le `--scan`
> ne matche **rien**, et se lit comme « il n'y a rien à balayer ».

### D13 — Dépendance à une branche non mergée
Le design et les prérequis de cette step vivent sur `docs/step-201-prereqs` (commit `4f7d764` +
celui-ci), **absente de `main`**. Les branches de code de PR1/PR2/PR3 partent de là, pas de `main`.
À annoncer en tête de chaque corps de PR.

### Ce que PR1 gèle sans le résoudre (arbitré avec l'utilisateur après 3 tours de revue)

Quatre tours de revue sur PR1. Le motif, stable et instructif : **l'instrument d'origine n'a reçu aucun
constat sur les trois derniers tours** ; tous les défauts étaient dans les *gardes numériques* et les
*affirmations* ajoutées en réponse aux revues précédentes (tour 2 : 8 constats sur 9 dans les correctifs
du tour 1 ; tour 3 : 11 sur 12 dans ceux des tours 1–2). Le tour 4 a été cadré : deux bloquants plus
les trois constats les plus graves, puis arrêt.

Corrigés au tour 4 : un rejet du pair précédé d'un pli de courbe était effaçable (l'outil imprimait
« no tier shed » sur un balayage contenant un palier disqualifié pour rejet) · un pair qui **accepte
sans jamais répondre** produisait un chiffre publiable avec code de sortie 0 — invisible pour toutes
les autres gardes, désormais couvert par `maxUnansweredFraction` · le README affirmait avoir écarté
l'hypothèse des binds figés, ce que la garde ne permet pas · `SubmittedMin/Max` est imprimé par palier,
pour que le prochain run confronte le seuil au lieu de le supposer · deux propriétés annoncées en godoc
sans test en ont reçu un.

**Gelé, nommément :**
- La **chute du débit par bind au-delà de 80 binds** n'est pas attribuée. `maxSubmitSpread` ne détecte
  qu'un gel détruisant > 85 % du travail d'une session ; un gel à mi-fenêtre passe. Trancher demande un
  débit **par session sur la durée**, que l'instrument ne relève pas. → suivi step-201b.
- Le **verdict de saturation à 320 binds tient à 1,6 %**, moins que le bruit inter-paliers du même run
  (~4 %). Le plafond est un ordre de grandeur, pas une valeur. → second balayage en step-201b.
- Constats de revue non traités, sans effet sur un chiffre : chiffres du README hérités d'un tableau
  antérieur dans deux phrases secondaires · un test devenu tautologique depuis que `Unanswered` n'est
  plus seuillé de la même façon · `binds > 1` mort dans la garde de dispersion · le diagnostic
  « sessions stopped being served » s'affiche aussi quand la cause est une erreur d'écriture.

### Dettes mises au jour par PR2, non traitées ici

- **`ingest_duration_seconds` a exactement le même défaut de buckets** que `message_e2e_duration_seconds`
  avant PR2 : ses seuils NFR (p50 < 50 ms, p99 < 250 ms, §1.2) tombent tous deux **entre** deux bornes
  (0,032/0,064 et 0,128/0,256), donc aucun des deux n'est décidable depuis son exposition. Correctif de
  deux lignes, même patron que celui appliqué ici. → à faire avant que quiconque publie un verdict sur
  le budget d'ingestion.
- **`router-svc` enregistre lui aussi un sous-ensemble du catalogue** (`cmd/router-svc/wiring.go:143`).
  Le patron « liste nommée + test d'exposition » n'a été appliqué qu'à `connector-pool-svc` : la même
  classe de trou peut exister là et n'a pas été auditée.
- **Le p99 est par `submit_sm`, pas par message.** Un message de N segments produit N observations ;
  la vraie latence par message est celle du dernier segment, et compter les précédents biaise la
  distribution du bon côté (optimiste). Dédupliquer demanderait un état inter-records par `message_id`,
  non borné et concurrent entre shards.
- **Un message rerouté est attribué au connecteur qui a fini par répondre**, avec le span complet
  incluant le saut échoué. C'est la bonne lecture de « bout en bout », mais un tableau de bord par
  connecteur montrera le second portant une latence causée par le premier.

### Limite connue, consignée sans être résolue
Les consumers d'un même processus partagent le préfixe `KAFKA_` : dans `router-svc`, monter
`KAFKA_FETCH_MIN_BYTES` pour la projection CDR affecte aussi le consumer du pipeline. Si le run de
référence prouve qu'il faut les découpler, on ajoutera un override par consumer — **pas avant la
mesure**.
