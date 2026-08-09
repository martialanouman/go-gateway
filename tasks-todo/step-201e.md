# step-201e — Attribuer le plafond : rendre mesurable ce que le harnais ne voit pas

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201d (livrée) · **Bloque :** step-201b

## But
Donner au harnais de charge de quoi **attribuer** un plafond de débit, et pas seulement le constater.
step-201b doit rendre un verdict NFR ; elle ne peut pas le faire avec un instrument qui, par
construction, ne voit qu'un tiers du système.

## Le constat

step-201d PR2 a levé le goulot du routeur et l'a prouvé à 2 400 msg/s. Poussée à 4 800, la mesure a buté
sur ses propres limites — et le harnais l'a dit lui-même.

**1. L'alarme intégrée a sonné.** `steady.Report.Behind` compte les soumissions parties après leur
instant prévu, et son godoc (`test/load/steady/inject.go:93-95`) dit quoi en faire :

> « A large share means the achieved rate is a property of **this harness** and not of the gateway, and
> the run should be re-read with more workers before any conclusion is drawn. »

| Run | injecteur en retard |
|---|---:|
| 2 400 msg/s | 1,7 % |
| 4 800, 8 lanes, 4 binds | 6,4 % |
| 4 800, 8 lanes, **16 binds** | **17,3 %** |

Le retard **triple quand on ajoute douze binds**. Les 64 workers d'injection perdent de l'ordonnancement
au profit du pool : à 4 800, le débit d'entrée est déjà une propriété du harnais.

**2. Les deux étages se disputent l'hôte.** Mêmes 8 lanes, `BIND_POOL` porté de 4 à 16 : le pool rattrape
son retard (`mt.routed` 147 244 → 213) mais le **routeur** retombe de 4 702 à 3 395/s. Personne n'a
touché au routeur.

**3. Et ce n'est pas le CPU.** 1,6 cœur sur 14 dans les deux cas. Si l'hôte n'est pas saturé en calcul,
le blocage est ailleurs — et `cpuSeconds` (`internal/e2e/reference_test.go`) ne compte **que le processus
Go**, comme son propre godoc le dit. Redpanda, Postgres et ClickHouse tournent dans des conteneurs à
côté. Le suspect le plus probable est précisément celui que l'instrument ne mesure pas.

**Conclusion consignée dans `test/load/README.md` (mesure du 08/08/2026, PR2) : les chiffres à 4 800
bornent des tendances, pas des capacités.** Cette fiche existe pour que ce ne soit plus vrai — et parce
que l'audit du 01/08 a déjà enseigné qu'**un commentaire n'est pas un backlog** (steps 190-192).

## Design arrêté (2026-08-08)

> Les décisions ci-dessous portent des **titres** et non des identifiants `D*` : cette fiche utilise
> déjà `D1`…`D6` pour ses **livrables**, et deux systèmes de numérotation dans un même fichier se
> confondent à la première relecture.

### Deux PRs — la mesure d'abord, le chronomètre ensuite
- **PR1** — `D1` + `D2`, plus l'extraction de scraper qu'ils réclament tous les deux.
- **PR2** — `D3` + `D5` + la clause `Behind` (voir plus bas), puis le `git mv` de la fiche.

Précédent maison : step-201d, livrée en deux PRs sous fiche unique.

**Le coût de ce découpage est assumé et doit être consigné dans la réserve de PR1.** La table de
décision de `D1` a une branche « la courbe plafonne → le coût est par record, sérialisé ailleurs ».
En PR1 le décorateur de producer **compte** sans chronométrer : cette branche ne sera donc tranchée
que par une *soustraction* — la famille de défaut que cette fiche reproche elle-même aux « ~819 µs
bloqués » de step-201d `D9` — jusqu'à ce que `D3` arrive. Conséquence pratique : **PR2 relance le
balayage de `D1`** pour lire l'histogramme. C'est un aller-retour choisi, pas un oubli.

### Le balayage de `D1` se fait sur des topics privés, pas sur `KAFKATEST_PARTITIONS`
Écart assumé avec la lettre de la fiche, pour trois raisons :

1. `topicPartitions()` est lu **une seule fois avec le conteneur** (`kafkatest.go:63-79`, singleton
   `sync.Once`) → un point de courbe = une invocation `go test` sur un conteneur différent. On perd le
   patron maison « une ligne `t.Logf` par palier dans un seul run », et la comparabilité avec lui.
2. `KAFKATEST_PARTITIONS` élargit **tous** les topics, `mt.routed` compris — le README le dit déjà de
   lui-même (`:569-571`, « `PARTITIONS=1` handicape plus que le routeur »). Un balayage qui fait varier
   l'entrée *et* la sortie ne mesure pas les lanes.
3. Le routeur ne code aucun topic en dur : c'est l'appelant qui construit le consumer, et
   `rec.PartitionKey()` est `{Topic, Partition}`. Consommer `loadref.inbound.p8` lui est transparent.

Paliers **1, 2, 4, 8, 16** — les quatre premiers sont ceux du tableau de step-201d PR2
(`README.md:540-546`), pour que les deux courbes soient superposables ligne à ligne. `mt.routed` reste
à 4 partitions sur tout le balayage.

**Contre-épreuve obligatoire, une fois :** rejouer le point à 8 lanes via
`make load-reference RUN=TestRouterConsumeCeiling PARTITIONS=8`, sur le vrai `mt.inbound`. Si les deux
chiffres se recoupent, l'écart est validé par une **mesure** et non par un argument ; sinon la lettre
de la fiche reprend la main.

### Le pré-remplissage est asynchrone, et son placement est vérifié, pas prédit
`Producer.Produce` est `ProduceSync` acks=all (`producer.go:43-52`) : à ~2 000/s mesurés, 150 000
records coûteraient 75 s **par palier**, 25× la fenêtre. Le pré-remplissage passe donc par un
`*kgo.Client` en `Produce` + un seul `Flush`, mêmes options que `NewProducer` moins la synchronie.
C'est légitime parce que le pré-remplissage est une **génération de fixture** : la fidélité qui compte
est celle du *record* (`pipeline.EncodeInbound`, clé `AccountID`, corps GSM-7 de la longueur que
l'injecteur envoie) et de son *placement*, pas celle du client qui l'écrit. Le débit de
pré-remplissage est journalisé — c'est ce que le broker absorbe **avec batching**, une borne haute
gratuite avant même `D2`.

`mt.inbound` est clé par `AccountID` (`wire.go:88`) et `kafka.NewProducer` **ne configure aucun
`RecordPartitioner`** (`producer.go:26-32`) : c'est le défaut franz-go qui décide, et ce défaut peut
changer à la prochaine montée de version. Donc **on ne prédit pas le hachage, on l'observe** :

- *optimisation* — les comptes sont choisis par échantillonnage par rejet contre le partitionneur, pour
  viser un compte par partition ; si le défaut franz-go diverge un jour, cette prédiction devient
  fausse sans rien casser ;
- *garde réelle* — après le `Flush`, les **end-offsets par partition** sont lus et un backlog
  déséquilibré fait échouer le palier. Cette garde ne suppose rien du hachage.

Le piège fermé est déjà consigné (`reference_test.go:559-566`) : des comptes tirés au hasard donnent
une distribution en boules-dans-des-boîtes ; à P=16 il est banal qu'une partition reçoive le double
d'une autre, et la partition légère draine avant la fin de la fenêtre.

**Ce que ça achète et ce que ça coûte, à consigner au README :** le backlog est équilibré *par
construction*, le trafic réel ne l'est pas. La courbe mesure ce que les lanes achètent **quand elles
sont toutes alimentées** — la borne haute du fan-out, jamais la moyenne d'un trafic réel.

### Trois gardes, parce qu'une mesure de plafond a deux façons plausibles de mentir
1. **Le backlog doit tenir.** La garde refuse **par partition, jamais sur le total** : une lane à sec
   sous-estime le palier pendant que le total reste sain. Message actionnable (`raise REF_PREFILL`).
2. **Les lanes doivent exister.** `handleBatch` ouvre une goroutine par partition **présente dans le
   lot**, et le lot est borné par `FetchMaxPartitionBytes` (56 KiB, ADR-0012). Un lot qui couvre 3
   partitions sur 16 fait tracer la courbe contre 3. Le nombre de lanes réellement ouvertes est mesuré
   et imprimé, pas supposé.
3. **Le chrono part à la première consommation, pas au lancement.** Un groupe neuf rejoint en quelques
   centaines de ms à quelques secondes ; sur une fenêtre de 10 s divisée par la durée **réelle**, c'est
   10 à 30 % de sous-estimation, **pire aux petits paliers** — donc une déformation de la pente,
   c'est-à-dire de ce que la courbe dit. Ni `TestCDRWriteCeiling` ni `TestRoutedProduceCeiling` n'ont
   ce problème : aucun n'a de groupe de consommation.

Débit recoupé par **deux sources indépendantes** : le compteur du décorateur et `lag_début − lag_fin`
(le topic privé est statique pendant la fenêtre). Divergence de plus de quelques pour cent = run non
notable.

### `D2` lit `/public_metrics`, et le nom de la famille n'est pas celui qu'on croyait
Redpanda sert **deux** expositions sur le port 9644 : `/metrics` (interne, préfixe `vectorized_*`, des
milliers de séries) et `/public_metrics` (curated, préfixe `redpanda_`, labels agrégés). Les deux
scrapers existants font, pour une origine nue, `u.Path = "/metrics"` : recopié tel quel, le troisième
renverrait **un 200 qui ne dit rien** — le pire mode de panne pour cet instrument.

Familles retenues, **arrêtées sur une capture réelle** (`redpandametrics/testdata/public_metrics.txt`,
v24.2.18 du 08/08/2026) et non sur la documentation — laquelle m'avait fait écrire ici, à tort, que la
famille par *handler* n'existait pas côté public :

- `redpanda_kafka_handler_latency_seconds{handler}` — `produce` et `fetch`. La latence **de service**,
  celle que le client a attendue. C'est le cœur de `D2`.
- `redpanda_kafka_request_latency_seconds{redpanda_request}` — `produce` et `consume`. La latence
  **interne** du broker : une autre question, donc lue séparément et jamais fusionnée avec la première.
- `redpanda_cpu_busy_seconds_total{shard}` — secondes cumulées, **typé `gauge`** par le broker malgré
  le suffixe `_total`, d'où une lecture agnostique du type.
- `redpanda_kafka_request_bytes_total{redpanda_request,redpanda_topic}` — contrôle de vue : un delta nul
  signifie *le scrape n'a pas couvert le run*, pas *broker au repos*.

**`offset_commit` n'a effectivement aucune série curée** — la fiche le demandait, le broker ne l'offre
que dans l'exposition interne (`vectorized_kafka_handler_latency_microseconds{handler}`, vérifié sur la
même capture). Décision : **on ne le lit pas**. Aller chercher les 10 266 lignes de `/metrics` pour une
seule figure alors que `produce` et `fetch` répondent à la question du plafond n'est pas un arbitrage
serré. Consigné plutôt que deviné ; le chemin est ouvert (`DefaultPath` est un paramètre) pour qui en
aura besoin.

Agrégation sur `shard` obligatoire (une série par shard) ; p99 rendue comme **intervalle**
`(borne_basse, borne_haute]`, jamais interpolée — les buckets seastar sont log-échelonnés et une
interpolation porterait 100 % d'erreur en se lisant comme une mesure. Leçon de `gatewaymetrics`.

`kafkatest` gagne `AdminAPI(t)` sur le patron exact de `Brokers(t)`, avec son **erreur stockée
séparément** : un port 9644 irrésolu doit faire échouer `AdminAPI` et rien d'autre. Plus de quarante
tests d'intégration dépendent de `Brokers` ; un instrument neuf ne les prend pas en otage.

### Le scraper est extrait, parce que le code le demande explicitement
`gatewaymetrics.go:473-476` dit textuellement : « *Extracting a common scraper is worth doing the day a
third one appears.* » `D2` **est** ce troisième. Écrire le troisième exemplaire en laissant ce
commentaire intact, ce serait commettre — dans la PR qui l'invoque — le défaut que cette fiche cite en
ouverture (steps 190-192, « un commentaire n'est pas un backlog »).

Extraction de la **seule** couche client : parse d'URL, redaction des credentials, chemin par défaut
**paramétrable** (ce dont `D2` a besoin de toute façon), horodatage avant la requête, garde 200.
`Parse` reste dupliqué par pair — c'est le contrat du pair, il n'a rien de commun. **La preuve que
l'extraction ne change rien : les tests des deux paquets existants restent verts sans une seule
modification.**

### `D4` est renvoyé à step-201b, et `D2` le rend inutile ici
`/sys/fs/cgroup` n'existe pas sur l'hôte de mesure (Darwin ; Docker tourne dans une VM), et le banc
`D1` n'embarque **ni Postgres, ni ClickHouse, ni faux SMSC** : un seul conteneur tourne, et
`redpanda_cpu_busy_seconds_total` dit combien de cœurs il a brûlés, par shard, sur la fenêtre — plus
précis qu'un cgroup, et déjà dans le périmètre de `D2`. `D4` garde toute sa valeur pour le run
plein-stack (neuf composants, trois conteneurs) sur un hôte Linux : c'est **step-201b**.

### `D6` est remplacé par une clause opposable
Le pré-remplissage **retire l'injecteur de l'équation** : le débit d'entrée n'est plus une intention
ordonnancée en boucle ouverte, c'est un backlog durable écrit avant que le chrono ne parte. C'est une
réponse plus forte à l'objection de `Behind` que de sortir l'injecteur du processus. Ce qui reste
ouvert — le run plein-stack à 4 800 — relève des hôtes séparés, que cette fiche renvoie elle-même à
step-201b.

À la place, en PR2 : `steady.Criteria.MaxBehindFraction` et le `Check` correspondant. `Report.Behind`
n'est aujourd'hui lu **qu'une fois dans tout le dépôt**, pour impression (`reference_test.go:234`) ; le
godoc de `inject.go:93-95` énonce une règle que **rien n'applique**. Zéro désactive la clause (aucun
verdict existant ne bouge), puis `refCriteria()` la fixe à 0,05. Ancrage falsifiable sur des données
déjà consignées : 1,7 % passe, 6,4 % et 17,3 % tombent. C'est ce qui empêche la réserve levée par cette
fiche de se reformer à la fiche suivante.

## Périmètre

Des **instruments**, aucun changement du chemin chaud de production.

### D1 — Le banc « routeur seul », le geste décisif
Un test de plafond dans l'idiome maison — `TestCDRWriteCeiling` et `TestRoutedProduceCeiling` en sont les
deux précédents : pré-remplir `mt.inbound`, puis lancer **uniquement** le routeur (pas d'injecteur, pas
de pool, pas de REST, pas de faux SMSC) et balayer `KAFKATEST_PARTITIONS`.

Question falsifiable : **le routeur replafonne-t-il vers 4 700/s une fois seul ?**
- oui → le plafond appartient au routeur ou au broker, et la contention n'était pas le sujet ;
- il monte franchement → le plafond de PR2 **était** la contention, et le chiffre de 4 702/s doit être
  annoté au README comme tel.

Diagnostic, pas porte : aucune assertion de seuil sauf « le chemin bouge ». C'est le seul des cinq à
avoir une valeur **avant** step-201b, parce qu'il dit combien de partitions et de pods provisionner —
donc il alimente **step-207**.

### D2 — Scraper le broker, l'angle mort le plus large
Le module testcontainers expose déjà `AdminAPIAddress()` (port 9644) et Redpanda y sert ses métriques
Prometheus. Le patron de lecture existe deux fois (`test/load/gatewaymetrics`, `test/load/smscmetrics`).
Latence de service par API — `produce`, `fetch`, `offset_commit` — et le broker est nommé ou blanchi.

Coût quasi nul pour la question la plus ouverte : c'est le premier à faire après `D1`.

### D3 — Chronométrer le produce *in situ*
Les « ~819 µs bloqués » de step-201d `D9` sont une **soustraction**, jamais une observation. Un
histogramme autour de `Producer.Produce` dans le harnais les transforme en mesure, et distingue « le
broker répond lentement sous charge » de « le temps est ailleurs ».

À poser **dans le harnais**, pas dans `internal/router` : step-201d `D7` a écarté un histogramme de
production au motif que le pipeline pèse 2,3 % du budget, et rien n'a changé depuis.

### D4 — Le CPU des conteneurs
`cpuSeconds` ne voit que le processus Go et le dit. Lire les cgroups de Redpanda / ClickHouse / Postgres
sur la fenêtre complète le tableau, et transforme « 1,6 cœur sur 14 » en une phrase qui a un sens.

### D5 — La latence d'ordonnancement Go
`runtime/metrics` : `/sched/latencies:seconds` et `/sync/mutex/wait/total:seconds`, relevés sur la
fenêtre. Si elle explose quand `BIND_POOL` passe de 4 à 16, la contention est **dans le runtime** et non
chez le broker — ce que ni `D2` ni `D4` ne diraient.

### D6 — Sortir l'injecteur du processus
Ce que le godoc de `steady` réclame dès que `Behind` est haut. Deux formes possibles, à trancher à
l'implémentation : plus de workers (correctif de surface) ou un injecteur dans son propre processus
(correctif de fond). C'est la condition pour que « 4 800 msg/s » soit un débit d'entrée réel et non une
intention.

## Points d'implémentation clés
- **Aucun changement du chemin chaud.** Tout vit sous `internal/e2e`, `test/load` ou `internal/testutil`.
  Un instrument qui change ce qu'il mesure ne mesure plus rien.
- **`D1` d'abord, `D2` ensuite.** Les deux répondent seuls à l'essentiel de la question ; `D3`-`D5` ne
  servent qu'à départager ce qui resterait.
- **Le harnais reste mono-processus** hors `D6`. Séparer les services en processus distincts est un
  travail de câblage (testcontainers ouvre des ports éphémères) qui appartient à step-201b, pas ici.
- Ne rien conclure d'un run dont `Behind` dépasse quelques pour cent : la règle est déjà écrite dans
  `steady`, elle a juste besoin d'être respectée.

## Tests
- `D1` : un test de plafond `loadref`, sur le patron exact de `TestCDRWriteCeiling` — unité de travail
  identique à la production, fenêtre par `context.WithTimeout`, débit divisé par la durée réelle, aucune
  assertion de seuil sauf « le débit n'est pas nul ».
- `D2`-`D5` : les lecteurs sont des fonctions pures (rendu, agrégation) et se testent hors conteneur,
  comme `pipelineShare` et `cpuShare` l'ont été en step-201d PR1.
- Balayage consigné en lignes **ajoutées** à `test/load/README.md`, aucune éditée.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] le plafond de 4 800 msg/s est **attribué** — routeur, broker, runtime ou hôte — ou l'échec à
      l'attribuer est consigné avec ce qui manque
- [ ] la réserve « les chiffres à 4 800 bornent des tendances, pas des capacités » est levée dans
      `test/load/README.md`, ou remplacée par une réserve plus étroite
- [ ] aucun changement du chemin chaud de production

## Hors périmètre
Le verdict NFR pleine échelle et l'environnement représentatif → **step-201b**. Les hôtes séparés
(broker sur son nœud, pods dédiés, injecteur et simulateur ailleurs) → step-201b également : ils
produisent une **capacité**, là où cette fiche produit une **attribution**. Le shard par clé de compte au
routeur → suite possible de step-201d `D11`, avec son propre ADR, si une courbe le réclame.
