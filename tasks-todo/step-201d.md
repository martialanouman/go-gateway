# step-201d — Le routeur est le goulot suivant du débit traversant

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201c · **Bloque :** step-201b

## But
Lever le goulot que le run de référence du 03/08/2026 a mesuré **après** step-201c : `router-svc`
consomme `mt.inbound` moins vite que l'ingestion ne l'alimente, et c'est désormais la seule étape qui
accumule du retard.

## Le constat, mesuré
Run `make load-reference` aux défauts livrés, 1 200 msg/s visés, 60 s mesurés
(`test/load/README.md`, « Mesure du 03/08/2026 ») :

```
mt.inbound  3 486 -> 22 403     → +291 rec/s : le routeur ne suit pas
mt.routed       7 ->     12     → plat : le pool de connecteurs suit
mt.outcome     20 ->     33     → plat : la projection de CDR suit
```

- Sortie **892 `submit_sm/s`** pour 1 200 acceptés — 25,7 % d'écart, tolérance 2 %.
- p99 bout-en-bout entre **10,2 et 20,5 s**, moyenne 11,2 s, quand l'ingestion répond en **11 ms** au
  p99. L'écart est donc de l'**attente en file**, pas du temps de traitement.
- Le pair est hors de cause : le faux SMSC a été calibré à **236 274 `submit_sm/s`** dans le run même.

step-201c a levé le goulot précédent (le pool sortait 192–330/s parce qu'il écrivait le CDR par
message) : `mt.routed` est passé de +631 rec/s à plat. Le facteur limitant a simplement changé d'étape.

## Design arrêté (2026-08-08)

### D1 — Deux portes, dans cet ordre : la mesure d'abord, le correctif ensuite
La contrainte cardinale de cette fiche — « le goulot est **nommé et isolé par une mesure**, pas déduit »
— ne peut pas être tenue par une PR qui contient les deux. Une fois mergée, l'artefact « mesuré puis
corrigé » est **indiscernable** de « corrigé, puis banc écrit pour être d'accord ». La séquentialité doit
vivre dans l'historique git, pas seulement dans l'intention.

- **PR1 (`step-201d` PR1)** — la porte de mesure. Instruments, micro-bancs, fidélité du harnais, runs
  consignés, goulot **nommé** en `D9` ci-dessous. Aucun changement de comportement de production hormis
  l'instrument de `D7`.
- **PR2 (`step-201d` PR2)** — la porte de correctif. Uniquement ce que PR1 a désigné, plus le run qui le
  prouve. Si le verdict désigne **deux** changements indépendants, ce sont deux PRs : un correctif dont le
  gain n'est pas isolé n'est pas plus mesuré qu'un goulot déduit.

Précédent maison : step-201 a été livrée en 3 PRs sous une fiche unique.

### D2 — Le routeur est sérialisé en trois points, tous structurels
Aucun n'est un réglage ; aucun levier de configuration ne les touche.

1. `internal/router/router.go:94` → `Consumer.Run` (`internal/storage/kafka/consumer.go:79-129`) →
   `fetches.EachRecord` : **une seule goroutine, un record à la fois**, aucun fan-out. L'interface
   `router.Consumer` (router.go:28-30) n'expose même pas `RunBatch`, alors que `RunBatch` existe et est
   déjà utilisé par `internal/ingest/accepted.go` et `internal/outcome/outcome.go`.
2. `router.go:163-176` : `ProduceSync` acks=all **dans la boucle de consommation**, un record à la fois.
   N segments = N allers-retours broker sérialisés avec le traitement du message suivant.
3. `mt.inbound` est clé par `AccountID` (`internal/pipeline/wire.go:88`) — « keyed by account so an
   account's submissions keep their partition order (§1.6) ».

### D3 — Le harnais mesure une configuration Kafka qu'aucun pod n'exécute
`internal/e2e/reference_test.go:278` construit `config.Kafka{Brokers, Timeout}` en littéral de struct :
aucun `envDefault` de `internal/config/config.go:161-202` ne s'applique, et `consumerOpts`
(`internal/storage/kafka/options.go:47-63`) ne pose une option que si le champ est `> 0`. Le run tourne
donc au défaut franz-go — **`FetchMaxPartitionBytes` = 1 MiB au lieu des 56 KiB que l'ADR-0012 a
délibérément choisis**. Le lot de poll du run est ~18× celui d'un pod.

C'est la **même forme de défaut** que celui déjà consigné pour `chCfg.MaxOpenConns` (« chtest leaves both
at zero, which the driver silently reads as unset »). Toute conclusion sur le batch ou le fan-out tirée
avant correction porterait sur une configuration fictive. → Corrigé en PR1, **avec un test d'épinglage**
sur les valeurs de `config.Kafka`, pour que la dérive ne se rejoue pas une troisième fois.

### D4 — Le harnais mesure un pipeline amputé de trois étages
`reference_test.go:354` : `pipeline.New(tracer, refResolver{resolver}, authorizer, enforcer, spam, nil,
nil)` — les deux `nil` finaux sont `rateLimiter` et `credit`, dont le godoc dit qu'un nil laisse l'étape
**en pass-through**. Et `reference_test.go:346` : `antispam.New(ctx, repo, nil, nil, nil)` = `StateStore`
nil. **Zéro aller-retour Redis dans le routeur du run.**

Or trois des cinq suspects que le *Périmètre* de cette fiche énumère — débit, réservation de crédit,
anti-spam — sont exactement ceux-là. **Ils n'ont jamais été mesurés.** Les 892/s du 03/08/2026 ne
contiennent que : décodage JSON, `phonenumbers`, sondes Bloom, encodage/segmentation, encodage JSON,
`ProduceSync`.

La cause est la **signature positionnelle à 7 paramètres** : `(…, nil, nil)` ne se relit pas, là où
`Deps{RateLimiter: nil, Credit: nil}` est une omission qu'un relecteur voit. → `pipeline.New` devient
`pipeline.Deps` en PR1 (`D6`), et les trois étages sont câblés pour de vrai.

### D5 — Le harnais ne peut structurellement pas montrer un gain de parallélisation
`seedRefControlPlane` (`reference_test.go:674`) sème **un seul compte**. La clé de `mt.inbound` étant
l'`AccountID`, tout le trafic tombe sur **une clé, donc une partition**. Ni des partitions
supplémentaires, ni des répliques, ni un fan-out interne par shard n'y changeraient quoi que ce soit :
un résultat plat serait garanti d'avance et ne prouverait rien.

**`REF_ACCOUNTS`, défaut 1.** Le défaut reproduit *verbatim* le run du 03/08/2026 — c'est ce qui garde
tout le tableau de `test/load/README.md` comparable, et c'est non négociable. **K = 32** pour les runs de
parallélisme : les topics de test ont 4 partitions
(`internal/testutil/kafkatest/kafkatest.go:48`), 32 comptes donnent > 99,9 % de chance que les quatre
portent du trafic, et 32 dépasse d'un facteur 2-4 tout nombre de shards in-process plausible — donc un
résultat plat ne pourra plus être imputé ni à une partition vide ni à un shard oisif.

**K ne change que la répartition des clés**, et c'est vérifiable ligne à ligne : un seul client, un seul
sender ID, une seule route ⇒ sender-ID reste deux hits de map, opt-out reste un miss Bloom sans
confirmation Postgres, débit et crédit restent non semés. K clients ou K sender IDs changeraient le
**travail** et rendraient la comparaison K=1/K=32 malhonnête.

Un agrégateur national servant **un** compte est le cas irréaliste : le défaut à 1 était le cas
dégénéré, choisi pour la simplicité du harnais, jamais pour la fidélité.

### D6 — La mesure la moins chère est une soustraction, et elle coûte une ligne
`reference_test.go:351-357` laisse `Metrics` **nil** dans `router.Deps` : `pipeline_duration_seconds`
n'est alimenté par **aucun** run à ce jour. Le câbler débloque une arithmétique décisive.

Le lag `mt.inbound` monte pendant toute la fenêtre, donc la boucle de consommation n'est **jamais** à
sec, donc `1/892 s = 1,12 ms` est exactement le **temps mural par message** de la goroutine unique :

```
1,12 ms = decode + Pipeline.Process + N×(encode + ProduceSync) + commit amorti
```

`pipeline_duration_seconds` donne le terme central ; **le reste est par soustraction**. Une moyenne
proche de 1,1 ms met tout le coût *dans* le pipeline ; une moyenne très inférieure le met *hors* du
pipeline et désigne `ProduceSync`. Une ligne de code divise l'espace des hypothèses en deux.

S'y ajoutent deux relevés du même ordre de coût : la **comptabilité CPU du processus**
(`Getrusage(RUSAGE_SELF)`, loggué en « X cœurs sur `runtime.NumCPU()` ») et le **lag par partition** au
lieu de la somme. Le premier répond à l'exigence explicite de cette fiche : le run tient sept composants
dans un processus plus trois conteneurs sur le même hôte, et **si le processus sature déjà les cœurs,
« le routeur est le goulot » est une propriété du portable** — auquel cas le livrable honnête de la step
est cette phrase-là, plus un harnais multi-processus. Le second empêche qu'un déséquilibre de hachage
passe pour un effet du correctif.

### D7 — Un histogramme par étape, en production, précédé de son propre banc de coût
`pipeline_duration_seconds` (`internal/observability/metrics/catalog.go:190-199`) est un `Histogram`
**sans aucun label**, observé en un seul point autour de `Pipeline.Process`. Aucune surface ne sait
donc situer le coût par étape — le *Périmètre* de cette fiche demande de « lire l'histogramme plutôt que
supposer », et l'instrument n'existe pas à cette granularité.

Les **spans** ne le remplacent pas : ils répondent « où **ce** message a passé son temps » (step-185,
`get-message-trace`), jamais « où la **flotte** passe son temps » — et le run de référence utilise
délibérément un tracer no-op parce qu'un recorder mesurerait sa propre pression mémoire.

Greffe dans `Pipeline.stage` (`internal/pipeline/pipeline.go:318-326`), qui reçoit déjà le nom d'étape :
un seul site d'appel, vocabulaire clos de 9 fixé par §6.1. Quatre contraintes :

- **Zéro string sur le chemin chaud** : les 9 `prometheus.Observer` résolus **une fois** au constructeur,
  dans un tableau indexé par une constante typée. Les noms d'étape deviennent inatteignables depuis le
  chemin chaud ⇒ la garde de cardinalité de step-180 est satisfaite **par construction**, pas par un
  contrôle à l'exécution.
- **9 lectures d'horloge, pas 18** : les étapes sont strictement séquentielles, la fin de l'une est le
  début de la suivante.
- `stage` ajouté à `allowed` (`internal/observability/metrics/labels.go`), justification : vocabulaire
  clos fixé par le code, jamais par le trafic.
- **Buckets propres** : 12 buckets exponentiels à partir de 1 µs. La spine actuelle démarre à 100 µs et
  empilerait huit étapes sur neuf dans le premier bucket.

**`BenchmarkPipelineStageObserve` est écrit d'abord**, sur le patron de
`internal/metricstream/bench_test.go` (`ReportAllocs`, cible zéro allocation) : la décision est
elle-même une mesure. Repli documenté — échantillonnage — si le coût dépasse ~2 % du budget par message.

*Assumé, à trancher en revue :* l'histogramme est Prometheus seulement, là où step-180 demande deux
surfaces alimentées depuis un site d'appel. Lecture retenue : cette règle vise les figures **présentes
des deux côtés**, pas un diagnostic que seul Grafana porte.

### D8 — Deux micro-bancs, et c'est la forme de la courbe qui tranche, jamais une valeur
Le patron est `TestCDRWriteCeiling` (`internal/e2e/cdrceiling_test.go`), qui a tranché le goulot de
step-201c : il se déclare diagnostic et non porte, n'assère que `rate == 0`, reproduit **exactement**
l'unité de travail de production, et laisse la courbe répondre.

- **M1 — `BenchmarkPipelineProcess`** (`internal/pipeline/bench_test.go`), Go pur, sans conteneur.
  Axe 1 : neuf sous-bancs par étape avec `-benchmem`, qui rendent une **répartition**. Axe 2 : balayage
  `b.RunParallel` × `GOMAXPROCS ∈ {1,2,4,8,16}` — c'est lui qui distingue les natures :

  | Forme de la courbe | Verdict | Correctif |
  |---|---|---|
  | ns/op constant, débit linéaire | coût CPU **parallélisable** | fan-out de la consommation (`D9`, A) |
  | ns/op qui croît, débit qui plafonne | **verrou partagé dans le pipeline** | le fan-out serait un no-op |
  | ns/op > 1 ms dès la concurrence 1 | coût CPU **irréductible** | seul le nombre de cœurs déplace le chiffre |

- **M2 — `TestRoutedProduceCeiling`**, jumeau exact du précédent (tag `loadref`, un `mt.routed` encodé
  par `pipeline.EncodeRouted`, clé = MessageID). Deux dimensions : concurrence `{1,4,16,64}` — *le RTT
  acks=all monte-t-il avec la concurrence ou plafonne-t-il ?*, un chiffre de l'ordre de 900-1 200/s à
  concurrence 1 **nommerait le goulot**, puisqu'il coïncide avec les 892 observés — et records par appel
  `{1,8,64,256}` à concurrence 1, qui **mesure le correctif B avant qu'il soit écrit**.

S'y ajoute le balayage des trois suspects rendus mesurables par `D4` : semer une `rate_limits` qui ne
throttle jamais, une règle de vélocité, la facturation. La mesure est le **delta de débit quand le levier
bascule** — le pipeline reste câblé en vrai dans tous les cas, seul le semis change.

### D9 — Le goulot, une fois nommé
*(à remplir par PR1, avec les chiffres — c'est le critère de sortie de la porte de mesure)*

Candidats classés avant mesure, avec ce qui les valide ou les infirme :

**A — fan-out de la consommation (`RunBatch` + shard par `rec.Key`).** Sharder par la clé **préserve
§1.6 exactement** : `shardIndex(rec.Key, n)` est une fonction pure de la clé, et la clé *est*
l'`AccountID`, donc tous les records d'un compte tombent dans un shard traité séquentiellement en ordre
d'offset. Deux comptes qui collisionnent sont *plus* sérialisés que nécessaire, jamais réordonnés ;
l'ordre inter-comptes n'a jamais été garanti par rien. Le contrat de `BatchHandler`
(`consumer.go:131-140`) est satisfait par `errShardHalted` — halter tout le shard est un sur-ensemble
strict de halter le groupe d'ordonnancement.
**Le risque n'est pas l'ordre, c'est le rayon de duplication** : il passe de **1** (aujourd'hui `Run`
commite le préfixe traité *avant* de retourner l'erreur) à ~250 records/partition. C'est dans l'enveloppe
que l'ADR-0012 a acceptée, **mais l'ADR raisonnait sur le pool de connecteurs, pas sur le routeur** ⇒
amendement d'ADR-0012 en PR2, pas une ligne glissée en silence.

**B — produce groupé avec barrière avant commit.** L'invariant « l'offset ne commite qu'après ACK de
**tous** les segments » survit par construction : `RunBatch` commite après le retour du handler, donc une
barrière placée *dans* le handler le rend trivial, à condition que l'erreur soit mappée **par record**
(sans quoi `committablePrefix` devient faux). L'ordre survit aussi — `mt.routed` est clé par `MessageID`
et franz-go idempotent maintient l'ordre par partition même avec plusieurs requêtes en vol.
**Ampleur à tempérer** : le run a un segment par message, donc grouper les segments *d'un* message ne
rapporte rien au chiffre mesuré ; le vrai levier est de grouper **à travers les messages** d'un lot, ce
qui n'existe qu'une fois A livré.

**C — ne pas payer `e164.Normalize` deux fois par processus.** `pipeline.go:157` et
`internal/ingest/accepted.go:82` tournent dans le même processus, alors que l'ingestion l'a
**délibérément déporté** de son chemin de requête (« too heavy to run inline at the ingest rate »,
`internal/ingest/ingest.go:70-72`). Ne compte que si M1 montre e164 dominant. **Piège explicitement
refusé** : mémoïser derrière un LRU — l'injecteur étale les destinations sur un bloc contigu, le taux de
hit serait un artefact du banc et mentirait sur la production.

**D — partitions et répliques.** Le levier d'exploitation réel, **structurellement invisible au harnais
mono-processus**. À nommer au README comme la réponse d'exploitation, sans prétendre l'avoir mesurée.

### D10 — Ce que ce harnais ne saura toujours pas dire
Même après `D3`-`D5`, le chiffre reste un **majorant** du débit réel : le tracer est no-op, `Sealer` est
nil (pas de scellement de contenu), il n'y a pas d'agrégat de disjoncteur Redis, et tout tourne dans un
processus sur un hôte de développement. À écrire au README, et à opposer à step-201b si elle prétend
rendre un verdict NFR sur ce harnais.

`ingest_duration_seconds` (déclarée `catalog.go:136-145`, **observée nulle part** en production) reste
hors périmètre : cette fiche dit « à corriger **si la mesure passe par elle** », et la séquence ci-dessus
n'y passe pas — le p99 d'ingestion vient des échantillons propres de l'injecteur. Elle appartient à
step-201b, qui porte le verdict NFR.

## Périmètre — deux PRs (`D1`)

**PR1, la porte de mesure.** Rendre le coût lisible et le harnais fidèle, puis nommer le goulot.
- `pipeline.New` → `pipeline.Deps` (`D4`), qui rend l'amputation du harnais visible en relecture.
- Fidélité du harnais : réglage Kafka de production + test d'épinglage (`D3`), pipeline complet câblé et
  ses trois leviers de semis (`D4`), `REF_ACCOUNTS` (`D5`).
- Les trois relevés qui ne coûtent presque rien : `Metrics` câblé, comptabilité CPU, lag par partition
  (`D6`).
- L'histogramme par étape, précédé de son banc de coût (`D7`).
- Les deux micro-bancs `BenchmarkPipelineProcess` et `TestRoutedProduceCeiling` (`D8`).
- Runs consignés en lignes **ajoutées** à `test/load/README.md`, goulot nommé en `D9`.

**PR2, la porte de correctif.** Uniquement le candidat que PR1 a désigné (`D9`), l'amendement d'ADR-0012
s'il s'agit de A, et le run de référence relancé qui le prouve.

## Points d'implémentation clés
- **Ne pas supposer le coupable.** Les candidats plausibles sont nombreux et de natures différentes :
  allers-retours Redis du débit et de l'anti-spam, réservation de crédit, résolution de route,
  encodage/segmentation, ou simplement le parallélisme de consommation de `mt.inbound`. La leçon de
  step-201 est qu'un goulot insensible à la concurrence n'est pas ce qu'on croit : `TestCDRWriteCeiling`
  avait tranché en isolant l'écriture. Prévoir le micro-banc équivalent avant de toucher au pipeline.
- **L'ordre du pipeline MT n'est pas réordonnable** (CLAUDE.md, §6.1). Un gain obtenu en déplaçant une
  étape de conformité n'est pas un gain, c'est une régression d'invariant.
- **`ingest_duration_seconds` est déclarée et observée nulle part** (relevé en step-201c) et ses bornes
  encadrent mal ses seuils NFR — à corriger si la mesure passe par elle.
- Le run de référence tourne dans **un seul processus** : un goulot de parallélisme peut être un artefact
  du harnais autant qu'une propriété du service. Le vérifier avant d'en faire une conclusion.

## Tests
**PR1** — `BenchmarkPipelineProcess` et ses sous-bancs par étape, `TestRoutedProduceCeiling` sur le
patron de `TestCDRWriteCeiling`, `BenchmarkPipelineStageObserve`, et le test d'épinglage de la config
Kafka du harnais (rouge sur le code actuel avant correction, `D3`).

**PR2** — le run de référence relancé : `mt.inbound` plat, ou le nouveau chiffre consigné avec le goulot
suivant nommé.

Dans les deux : les 4 invariants restent verts — en particulier b) : un message routé par numéro exact
traverse toutes les étapes de conformité. Aucun test d'invariant n'est modifié ; en modifier un serait le
signal d'une régression, pas d'un test à ajuster.

## Definition of Done
**PR1**
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] le goulot est **nommé et isolé par une mesure**, pas déduit (`D9` rempli avec ses chiffres)
- [ ] `make load-reference` aux défauts (`REF_ACCOUNTS=1`) reste comparable au run du 03/08/2026
- [ ] mesures consignées en lignes **ajoutées** à `test/load/README.md`, aucune ligne éditée

**PR2**
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] run de référence relancé et consigné
- [ ] ordre du pipeline inchangé, invariants verts
- [ ] si le correctif est A : ADR-0012 amendée pour couvrir le routeur (`D9`)

## Hors périmètre
Verdict NFR pleine échelle → step-201b. Le goulot du pool de connecteurs → step-201c (livré).
