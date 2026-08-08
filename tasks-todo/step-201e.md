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
