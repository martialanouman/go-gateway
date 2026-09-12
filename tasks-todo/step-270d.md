# step-270d — Les résidus logiciels de step-270c, avant que la campagne ne mesure

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-250e, step-270c · **Bloque :** step-280

## But

Solder les quatre résidus que step-270c a renvoyés à step-280 (`tasks-done/step-270c.md:390-403`) et
qui sont **du logiciel** : ils n'attendent aucune machine, et trois d'entre eux changent ce que la
campagne NFR mesurera ou ce qu'elle pourra appliquer.

## Pourquoi cette fiche existe

step-280 est bloquée par **du matériel** : sa propre fiche écrit qu'un « 8 000/s tenu » mesuré sur
une machine de développement *ne validerait rien, et qu'un échec ne condamnerait rien*
(`step-280.md:16-17`). Le report ne coûte rien — elle ne bloque que step-410.

Mais les résidus ci-dessous ne vivent aujourd'hui **que dans la prose de cette fiche bloquée**.
C'est le motif que le dépôt a déjà payé : sans fiche, la dette est perdue au go-live. Les sortir
d'ici, c'est aussi les rendre exécutables pendant que l'environnement se provisionne.

### R1 — Le run de référence plein-stack ne traverse pas l'étage L0

`internal/e2e/reference_test.go:410` câble `refResolver{resolver}`, et `refResolver` (`:887-893`)
enveloppe `routing.SnapshotResolver` **en direct**, godoc à l'appui. La production câble
`exact.NewResolver` puis `routing.NewL0Resolver` (`cmd/router-svc/wiring.go:397-400`).

step-270c a prixé l'étage sur le **banc routeur isolé**, à dessein — son D1 démontre que le run de
référence *tient* `REF_RATE` et ne pourrait donc pas rendre un coût par message. Ce n'est pas ce
qu'on demande ici : à pleine échelle, la campagne publie des latences bout-en-bout et une charge
Postgres/Redis absolues. Les publier avec une étape du chemin chaud absente du harnais, c'est
publier un dimensionnement faux — et le dimensionnement est le livrable de step-280, pas le débit.

### R2 — L'injecteur plafonne les destinations distinctes à 4 096, et R1 sans R2 ne mesure rien

`newPayloads` (`test/load/steady/inject.go:323-338`) pré-rend `payloadRing = 4096` corps, et la
destination est **dans** le corps ; `at(seq) = bodies[seq%payloadRing]` (`:340`). Le `Dest` par
défaut sait pourtant étaler sur un million de numéros (`:361`) — il n'est jamais appelé au-delà de
l'index 4 095.

Conséquence directe sur R1 : 4 096 MSISDN distincts à 8 000/s, c'est un cache `exactroute:{msisdn}`
**100 % chaud en une seconde**, et pour 6 h. La campagne lirait `pg_hit/s ≈ 0` et publierait que la
grandeur n°1 de step-280 — le débit Postgres du L0 — est nulle. C'est le piège que ce harnais a
déjà payé trois fois, toujours de la même façon : un littéral laissé petit, et une mesure qui porte
sur autre chose sans que rien ne le dise.

**R2 passe donc avant R1.** Câbler L0 sur un anneau de 4 096 produirait une mesure nulle qui a
l'air d'un résultat.

R1 et R2 sont déjà consignés ensemble, mot pour mot, par step-270c : `test/load/README.md:1157-1159`
les nomme et conclut « les deux restent à step-280 ». Cette fiche est ce que cette phrase attendait.

### R3 — Le TTL du cache L0 n'a aucun levier de configuration

`exact.DefaultCacheTTL = 6 * time.Hour` est une **constante de paquet**
(`internal/routing/exact/resolver.go:19`), passée en dur au câblage (`cmd/router-svc/wiring.go:397`).
Aucune clé `EXACT_*` n'existe dans `internal/config/config.go`.

Or `deploy/README.md:111-114` assigne à step-280 la décision « mémoire et politique d'éviction du
Redis partagé », dont ce cache pèse 1,3 à 10 Go de clés en vol, et la DoD de step-280 exige des
« valeurs de leviers retenues consignées **et reportées dans les manifests** ». Une décision sans
clé de manifest n'est pas applicable : aujourd'hui, le seul recours si le Redis se remplit est un
**redéploiement**. `exact.NewResolver` prend déjà le TTL en paramètre (`resolver.go:127`) — il
manque la clé de config et la ligne de câblage, rien d'autre.

Le TTL n'est pas *mesurable* dans une fenêtre de campagne : 6 h ne s'observent pas en dix minutes.
Ce qui est demandé ici est le **levier**, pas sa valeur.

### R4 — Le rapport « MaxConns / voies par pod » que step-280 annonce à 10/12 n'est pas atteignable

`step-280.md:132-133` écrit que « le levier est le rapport `MaxConns` / voies par pod, **aujourd'hui
10/12**, et c'est ce rapport qu'il faut porter dans les manifests ». **Les manifests livrés par
step-270 ne produisent pas ce cas.**

`deploy/k8s/router-svc.yaml:13` pose `replicas: 4`, `:106-107` `minReplicas: 4` / `maxReplicas: 8`,
pour `KAFKA_TOPIC_PARTITIONS: "12"` (`deploy/k8s/configmap.yaml:40`) ; et `handleBatch`
(`internal/router/router.go:124`) ouvre une goroutine par partition **assignée au pod**, chaque voie
traitant séquentiellement. Chaque pod porte donc **2 à 3 voies contre 10 connexions**, pas 12. Le
rapport 10/12 n'existe qu'à un seul pod.

Le levier reste réel — `POSTGRES_MAX_CONNS: "10"` (`deploy/k8s/configmap.yaml:45`) est dans le
ConfigMap **partagé** que les dix services tirent par `envFrom`, donc il ne se règle pas par service
— mais la crainte telle qu'écrite envoie au mauvais cadran. L'invariant qui la remplace est
calculable et gardable : `MaxConns ≥ ⌈partitions / minReplicas⌉`, soit **10 ≥ 3** aujourd'hui.

## Portée

Quatre unités, dans cet ordre.

1. **R2 — la cardinalité des destinations devient un levier de `steady.InjectConfig`**, défaut
   **4 096** : le run d'hier ne bouge pas d'un octet tant que personne ne tourne le bouton.
   Le compromis à écrire dans le design : un anneau large perd la localité de cache que le godoc de
   `payloadRing` invoque (`inject.go:319-320`). Il n'est pas silencieux — l'injecteur **mesure déjà
   son propre décrochage**, avec une bande de 5 % (`reference_test.go:115`) qui refuse un run dont
   le harnais est devenu le sujet.
2. **R1 — `refResolver` passe par `routing.NewL0Resolver(exact.NewResolver(...), nil, snap)`**, et
   le semis **réutilise tel quel** ce que step-270c a livré hors build tag dans
   `internal/e2e/refl0_test.go` : `l0Dest` (`:41`), `portedSet` (`:89`), `mixHolds` (`:222`),
   `cacheFootprint` (`:475`). Même paquet `e2e_test`, aucun tag — la réutilisation est gratuite, et
   `l0Dest` est déjà indexé par l'entier que `newPayloads` fait varier.
   Part portée par défaut **0**, pour que le journal publié reste comparable.
   **`share=0` n'est pas neutre**, et c'est le D3 de step-270c : dès que L0 est câblé, chaque
   message paie la porte Bloom, l'observation d'`outcome` et un saut d'interface. La relance à
   `share=0` contre la bande de reproductibilité du journal est **la preuve à lire**, pas une
   formalité — c'est elle qui dit si le run de référence reste comparable à lui-même.
   `scripts=nil` : L1 est hors sujet, comme en step-270c.
3. **R3 — la clé de config et son câblage.** Couplage à ne pas découvrir en route : ajouter une
   section de config touche **trois endroits**, gardés par un test AST
   (`internal/config` — voir la garde de déclaration de section).
4. **R4 — corriger `step-280.md:132-133`, et poser la garde.** L'invariant
   `MaxConns ≥ ⌈partitions / minReplicas⌉` va dans `internal/deploy/manifests_test.go`, à côté de
   `TestHPACeilingUsesTheDeployedPartitionCount` (`:610`), qui garde déjà l'autre moitié du même
   couplage (`maxReplicas` strictement sous le nombre de partitions). Une garde plutôt qu'un
   paragraphe : c'est la seule forme qui survit au prochain changement de réplicas.

## Chaîne de preuves

L'ordre est contraignant, et le premier point n'est pas une formalité.

1. **R2 puis R1**, jamais l'inverse : un L0 câblé sur 4 096 destinations rend un mélange 100 %
   `redis_hit` que `mixHolds` accepterait comme une borne chaude légitime. La mesure serait fausse
   *et* verte.
2. Chaque test neuf est **vu tomber sous une mutation**, y compris et surtout le levier de
   cardinalité : le figer à 4 096 doit faire tomber une assertion, sinon le bouton n'est pas gardé.
3. **Relance du run de référence à `share=0`**, comparée à la bande de reproductibilité consignée
   dans le journal. Ce qu'elle établit : si l'écart sort de la bande, la porte Bloom se chiffre et
   se consigne ; s'il y reste, c'est **ça** le résultat, et il s'écrit tel quel. Ce qu'elle
   n'établit pas : que « rien n'a bougé » — cette conclusion-là exigerait une comparaison entre deux
   sections du journal que le journal lui-même déclare invalide (step-270c, DoD).
4. Pour R4, la mutation est le manifest : abaisser `minReplicas` sous ⌈12/10⌉ doit faire tomber la
   garde, et la remonter doit la relever.
5. `make check` vert.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] levier de cardinalité livré, défaut inchangé, et la mutation qui le fige vue tomber
- [ ] le run de référence traverse L0 avec le vrai `NewL0Resolver`, semé par les fonctions pures de
      step-270c — aucune copie
- [ ] relance à `share=0` consignée **contre la bande**, quel que soit son verdict
- [ ] clé de config du TTL livrée, câblée, et reportée dans `deploy/k8s/configmap.yaml`
- [ ] `step-280.md:132-133` corrigée, et l'invariant `MaxConns ≥ ⌈partitions / minReplicas⌉` gardé
      par un test dans `internal/deploy`
- [ ] `test/load/README.md:1157-1159` — « les deux restent à step-280 » ne survit pas à cette PR : le
      README dit ce que le run de référence traverse désormais et ce que le nouveau levier fait. Un
      document faux coûte plus cher qu'un document absent

## Hors périmètre — et qui reste à step-280

Trois décisions qui exigent l'environnement représentatif (`deploy/README.md:105-115`), et qu'aucun
portable ne tranche :

- la **valeur retenue** de `POSTGRES_MAX_CONNS`, à arbitrer contre `max_connections` × services ×
  réplicas ;
- la **part portée représentative** (10 à 30 % en marché MNP mûr) et la localité par MSISDN ;
- la **politique d'éviction** du Redis partagé, et la valeur du TTL que R3 rend applicable.

Et, comme toujours : le **verdict NFR** lui-même.
