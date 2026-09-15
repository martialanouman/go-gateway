# step-270d — Les résidus logiciels de step-270c, avant que la campagne ne mesure

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** FAIT
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

`internal/e2e/reference_test.go` câble `refResolver{resolver}` au pipeline, et `refResolver`
enveloppe `routing.SnapshotResolver` **en direct**, godoc à l'appui. La production câble
`exact.NewResolver` puis `routing.NewL0Resolver` (`cmd/router-svc/wiring.go:397-400`).

step-270c a prixé l'étage sur le **banc routeur isolé**, à dessein — son D1 démontre que le run de
référence *tient* `REF_RATE` et ne pourrait donc pas rendre un coût par message. Ce n'est pas ce
qu'on demande ici : à pleine échelle, la campagne publie des latences bout-en-bout et une charge
Postgres/Redis absolues. Les publier avec une étape du chemin chaud absente du harnais, c'est
publier un dimensionnement faux — et le dimensionnement est le livrable de step-280, pas le débit.

### R2 — L'injecteur plafonne les destinations distinctes à 4 096, et R1 sans R2 ne mesure rien

`newPayloads` (`test/load/steady/inject.go`) pré-rend `payloadRing = 4096` corps, et la
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
   `payloadRing` invoque (godoc de `DefaultDestRing`, `inject.go`). Il n'est pas silencieux —
   l'injecteur **mesure déjà son propre décrochage**, avec la bande de 5 % de
   `refCriteria().MaxBehindFraction` qui refuse un run dont
   le harnais est devenu le sujet.
2. **R1 — `refResolver` passe par `routing.NewL0Resolver(exact.NewResolver(...), nil, snap)`**, et
   le semis **réutilise tel quel** ce que step-270c a livré hors build tag dans
   `internal/e2e/refl0_test.go` : `l0Dest`, `portedSet`, `cacheFootprint`, et la moitié de `mixHolds`
   qui survit à une fenêtre chaude — voir la revue plus bas, l'autre moitié ne s'applique pas ici.
   Même paquet `e2e_test`, aucun tag — la réutilisation est gratuite, et
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
   `TestHPACeilingUsesTheDeployedPartitionCount`, qui garde déjà l'autre moitié du même
   couplage (`maxReplicas` strictement sous le nombre de partitions). Une garde plutôt qu'un
   paragraphe : c'est la seule forme qui survit au prochain changement de réplicas.

## Design arrêté

Trois décisions, prises avant la première ligne de code. Les deux premières sortent d'une lecture qui a
trouvé, sous les deux collisions que « Portée » annonce, une troisième que personne n'avait vue.

### D1 — La géométrie des destinations : composer, et refuser l'anneau trop petit

Câbler `Dest = l0Dest` tel quel ne marche pas, pour trois raisons qui se cumulent :

1. `l0Dest(i, 0, pool)` rend le **littéral unique** `nonPortedDest` pour tout `i`
   (`l0Dest`, `internal/e2e/refl0_test.go`). À la part portée par défaut — 0 — l'injecteur retomberait sur
   **une** destination, l'inverse exact de ce que R2 demande.
2. Le bloc porté de `l0Dest` (`2250700%06d`, `1+ordinal%pool`) **recouvre** celui du `Dest` par défaut
   de l'injecteur (`+2250700%06d`, `seq%10⁶`, défaut de `Dest` dans `withDefaults`). Une destination tirée
   comme « non portée » pourrait donc être portée en base : `mixHolds` calcule `pg_hit` attendu à partir
   de `share`, et lirait un mélange que sa propre géométrie ne prédit pas.
3. **Celle qu'on ne voyait pas.** `newPayloads` n'appelle `cfg.Dest(i)` que pour `i ∈ [0, ring)`
   (`newPayloads`, `inject.go`) : l'anneau plafonne donc aussi la cardinalité **portée**. À `ring = 4096` et
   `share = 0,3`, le tirage ne touche que ~1 200 numéros portés **quel que soit** `REF_PORTED_POOL`.
   Le cadran de localité de step-270c ne commanderait rien dans le run plein-stack.

D'où la composition : porté → `l0Dest` **tel quel**, aucune copie ; non porté → étalement sur
`pool + 1 + i%ring`. Les portés vivent dans `[1, pool]`, donc la disjonction est **arithmétique** et
s'assert contre `portedSet` — pas un bloc neuf à justifier, pas une convention à retenir.

Et le point 3 devient une garde plutôt qu'une note : `ring ≥ pool × 1000 / num`, sinon le run
**refuse**. C'est le seul moyen que le levier de cardinalité commande quelque chose à `share > 0` ;
c'est aussi l'assertion que la mutation « figer l'anneau à 4 096 » doit faire tomber. Sans elle, R2
livre un bouton : le run publierait une lecture Postgres nulle comme un résultat, et aucune garde ne
le verrait — celle du mélange compare au tirage **de l'anneau**, donc un anneau trop étroit est cohérent
avec lui-même.

### D2 — La bande de reproductibilité n'existe pas : cette PR l'établit

La chaîne de preuves demande de comparer la relance à `share=0` « à la bande de reproductibilité
consignée dans le journal ». Vérification faite, **il n'y en a pas** pour le run de référence : la
bande « −0,1 à −1,0 % » est celle du **banc routeur isolé** (`test/load/README.md:1004`), et le run de
référence n'a qu'un ordre de grandeur (1 100–1 200/s, 03/08) sous une réserve qui dit « un seul hôte,
une seule mesure par configuration » (`:723-728`). Le journal déclare par ailleurs invalide toute
comparaison entre deux de ses sections.

Elle s'établit donc ici : **trois runs sur `main`**, puis **trois runs après le câblage**, même hôte,
même session. La dispersion des trois « avant » EST la bande, et c'est la seule forme que ce journal
reconnaisse — une section datée, un hôte, une session.

Écarté : une bascule `REF_L0=off`. Elle laisserait dans le harnais un chemin permanent que la
production n'a pas, pour un delta que D1 de step-270c prédit **nul par construction** — le run de
référence *tient* `REF_RATE`, son débit est une entrée. Ce que les six runs mesurent n'est donc pas un
débit : c'est de savoir si le Redis ajouté et la porte Bloom sortent la p99, la part pipeline et le
verdict de la dispersion propre du run.

### D3 — La garde R4 porte sur l'assignation, pas sur le fan-out

`⌈partitions / minReplicas⌉` est le nombre de partitions qu'un pod peut se voir **assigner** par le
rebalance du groupe. C'est vrai de tout membre, quel que soit son style de consommation — donc la garde
prend le même filtre que `hpa-max-replicas` (métrique `queue` ≠ `mt.routed`), soit `router-svc` et
`mo-dlr-router-svc`.

Le message énonce ce fait-là, et cite `handleBatch` (`internal/router/router.go:124`) comme le cas où
la borne est **serrée** : une goroutine par partition assignée, chacune séquentielle, donc au plus une
acquisition pgx concurrente par voie. Il n'affirme pas que `mo-dlr-router-svc` fan-oute — il consomme
avec `Consumer.Run`, un enregistrement à la fois, et passe la garde avec de la marge.

Écarté : cibler `router-svc` par son nom. Un nom de Deployment en dur dans une garde pourrit au premier
service qui passe à `RunBatch`, et c'est exactement ce que R4 reproche à la prose qu'il corrige.

### Leviers

| Levier | Défaut | Ce qu'il commande |
|---|---|---|
| `steady.InjectConfig.DestRing` / `REF_DEST_RING` | **4 096** | destinations distinctes pré-rendues — le run d'hier ne bouge pas tant que personne ne tourne le bouton |
| `REF_PORTED_SHARE` | **0** | part portée du run de référence ; déjà déclaré par step-270c |
| `REF_PORTED_POOL` | celui de step-270c | ensemble porté distinct, donc la localité |
| `EXACT_CACHE_TTL` | **6h** | TTL du cache `exactroute:{msisdn}` — le levier, pas sa valeur |

## Chaîne de preuves

L'ordre est contraignant, et le premier point n'est pas une formalité.

1. **R2 puis R1**, jamais l'inverse : un L0 câblé sur 4 096 destinations rend une lecture Postgres
   nulle qu'aucune garde ne peut contredire, puisque le mélange attendu se déduit du tirage de
   l'anneau. La mesure serait fausse *et* verte.
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

- [x] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [x] levier de cardinalité livré, défaut inchangé, et la mutation qui le fige vue tomber
- [x] la garde `ring ≥ pool × 1000 / num` livrée (D1, point 3) : sans elle le levier est un bouton qui
      ne commande rien à `share > 0`, parce que `newPayloads` n'échantillonne `Dest` que sur l'anneau
- [x] le run de référence traverse L0 avec le vrai `NewL0Resolver`, semé par les fonctions pures de
      step-270c — aucune copie
- [x] relance à `share=0` consignée **contre la bande**, quel que soit son verdict — bande qui n'existe
      pas encore et que cette PR établit elle-même (D2), trois runs avant / trois après
- [x] clé de config du TTL livrée, câblée, et reportée dans `deploy/k8s/configmap.yaml`
- [x] `step-280.md:132-133` corrigée **et `:103-104` avec elle** — les deux portent la même erreur à
      trente lignes d'écart, et corriger l'une seule ferait se contredire la fiche
- [x] l'invariant `MaxConns ≥ ⌈partitions / minReplicas⌉` gardé par un test dans `internal/deploy`
- [x] `test/load/README.md:1157-1159` — « les deux restent à step-280 » ne survit pas à cette PR : le
      README dit ce que le run de référence traverse désormais et ce que le nouveau levier fait. Un
      document faux coûte plus cher qu'un document absent

## Ce que la livraison a trouvé

Je ne donne pas de décompte global. J'en ai annoncé un — « neuf défauts, tous découverts par une
mutation » — et il ne tenait sur aucun des deux mots : le premier de la liste vient d'une **lecture**,
que le design de cette fiche décrit lui-même, et le total ne se réconciliait avec aucun des messages de
commit. Un chiffre réestimé sans piste d'audit n'est pas une piste d'audit, c'en est le contraire ; c'est
le constat que step-270c avait déjà payé. Ce qui se vérifie est test par test.

### Trouvé en écrivant, par mutation

- **L'ordre des branches de `ringCoversPool`** : sur une configuration qui déborde *et* sous-couvre, le
  remède de la couverture — élargir l'anneau — aggrave le débordement. Le débordement passe devant.
- **Le conseil « augmenter l'anneau à au moins `pool × 1000 / num` »** annonçait un minimum faux :
  5 000 là où 4 001 couvre, 333 là où 100 suffit.
- **Le couple falsifiant de `minRingFor` était circulaire** : il tirait son attendu de la fonction que
  `ringCoversPool` utilise aussi, donc les deux côtés bougeaient ensemble.
- **Trois fixtures creuses** : la disjonction ne mord qu'à `pool > num` ; « aucune copie » passait sous
  une seconde formule parce que `pool=100` divise 700 ; le garde de l'anneau vide n'était atteignable
  qu'à `share=0`.
- **Deux branches non couvertes dans `internal/deploy`** (`minReplicas` absent, arrondi supérieur) et
  **un code mort** supprimé dans `minRingFor`.

### Trouvé par la revue en lecture seule, après coup

Deux défauts **bloquants**, tous deux invisibles aux portes vertes :

- **`mixHolds` modélise une fenêtre FROIDE, ce run n'en a jamais.** Son godoc pose sa propre
  précondition — un cache vidé avant le palier — que le banc routeur remplit par un `FLUSHDB` et que ce
  run-ci ne remplit pas. Le palier porté échouait donc **déterministement**, avec un message accusant un
  flush que ce banc n'a jamais été écrit pour faire. J'avais écrit « mixHolds est le filet ». C'était
  faux, et le seul run qui l'aurait montré est celui qui a été affamé.
- **Le garde de l'anneau acceptait un numéro hors bloc.** `refDest` tire `pool+1+i`, donc le dernier
  tirage vaut `pool+ring` : la borne devait être stricte. `REF_DEST_RING=900000` au défaut rendait
  « +22507001000000 », que l'ingress refuse — et le seul test qui pouvait l'attraper n'assérait que la
  regex, qui n'a aucune contrainte de longueur.

Plus : l'anneau **quantifie** la part portée (31,6 % à l'anneau par défaut, pas 30) · le dénominateur du
mélange était le compteur SMSC, en segments, deux étages en aval · deux **mécanismes inventés** dans mes
commentaires · une ligne de journal annonçant un ensemble porté à `share=0`, où rien n'est semé · le
banc figeait `exact.DefaultCacheTTL` alors que cette PR même en fait un levier · et six affirmations
fausses dans la documentation, dont une clause d'échec omise et une conclusion plus forte que ses
données.

**La leçon n'est pas « il fallait relire ».** C'est que les deux bloquants vivaient exactement là où le
run porté n'a pas tourné. Une porte verte sur un chemin jamais emprunté ne dit rien de ce chemin, et
cette fiche l'a écrit dans ses réserves sans en tirer la conséquence : le palier à `share > 0` n'était
pas seulement non mesuré, il était **non exécutable**.

## Hors périmètre — et qui reste à step-280

Trois décisions qui exigent l'environnement représentatif (`deploy/README.md`, « Restent trois
grandeurs à trancher »), et qu'aucun portable ne tranche :

- la **valeur retenue** de `POSTGRES_MAX_CONNS`, à arbitrer contre `max_connections` × services ×
  réplicas ;
- la **part portée représentative** (10 à 30 % en marché MNP mûr) et la localité par MSISDN ;
- la **politique d'éviction** du Redis partagé, et la valeur du TTL que R3 rend applicable.

Et, comme toujours : le **verdict NFR** lui-même.
