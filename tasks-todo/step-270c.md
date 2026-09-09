# step-270c — Rendre l'étage L0 mesurable dans le banc

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201e, step-201f, step-250e, step-270b · **Bloque :** step-280

## But

Livrer le banc qui **prix l'étage L0**, pour que step-280 puisse établir les trois grandeurs qu'elle
nomme (`tasks-todo/step-280.md:87-104`) au lieu de publier un dimensionnement qui ignore une étape du
chemin chaud.

Le banc rend des **ratios** : lookups par message, part par `outcome`, coût relatif du débit, octets
par clé Redis, attentes d'`Acquire` par message. Jamais un débit absolu — celui-là appartient au
portable, et son attribution est déjà close (step-201e, step-201f).

## Pourquoi cette fiche existe

step-280 écrit que « `test/load/` ne sème **aucune** route exacte : le banc mesure donc un profil à
0 % de trafic L0 ». Le code dit pire : **le banc ne traverse pas du tout l'étage L0.**

- `internal/e2e/reference_test.go:887-893` — `refResolver` branche `routing.SnapshotResolver` en
  direct, godoc à l'appui (« the reference run exercises declarative routing, not the script
  short-cut »). La production câble `exact.NewResolver` puis `routing.NewL0Resolver`
  (`cmd/router-svc/wiring.go:371-373, 397-400`).
- `seedRefControlPlane` (`reference_test.go:822-885`) sème 1 customer, 1 sender ID, K comptes,
  1 connecteur, 1 route catch-all. Aucune ligne d'`exact_routes` nulle part sous `internal/e2e/` ni
  `test/load/`.
- Le banc routeur isolé bouchonne le resolver entièrement (`refrouter_test.go:479-483`) et fige sa
  destination à **un seul numéro** (`refrouter_test.go:513`).

Ni le Bloom, ni le `GET` Redis, ni la lecture Postgres par clé primaire ne sont sur le chemin mesuré.
C'est le motif de `loadref-harness-fidelity-traps` : le harnais mesure autre chose que la production.

---

## Design arrêté

### D1 — Le banc porteur est le **routeur isolé**, pas le run de référence

`TestRouterL0Fidelity`, bâti sur `measureRouterCeiling` (`refrouter_test.go:48`) selon le patron exact
de `TestPoolDLRMapFidelity` (`poolfidelity_test.go:76`). Trois arguments, dans l'ordre de force :

1. **Le run de référence a un débit d'*entrée*, pas de *sortie*.** `TestReferenceRun` **tient**
   `REF_RATE` (défaut 1 200) et score l'état stationnaire : le nombre qu'il publie est un paramètre.
   Y câbler L0 et comparer donnerait Δ ≈ 0 **par construction** tant que la pile ne sature pas — le
   banc dirait « toujours stationnaire à 1 200/s » et ne prixerait rien. Le banc routeur sature sur un
   backlog pré-rempli : son débit est une *sortie*, donc `(rate_off − rate_on)/rate_off` est un coût
   par message. C'est la définition même du ratio demandé.
2. **L'attribution.** step-201e a établi que le plafond de 4 800/s du plein-stack était la
   co-résidence : tout écart lu là appartiendrait au mauvais propriétaire. Le banc isolé n'a ni
   injecteur, ni REST, ni pool, ni pair, et `fidelityDelta` refuse déjà de chiffrer un écart plus
   petit que la dispersion dont il est tiré.
3. **La grandeur n°2 n'existe que là.** « Pool pgx contre voies Kafka » ne s'observe qu'en saturant :
   le banc routeur crée un topic privé et fixe son nombre de partitions ; le run de référence partage
   `KAFKATEST_PARTITIONS` et n'est jamais borné par ses voies.

**Ce que ce choix ne donne pas, et qui va noir sur blanc dans le journal :** le Postgres est un
conteneur sur le même hôte (latence PK optimiste), la fenêtre fait 30 s contre un TTL de 6 h, et le
débit absolu du palier appartient à ce portable.

### D2 — L'A/B prix l'étage L0 seul

Côté « sans » : `refResolver{snap}` — le résolveur déclaratif **réel**.
Côté « avec » : `routing.NewL0Resolver(exact.NewResolver(...), nil, snap)`, même `snap`.

Opposer L0 au bouchon `ceilResolver` prixerait aussi l'étage déclaratif. `scripts=nil` saute L1, hors
sujet ici. **Conséquence à écrire dans le journal : le côté « sans » de ce banc n'est PAS la ligne du
balayage** (`TestRouterConsumeCeiling`, qui utilise le bouchon).

### D3 — « L0 câblé à 0 route exacte » n'est pas neutre, et c'est une des mesures

`exact.LoadBloom` sur une table vide construit quand même un filtre (`minBloomCapacity = 1024`,
`bloom.go:21`) : chaque message paie un vrai `MightContain` (1 hash + k tests de bits, fp=0,001), plus
le `defer func(){ r.meter.Observe(outcome) }()` de `resolver.go:160-161`, plus un saut d'interface.

Le palier `share=0` est donc **publié comme mesure** : c'est ce que paient les 70-90 % de trafic non
porté en production. La fiche refuse d'écrire « neutre » là où le code dit « une porte ».

Ce qui reproduit l'existant, c'est que **le chemin d'hier n'est pas modifié** :
`TestRouterConsumeCeiling` garde `ceilResolver` et appelle le lit avec `(share=0, pool=0)` ⇒ `l0Dest`
rend le littéral d'hier. C'est `TestL0DestReproducesTheLegacyFixture` qui le prouve, pas un commentaire.

### D4 — La géométrie : `share` et `pool`, déterministes par index

La localité par MSISDN sur un TTL de 6 h n'est pas simulable dans une fenêtre de 30 s. On ne la simule
donc pas : **on la fabrique**, par la taille de l'ensemble de travail porté, avec un `FLUSHDB` par
palier.

```
l0Dest(i int, share float64, pool int) string   // pure, testée hors build tag
```

- `i` indexe l'enregistrement du prefill. Le placement en partition dépend de la clé **compte**, pas
  de la destination : la répartition des portés sur les voies est uniforme gratuitement.
- `share` (`REF_PORTED_SHARE`) : part portée, **déterministe** (`i%den < num`), jamais aléatoire — la
  garde doit pouvoir calculer le mélange attendu exactement.
- `pool` (`REF_PORTED_POOL`) : MSISDN portés **distincts** = lignes semées dans `exact_routes` =
  entrées du Bloom = ensemble de travail. `porté = blocPorté + (i/den)%pool`.
- Non porté : le littéral d'aujourd'hui, **bloc disjoint** du bloc porté. `share=0` ⇒ exactement
  `"2250700000000"`.

Avec `W` = lookups portés dans la fenêtre :

| Régime | Condition | Ce qu'il prix |
|---|---|---|
| **porte Bloom** | `share=0` | ce que paient les non-portés |
| **borne chaude** | `pool ≪ W` | `pg_hit ≈ pool`, le reste `redis_hit` — le bras Redis |
| **borne froide** | `pool ≥ W` | 100 % `pg_hit` de la part portée — le bras Postgres, et la saturation du pool pgx |

step-280 interpole avec **sa** localité :
`coût/msg = share·[(1−L)·c_pg + L·c_redis] + (1−share)·c_bloom`.

### D5 — Les autres décisions

| # | Décision | Pourquoi |
|---|---|---|
| D5 | `FLUSHDB` avant chaque palier « avec » | patron `freshStore` (`poolfidelity_test.go:118`). Sans lui le 2ᵉ palier lit un cache déjà chaud et `pg_hit` s'effondre en silence |
| D6 | Pool pgx **dédié** — `postgres.NewPool(ctx, pgtest.Config(t))`, `MaxConns` = `REF_PG_MAX_CONNS` (défaut **10**, celui de production) | `pgtest.Pool` est **partagé** par le paquet et ouvert par `pgxpool.New(ctx, url)` sans config (`pgtest.go:103`) : inutilisable pour un dimensionnement. Le semis passe par le pool partagé ; le pool dédié ne porte que la fenêtre |
| D7 | 12 voies, **constante** `l0Lanes`, pas de levier | patron `poolFidelityBinds = 8` : on mesure là où step-270 opère. 12 = `TopicPartitions` de production (`config.go:263`), le nombre même que step-280 oppose à `MaxConns=10`. Aucune env var tant que personne ne balaie |
| D8 | `measureRouterCeiling` scindé en `newRouterBed` + palier retournant `float64` | **forcé** : `newCeilingTopic` pose son `t.Cleanup` sur le *test* (`refrouter_test.go:212-221`) et son propre commentaire nomme le risque (« five of them fill the single-node broker's volume »). Six paliers entrelacés × ~100 Mo resteraient jusqu'à la fin. Patron `newPoolBed`/`measurePoolCeiling` |
| D9 | Compteur d'`outcome` local (`countingLookups`), pas Prometheus | patron `countingProducer`/`countingCDR` : atomiques, rien de retenu, aucun scrutin HTTP dans la fenêtre |
| D10 | `fidelityDelta` gagne un paramètre `subject string` | il code « the DLR store » en dur (`refceiling_test.go:294-305`) : une ligne L0 nommerait le mauvais banc. 1 argument, 1 site d'appel existant |
| D11 | Empreinte Redis **mesurée** (`DBSIZE` + `used_memory`, bracketés) | transforme la grandeur n°3 d'une arithmétique (« ~150-200 o par clé », step-250e) en une constante mesurée — et **celle-là** se transpose |
| D12 | **Zéro changement du chemin chaud de production** | tout est déjà paramétré : `NewResolver(ttl)`, `NewL0Resolver`, `NewPool(cfg)`, `pipeline.Resolver` déclarée côté consommateur. Si l'implémentation en découvre un nécessaire, c'est un rouge de conception : s'arrêter et le nommer |

### Leviers

| Env | Make | Défaut | Rôle |
|---|---|---|---|
| `REF_PORTED_SHARE` | `PORTED_SHARE=` | `0.30` | part portée. Défaut **du test**, pas de `l0Dest` : le balayage passe 0. 0,30 = haut de la fourchette MNP mûre de step-280 (dimensionnement pessimiste) |
| `REF_PORTED_POOL` | `PORTED_POOL=` | `100000` | portés distincts = lignes semées = entrées Bloom = ensemble de travail |
| `REF_PG_MAX_CONNS` | `PG_MAX_CONNS=` | `10` | `MaxConns` du pool dédié ; le défaut est celui de production (`config.go:186`) |

Réutilisés tels quels : `REF_FIDELITY_PAIRS`, `REF_CAL_HOLD` (fenêtre), `REF_PREFILL`.

---

## Tests — chacun avec la mutation qui doit le faire tomber

### Phase A — fonctions pures, **hors build tag** (`refceiling_test.go`)

| # | Test | Mutation |
|---|---|---|
| A1 | `TestL0DestReproducesTheLegacyFixture` — `share=0` ⇒ `"2250700000000"` pour tout `i` | étaler quand `share=0` → rouge (et le journal redevient illisible) |
| A2 | `TestL0DestHitsTheSeededShare` — `share=0,3`, `pool=10`, 1 000 index ⇒ exactement 300 portés, exactement 10 distincts, blocs disjoints | `i%den <= num` → 400 portés ; retirer le `%pool` → distincts ≠ 10 |
| A3 | `TestL0DestIsCanonicalE164` — tout numéro satisfait `^[1-9][0-9]+$` (CHECK de `migrations/0004`) **et** `e164.Normalize` est l'identité dessus | préfixer `"+"` → rouge. **Piège n°1 du banc** : un semis non canonique donnerait 100 % `bloom_miss`, une mesure nulle qui a l'air d'un résultat |
| A4 | `TestMixHoldsRefusesARunThatNeverReachedTheStore` — `share>0` mais `pg_hit+redis_hit ≈ 0` ⇒ erreur nommée | supprimer le contrôle → vert sur un banc qui n'a jamais lu la table (classe `putsMatchSubmits`) |
| A5 | `TestMixHoldsRefusesAMixThatDoesNotTotalTheMessages` — Σ des `outcome` = messages ±2 %, l'invariant « une observation par résolution » de `resolver.go:160` vérifié de bout en bout | retirer la somme → rouge ; un `outcome` inconnu doit faire échouer la somme, pas disparaître |
| A6 | `TestMixHoldsJudgesBothEndpoints` — borne froide et borne chaude acceptées, l'inverse refusé | échanger les attendus `pg_hit`/`redis_hit` → rouge |
| A7 | `TestMixHoldsRefusesAnErrorMix` — tout `pg_error`/`redis_error` > 0 ⇒ erreur nommant la saturation du pool comme premier suspect | retirer → le palier échoue plus loin sur « N messages rejetés », diagnostic perdu |
| A8 | `TestPoolPressureNamesTheStarvation` — `emptyAcquires > 0` ⇒ le rendu le dit ; `= 0` ⇒ il dit que le pool ne s'est jamais vidé | supprimer la branche → un pool saturé s'imprime comme un pool sain |
| A9 | `TestPoolPressureRefusesToDivide` — 0 message ou fenêtre nulle ⇒ « unreadable », jamais `NaN` | diviser quand même → `NaN` en sortie |
| A10 | `TestCacheFootprintPricesOnlyTheKeysItAdded` — `(Δmémoire)/(Δclés)`, refuse `Δclés ≤ 0` | utiliser `memAfter` absolu → l'octet/clé absorbe tout le Redis |
| A11 | `TestFidelityDeltaNamesItsSubject` — le sujet apparaît dans les trois branches | garder « DLR store » en dur → une ligne L0 nomme le mauvais banc |

A11 ajuste les quatre tests existants de `fidelityDelta` (un argument) ; ils doivent rester verts — c'est le filet.

### Phase B — scission mécanique, sous tag `loadref`

`newRouterBed` + palier retournant `float64`, paramètres `resolver` et `lookups`, appelants inchangés
en comportement. **Rouge lu :** relancer `TestRouterConsumeCeiling` et vérifier que la courbe retombe
dans la bande de reproductibilité consignée. **Mutation :** passer `share=0,3` au balayage → la courbe
bouge → preuve que le lit est bien celui qu'on croit.

### Phase C — le banc, sous tag `loadref`

- C1. Semis : `l0Dest` → `BulkUpsert` par tranches de 1 000 → `exact.LoadBloom`.
  **Rouge à provoquer et à lire :** construire le Bloom **avant** le semis ⇒ 100 % `bloom_miss` ⇒ A4
  échoue avec son message. Démonstration que la garde attrape « le banc tenait un vrai magasin et ne
  l'a jamais appelé ».
- C2. Préflight hors fenêtre : `MightContain` vrai sur un échantillon de portés, faux sur le non-porté
  — échec immédiat plutôt qu'après 30 s.
- C3. Couples entrelacés, ordre alterné, `FLUSHDB` avant chaque palier « avec ».
  **Rouge à provoquer :** retirer le `FLUSHDB` ⇒ dès le 2ᵉ palier `pg_hit → 0` ⇒ A6 échoue sur la
  borne froide. Preuve que `REF_PORTED_POOL` est un vrai cadran de localité.
- C4. Rendus : mélange des `outcome`, `poolPressure`, `cacheFootprint`,
  `fidelityDelta(..., "the L0 stage")`.

**Aucun vert déclaré avant d'avoir vu tomber la mutation correspondante.**

---

## Pièges, dans les godoc et pas à découvrir en run

1. **Hôte qui vient de travailler** (`test/load/README.md:978-980`) : lancer ce banc **seul**, hôte
   reposé. L'entrelacement défend contre la dérive lente, pas contre 25 min de bancs enchaînés.
2. **`FLUSHDB` vide toute la base Redis**, partagée avec le banc DLR : sûr parce que les tests d'un
   paquet sont séquentiels et que tout vit derrière `loadref` — même réserve que `freshStore`.
3. **Un palier peut échouer parce que le pool a saturé** : `Acquire` attend jusqu'à
   `DefaultLookupTimeout = 2 s` (`resolver.go:26`) → `pg_error` → message rejeté → la garde
   `countingCDR` fait tomber le palier. **C'est un résultat, pas un bug du harnais** : A7 le nomme
   avant que la garde CDR ne parle, et la valeur de `REF_PG_MAX_CONNS` à laquelle ça apparaît **est**
   la réponse de la grandeur n°2.
4. `pgtest.Pool` est **partagé** : ne jamais lire ses `Stat()` comme une mesure.
5. Le côté « sans » de ce banc n'est **pas** la ligne du balayage.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats) ; `make test` inchangé en durée
- [ ] 11 tests purs verts hors build tag ; pour chacun, la mutation listée a été **vue** rouge
- [ ] `TestRouterConsumeCeiling` relancé : courbe dans la bande de reproductibilité déjà publiée
- [ ] `TestRouterL0Fidelity` livre les **trois** lignes sur hôte reposé — porte Bloom, borne chaude,
      borne froide — chacune avec son mélange d'`outcome`, ses lookups/message et son verdict
      `fidelityDelta` (y compris « illisible », qui est un résultat)
- [ ] pression du pool pgx consignée à `MaxConns=10` **et** à une valeur relevée, à 12 voies :
      acquisitions/message, attente moyenne, part d'`Acquire` sur pool vide, et la valeur où
      `pg_error` apparaît s'il apparaît
- [ ] empreinte Redis consignée en **octets par clé** `exactroute:{msisdn}` — mesurée, pas déduite
- [ ] les deux rouges d'intégration provoqués et lus : Bloom construit avant le semis (A4), `FLUSHDB`
      retiré (A6)
- [ ] `test/load/README.md` : section **ajoutée**, mentionnant explicitement que le côté « sans »
      n'est pas la ligne du balayage, que `payloadRing = 4096` plafonne l'injecteur du run de
      référence à 4 096 destinations, et que ces chiffres sont des **ratios**
- [ ] `step-280.md` : bloc « Mise à jour » listant l'acquis et ce qui reste bloqué sur le matériel ;
      **step-280 reste À FAIRE**
- [ ] `git diff --stat` ne montre que `internal/e2e/`, `test/load/README.md`, `Makefile`, `tasks-*`

## Hors périmètre — et qui retourne à step-280

- **Le verdict NFR lui-même**, et les débits absolus (`pg_hit/s` à 8 000 SMS/s, empreinte en Go).
- **Levier de config du TTL de production** — `cmd/router-svc/wiring.go:397` fige
  `exact.DefaultCacheTTL`. Le banc n'en a pas besoin : `exact.NewResolver` prend déjà le TTL en
  paramètre (`resolver.go:127`), et une fenêtre de 30 s n'observe pas une expiration à 6 h. Ce qui
  règle la localité ici est le `FLUSHDB` et `REF_PORTED_POOL` — un *reset de fixture*, pas une
  expiration. Le banc passe `exact.DefaultCacheTTL` **explicitement**, jamais 0.
- **Valeur retenue de `POSTGRES_MAX_CONNS`** et son report dans `deploy/k8s/configmap.yaml:45` : une
  valeur de production ne se choisit pas sur un portable, mais avec le ratio d'ici et
  `max_connections=100 × services × réplicas` (step-201 `D9`).
- **Exposition de `DefaultLookupTimeout`** (`resolver.go:26`) : n'a de sens que face à une vraie file pgx.
- **L0 dans le run de référence plein-stack**, et le plafond de 4 096 destinations de `payloadRing`.
- **Le bloc MSISDN de `test/load/k6/messages.js`** reste verrouillé → step-410 (verrou d'envoi).
- **L1 (scripts de routage)** : `scripts=nil`, hors sujet.
- **Cache négatif sur `pg_miss`**, `singleflight` par msisdn, visibilité d'un `DEL` perdu : suites
  ouvertes de step-250e. Ce banc leur fournit un dénominateur, il ne les tranche pas.
- **Rechargement du Bloom sous charge** : famille chaos, step-250/260.
