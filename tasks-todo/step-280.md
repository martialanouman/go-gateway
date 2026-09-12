# step-280 — Campagne NFR pleine échelle sur environnement représentatif

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-201, **step-201c**, **step-201d**, **step-201e**, **step-201f**, step-270,
> **step-270b**, **step-270c** · **Bloque :** step-410

## But
Rendre le **verdict NFR** que step-201 ne pouvait pas rendre : débit soutenu **8 000 SMS/s**, pic
**15 000**, ingestion p99 < 250 ms, bout-en-bout p99 < 2 s, disjoncteur fermé (spec §1.2, plan §16).

## Pourquoi cette fiche existe
Le débit soutenu de la spec est **traversant**, pas un débit d'acceptation (step-201 `D1`). Le tenir
suppose ~10 400 `submit_sm/s` absorbés en sortie — soit **≥ 52 binds** sur un simulateur qui sérialise
le service par bind — pendant que tournent 9 services, 4 magasins et l'injecteur. La spec §2.5
dimensionne la cible à 8–16 vCPU de workers *dédiés* plus un Kafka répliqué 3. Aucune machine de
développement ne porte ça : un « 8 000/s tenu » mesuré là ne validerait rien, et un échec ne
condamnerait rien.

step-201 a donc livré les **instruments** et prouvé l'état stationnaire à la borne basse du modèle
par-worker (§2.5). Ici, seule **l'échelle** change.

> **Correction (08/08/2026, après step-201d).** « Les instruments sont réutilisés tels quels » n'est plus
> vrai. step-201d les a poussés jusqu'à leur limite : à 4 800 msg/s le harnais ne sait plus **attribuer**
> un plafond — l'injecteur décroche de 17,3 %, les étages se disputent l'hôte, et la comptabilité CPU ne
> voit que le processus Go quand le suspect probable est un conteneur. Ce qui manque est listé et
> planifié en **step-201e**, qui bloque cette fiche.

> **Mise à jour (09/08/2026, step-201e livrée).** Le harnais sait désormais attribuer, et il l'a fait :
> le plafond de 4 800 msg/s était la **co-résidence**, ni le routeur (×4,2 une fois isolé) ni le broker
> (131 µs de latence de service pour 0,56 cœur). Reste **un seul étage jamais mesuré seul** — le pool de
> connecteurs, qui borne aujourd'hui le bout-en-bout à 2 400/s contre les 10 400 `submit_sm/s` de la
> cible. C'est **step-201f**, et elle bloque cette fiche pour la même raison que step-201e la bloquait :
> une campagne pleine échelle qui démarre sans savoir à qui appartient le plafond mesurera l'hôte.

> **Mise à jour (09/09/2026, step-270c ouverte).** Le prérequis logiciel de la section « profil de
> routage L0 » est plus profond que ce qu'elle décrit : le banc ne mesure pas 0 % de trafic L0, **il ne
> traverse pas du tout l'étage L0**. `internal/e2e/reference_test.go:887-893` branche le résolveur
> déclaratif en direct, et le banc routeur isolé bouchonne le résolveur entièrement en figeant sa
> destination à un seul numéro. **step-270c** livre le banc qui prix l'étage — les *ratios* (lookups
> par message, part par `outcome`, octets par clé Redis, pression du pool pgx), qui seuls se
> transposent depuis un portable. Elle ne rend aucun verdict et ne débloque pas le matériel.

## Prérequis logiciel : step-201c
Le run de référence de step-201 a mesuré un plafond de sortie de **192–330 `submit_sm/s`** dû à quatre
allers-retours ClickHouse par message dans le `connector-pool-svc`. Mesurer à pleine échelle avant de
l'avoir levé mesurerait ce goulot, pas la passerelle.

## Prérequis logiciel : step-270b
Les manifests existent depuis step-270, mais **aucune image ne les accompagne** : ils nomment des
images GHCR que rien ne construit encore. Un environnement représentatif ne se monte pas sans elles.

## Prérequis matériel (à provisionner — ce n'est pas du code)
- Environnement représentatif : workers dédiés, Kafka **répliqué 3**, ClickHouse et Postgres séparés
  des workers, simulateur SMSC sur sa propre machine (sinon il concourt pour le CPU qu'il mesure).
- Dimensionné d'après la spec §2.5 et les valeurs de leviers retenues en step-201.
- La dépendance à **step-270** est structurelle : les manifests Kubernetes sont ce qui rend cet
  environnement instanciable de façon reproductible.

## Périmètre
- Provisionner l'environnement et y appliquer les leviers de step-201 (`D5`) + le provisionneur de
  topics (`D7`).
- Établir le **plafond du pair à cette échelle** avec l'injecteur de step-201 (`D3`) — il ne se déduit
  pas de la mesure locale : le simulateur y tourne sur une autre machine, avec un autre nombre de binds.
- Runs `sustained` (8 000/s) et `peak` (15 000/s) du harnais, en **état stationnaire** : sortie =
  acceptation, lag consumer plat.
- Mesurer la latence bout-en-bout par corrélation `message_id` (step-201 `D4`).
- Consigner les valeurs de leviers retenues, la courbe débit-vs-ressources et le goulot identifié.

## Points d'implémentation clés
- **Le plafond du pair d'abord, le tuning ensuite.** Un run de référence au niveau du plafond du
  simulateur ne prouve rien de la passerelle. Si le plafond reste sous 10 400 `submit_sm/s` malgré le
  balayage de binds, c'est le simulateur qu'il faut traiter (sharding des goroutines, cf. sa propre
  spec §250) — pas la passerelle qu'il faut régler contre une contrainte artificielle.
- **Mesurer aussi le chemin `Idempotency-Key`** (`IDEMPOTENCY=on`, step-201 `D10`) : les NFR déclarés
  tenus sur le seul cas favorable ne valent pas.
- Dimensionner le `maxmemory` du Redis cible : un run `peak` ajoute ~900 000 clés d'idempotence à 24 h
  de TTL (~150 Mo), cumulables sur la fenêtre (step-201 `D12`).
- Aucun réglage ne doit affaiblir un invariant (idempotence, ordre, non-fuite) ni toucher aux six
  frontières de contrat listées en step-201 `D6`.

## Tests
- Plafond du pair mesuré et consigné **à cette échelle**, et les runs de référence se situent en dessous.
- `sustained` : 8 000 SMS/s traversants soutenus ≥ 10 min, lag plat, p99 ingestion < 250 ms,
  bout-en-bout p99 < 2 s, disjoncteur fermé.
- `peak` : 15 000 SMS/s tenus sur la durée de pic, dégradation conforme aux politiques documentées.
- Les 4 invariants (a/b/c/d) restent verts sous charge.

## Hérité de step-250e — le profil de routage L0

`test/load/` ne sème **aucune** route exacte : le banc mesure donc un profil à 0 % de trafic L0, alors
que le court-circuit par numéro exact est désormais fonctionnel et qu'il change le coût par message.
Depuis step-250e, `exactroute:{msisdn}` est un cache read-through — un numéro porté coûte un `GET`
Redis en régime établi, et une lecture Postgres par clé primaire à froid ou sur faux positif du Bloom
(taux 0,001, soit ~8 req/s à 8 000 SMS/s). Le filtre pèse ~1,8 Mo par million d'entrées.

**Trois grandeurs à mesurer, que step-250e n'a pas pu établir** (revue de branche du 2026-09-02) :

1. **Débit Postgres du L0.** `pg_hit/s = débit × part portée × (1 − localité par MSISDN sur le TTL)`.
   En A2P la localité est faible : 400 à 2 400 req/s à 8 000 SMS/s pour 10-30 % de portés. S'y ajoutent
   ~8 req/s de faux positifs du Bloom, constants.
2. **Pool pgx de `router-svc`.** `MaxConns=10` par défaut, et `internal/storage/postgres/pool.go`
   annonce lui-même qu'« un jalon qui met du trafic ici devrait les revisiter ». Loi de Little : à
   2 400 req/s la marge disparaît dès ~4 ms de latence PK, et un pod peut posséder plus de lanes Kafka
   que de connexions. *(Écrit « 12 par défaut » jusqu'à step-270d : c'est le nombre de partitions du
   topic, pas ce qu'un pod se voit assigner. Le nombre de voies d'un pod est
   ⌈partitions / réplicas⌉ — voir le point 2 ci-dessous.)* Le mode de panne est vicieux : `Acquire` attend jusqu'à 2 s, puis
   erreur transitoire, donc redélivrance — qui refait le même lookup sur un pool déjà saturé. Toute
   hausse se pèse contre `max_connections`=100 × services × réplicas (step-201 D9).
3. **Empreinte Redis du cache.** `clés en vol = taux de peuplement × TTL(6 h)`, soit 1,3 à 10 Go sur le
   Redis partagé avec les soldes de facturation. Le TTL est une constante de paquet, sans levier de
   configuration : si le Redis se remplit, le recours est un redéploiement.

**Les trois grandeurs ont été mesurées par step-270c** (`TestRouterL0Fidelity`, journal du 09/09/2026),
en *ratios* — les seuls chiffres qui se transposent depuis un portable :

1. **Débit Postgres du L0** — la borne froide a tenu **~4 500 lectures/s**, et le coût de l'étage va de
   **12 %** du débit (bras Redis, pool porté petit) à **28-33 %** (bras Postgres, aucune répétition, cinq
   lectures). La porte Bloom seule est **non chiffrable** sur cet hôte : son delta de 3 % est sous la
   dispersion de 12 % des lectures dont il est tiré — mais elle ne fait **aucun** appel réseau, vérifié
   sous charge (zéro acquisition pgx sur 616 744 messages).
2. **Pool pgx — la question n'est pas celle que cette fiche posait.** `MaxConns=10` **est** atteint : sur les
   quatre runs qui portent le compteur de constructions, 5 à 33 acquisitions ont attendu derrière un pool
   plein — **une sur 4 300 à 25 300** — et **aucune** n'est explicable par une construction. Leur attente
   **totale** va de **826 µs à 12 ms** sur trente secondes, trois à quatre ordres de grandeur sous le
   `DefaultLookupTimeout` de 2 s qui ferait basculer la lecture en échec. *(Le total est le seul majorant
   par appelant que ces compteurs donnent : le diviser par le nombre d'attentes rend une moyenne, qui
   n'en est pas un.)*
   *(Aucun compteur de `pgxpool` ne mesure une famine seul : `AcquireDuration` moyenne le chemin rapide,
   et `EmptyAcquireCount` comme `EmptyAcquireWaitTime` comptent aussi les constructions de connexion. Le
   chiffre ci-dessus est un plancher de contention, `emptyAcquires − newConns`.)*
   **La loi de Little sur un débit était le mauvais modèle** : `MaxConns` borne une *concurrence*, et
   `handleBatch` (`internal/router/router.go`) ouvre une goroutine par partition dont chaque voie traite
   **séquentiellement** — un pod ne peut donc jamais offrir au pool plus d'acquisitions simultanées qu'il
   n'a de voies. **Le levier est le rapport `MaxConns` / voies par pod**, et non les 4 500 req/s.
   *(Corrigé par step-270d. Cette fiche l'annonçait « aujourd'hui 10/12 » : un cas que les manifests
   livrés par step-270 **ne produisent pas**. Les voies d'un pod sont les partitions que le rebalance
   lui assigne, soit ⌈12/4⌉ = **3** au plancher de l'HPA (`router-svc.yaml:13`, `:106`) et 2 au plafond
   de 8 réplicas — jamais 12, qui n'existerait qu'à un seul pod. Le rapport réel est donc 10/3, et la
   crainte telle qu'écrite envoyait au mauvais cadran.)*
   Ce qui remplace le chiffre est un invariant calculable et **gardé** :
   `MaxConns ≥ ⌈partitions / minReplicas⌉`, soit 10 ≥ 3 aujourd'hui, tenu par la règle
   `pool-covers-lanes` d'`internal/deploy`. Une garde plutôt qu'un paragraphe : c'est la seule forme
   qui survit au prochain changement de réplicas. Ce qui reste **ici**, et qu'aucune garde ne tranche,
   est la **valeur** de `POSTGRES_MAX_CONNS`, à peser contre `max_connections` × services × réplicas.
3. **Empreinte Redis** — **200 octets par clé** `exactroute:{msisdn}` comme majorant de
   dimensionnement (moyenne 184 sur les cinq grands échantillons, étendue 168-201 ; fourchette
   168-210 sur huit lectures, les petits échantillons étant biaisés par les tampons de connexion que
   `used_memory` compte). Cela confirme le haut de l'estimation de step-250e et place le haut de la
   fourchette à **~10,4 Go** sur le Redis partagé avec les soldes.

Ce qui reste **ici** : choisir la part portée et la localité représentatives, refaire ces mesures à
l'échelle avec un Postgres et un Redis en réseau, et en tirer le dimensionnement — plus la **valeur**
de `POSTGRES_MAX_CONNS`, que le ConfigMap partagé impose à tous les services à la fois.

**Les quatre autres résidus ont été livrés par step-270d**, parce qu'ils n'attendaient aucune machine :
le TTL du cache a sa clé (`EXACT_CACHE_TTL`, câblée et reportée dans `deploy/k8s/configmap.yaml`), le
run de référence plein-stack traverse L0 avec le vrai `NewL0Resolver`, la cardinalité des destinations
est un levier (`REF_DEST_RING`, défaut inchangé) que `ringCoversPool` refuse de laisser sous-couvrir un
pool porté, et le rapport `MaxConns`/voies ci-dessus est corrigé et gardé.

La campagne doit décider quelle part de numéros portés est représentative d'un agrégateur national
(10 à 30 % en marché MNP mûr) et semer le banc en conséquence, sans quoi le dimensionnement publié
ignorera une étape du chemin chaud. La métrique `exact_route_lookups_total{outcome}` sépare les cas.

## Definition of Done
- [ ] plafond du pair à l'échelle mesuré et consigné · runs de référence en dessous
- [ ] débit soutenu **8 000 SMS/s traversants** tenu, budgets de latence respectés (disjoncteur fermé)
- [ ] pic **15 000 SMS/s** tenu ou dégradation conforme aux politiques documentées
- [ ] valeurs de leviers retenues consignées et reportées dans les manifests de step-270
- [ ] chemin `Idempotency-Key` mesuré, pas seulement le chemin nominal
- [ ] si un NFR n'est pas tenu : consigné **nommément** comme non tenu, avec le goulot identifié —
      jamais coché par approximation

## Hors périmètre
Chaos → step-250/260. Sécurité → step-290+. Manifests → step-270 (prérequis, pas livrable d'ici).
