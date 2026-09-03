# step-260d — `least_loaded` lit une clé que personne n'écrit : le pool dérive `connectorload`

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-280 (la campagne NFR mesure le profil de routage)

## Pourquoi cette fiche existe

L'audit du 2026-09-03 a trouvé, et vérifié, que `cmd/router-svc/wiring.go:840-851` lit
`connectorload:{id}` par message pour la stratégie `least_loaded`, et que **rien dans le dépôt n'écrit
cette clé** — ni Go, ni Lua. `grep -rn connectorload` ne rend que le lecteur, la spec et les guides. Le
lecteur avale toute erreur en `0` ; `strategy.LeastLoaded` départage à égalité sur l'UUID ; donc depuis
step-114 la stratégie choisit **toujours le plus petit UUID**, sans métrique ni log qui le dise.

step-114 (`tasks-done/step-114.md:10,16`) avait livré le lecteur et écrit « tolère l'absence de la
clé ». C'est cette tolérance qui a rendu un écrivain absent acceptable pendant deux jalons — le même
mécanisme que step-250e a trouvé pour `exactroute:{msisdn}`. Tous les tests de `least_loaded`
injectent une jauge factice (`fakeLoad`, `strategy_integration_test.go:134`) : le défaut est
structurellement invisible à la suite.

## Ce que l'exploration a établi

- La donnée source **existe et transite déjà** : `bind.inFlight() = len(b.window)`
  (`internal/connectorpool/bind.go:181`, l'occupation de la fenêtre SMPP) est publiée toutes les 2 s
  par `runStatusHeartbeat` (`connectorpool.go:617-653`) via `StatusControl.PublishBind`
  (`internal/connector/status/status.go`) : un `HSET connector:binds:{id} pod:idx → JSON
  {link_status,in_flight,ts}` + `PEXPIRE 30 s`. `status.Reader.Read` filtre les champs périmés à la
  lecture mais **n'agrège pas** ; seule l'Admin l'appelle.
- La spec l'exige dérivée : `docs/specification-technique-passerelle-sms.md:417` (« periodically-published
  in-flight gauge per connector ») et `:814` (« la charge (`connectorload`) suit le même schéma » que
  `breaker:state`, c'est-à-dire un agrégat multi-pod **dérivé** des champs que chaque pod écrit seul).
  `:927` annonce un second lecteur : le draineur de reroutage.
- Le modèle est prêt : `internal/connector/breaker/aggregate.lua` — un script, des clés à hash tag commun,
  `HSET` → `HGETALL` → balayage TTL → agrégat → `SET` + `PEXPIRE`. La règle d'or (`.claude/rules/go-code.md`)
  interdit de faire cette somme depuis Go.
- Le routeur lit `breaker:state` une fois par rebuild (`wiring.go:855-885`) et **jamais** par message
  (`snapshot.go:55-59`) ; `least_loaded`, lui, fait un `GET` Redis **par message** — l'asymétrie n'est
  écrite nulle part.

## Design arrêté

**L'écrivain : `PublishBind` devient un script Lua.** `internal/connector/status/publish_bind.lua`,
embarqué et exécuté via `redisstore.NewScript` exactement comme `aggregate.lua`. Deux clés au même hash
tag `{connector_id}` : `KEYS[1] = connector:binds:{id}`, `KEYS[2] = connectorload:{id}` ;
`ARGV = field, JSON BindEntry, now_ms, ttl_ms`. Corps : `HSET` de ce bind → `HGETALL` → pour chaque
champ, `cjson.decode` sous `pcall` ; si le champ est illisible ou si `ts ≠ 0` et `now − ts > ttl`, `HDEL`
(balayage, comme `aggregate.lua`) ; sinon `sum += in_flight` → `SET KEYS[2] sum PX ttl` → `PEXPIRE
KEYS[1] ttl` → `return sum`. La **signature Go de `PublishBind` ne change pas** : `StatusControl`
(`connectorpool.go:175-178`) et `cmd/connector-pool-svc/wiring.go:160` restent tels quels, et le dernier
`PublishBind` d'un cycle de heartbeat voit toute la table et écrit la somme finale. `cjson` est
disponible dans `redis:7-alpine` (l'image de `redistest`, la même que `docker-compose.yml`) et dans
Dragonfly ; c'est sa première utilisation dans le dépôt, le premier test d'intégration le prouve. Le
filtre de péremption à la lecture dans `Read` reste (ceinture et bretelles).

*Écarté* : faire sommer le routeur (`HGETALL` + décodage de P×B champs JSON, par cible, par message, à
8 000 msg/s — et une seconde implémentation de l'agrégat le jour où le draineur de reroutage lit la
même charge). La clé dérivée est le contrat partagé : Admin, routeur, draineur lisent un entier.

**Le lecteur sort de `wiring.go`.** Nouveau `internal/connector/status/load.go` : `LoadKey(id)` (la
clé n'est plus un littéral dans un main), `LoadReader` construit par `NewLoadReader(rdb, opts...)`, dont
`InFlight(ctx, id) int` satisfait `routing.LoadReader` (`snapshot.go:52-54`, **inchangée**). Options :
`WithLoadCacheTTL` (défaut **1 s**, `0` = pas de cache, pour les tests), `WithLoadMeter`,
`WithLoadClock`. Clé absente → `0` ; erreur Redis → `0` + un `WarnContext`, naturellement limité par le
cache. **Politique chemin chaud : cache mémoire par connecteur, TTL 1 s.** La jauge est republiée
toutes les 2 s : un `GET` par message n'apporte rien de plus qu'un cache de 1 s, et le coût passe de
~16 000 GET/s (deux cibles à 8 000 msg/s) à 2 GET/s par pod.

*Écartés* : lire au rebuild seulement, comme `breaker:state` — le snapshot n'est reconstruit que sur
invalidation de config, la charge serait gelée entre deux changements et `least_loaded` redeviendrait
statique ; une boucle `MGET` périodique en overlay atomique — propre, mais elle touche `snapshot.go`,
les fakes de routage et ajoute une goroutine supervisée : c'est la suite si le p99 le réclame, pas
cette PR.

**Observabilité.** Compteur borné `routing_connector_load_reads_total{outcome="hit|missing|error"}`
dans `metrics.Catalog`, adaptateur `loadMeter` sur le modèle de `lookupMeter` (`wiring.go:831-833`).
Sans lui, « clé absente » et « tout le monde à zéro » restent indiscernables — c'est exactement ce qui
a caché le défaut. Avec le cache, au plus un incrément par seconde et par connecteur.

**Ce qui ne bouge pas.** `routing.LoadReader`, `SnapshotResolver.UseLoadReader`, `strategy.LeastLoaded`,
`Deps.StatusControl`, la cadence du heartbeat, `tasks-done/step-114.md` (document daté).

## Chaîne de preuves — le rouge d'abord, la mutation ensuite

1. `internal/connector/status/load_integration_test.go` (redistest) —
   `TestPublishBindDerivesConnectorLoad` : `PublishBind` pod-a/0 → 3, pod-a/1 → 4, pod-b/0 → 5 ⇒
   `GET LoadKey(id) == 12`, `PTTL > 0`, et `NewLoadReader(rdb, WithLoadCacheTTL(0)).InFlight == 12`.
   Rouge attendu : `LoadKey` / `NewLoadReader` n'existent pas. Mutation : retirer le `SET` du script → 0.
   `TestPublishBindSweepsStaleBinds` : un champ posé à la main avec `ts` à −10 min et `in_flight 99`,
   puis un `PublishBind` frais à 2 ⇒ clé = 2 et le champ périmé a disparu du hash (mutation : retirer la
   comparaison `now − ts > ttl` → 101). `TestLoadReaderCachesWithinTTL` : un faux `loadStore` qui compte
   les `GET`, horloge injectée ; deux `InFlight` dans le TTL = 1 `GET`, après avance = 2.
2. `internal/connectorpool/load_integration_test.go` (redistest + fakesmsc, sur le modèle de
   `chaos_integration_test.go`) — « le heartbeat publie → le lecteur lit une somme non nulle » : pool
   réel, `StatusHeartbeat` court, fenêtre occupée par des réponses retardées ⇒ `InFlight > 0` en moins
   de 2 s. Fixture non creuse : « ne pas écrire la clé » **et** « publier 0 au lieu de `b.inFlight()` »
   (`connectorpool.go:623`) la font tomber.
3. `internal/routing/load_integration_test.go` (redistest) — « la cible CHANGE quand la charge change » :
   route `least_loaded` A/B ; publier A=10, B=1 ⇒ `Resolve` = B ; publier A=0, B=20 ⇒ `Resolve` = A. Sans
   écrivain, les deux lectures valent 0 et le départage UUID donne deux fois le même gagnant : une des
   deux assertions tombe, toujours. `TestResolveLeastLoaded` (`fakeLoad`) reste comme unitaire.
4. `cmd/router-svc/wiring_test.go` — `TestNewRouterAppRoutesLeastLoadedOnThePublishedGauge` (pgtest +
   redistest, sur le modèle de `TestNewRouterAppBuildsTheWholeGraph`) : deux connecteurs et une route
   `least_loaded` en base, charges publiées via `status.NewReader(rdb).PublishBind`, résolution à
   travers le résolveur **du graphe réel**. Mutation : supprimer la ligne `UseLoadReader` → départage
   UUID → tombe. C'est le seul test qui prouve le câblage — celui qui manquait depuis step-114.

## Commits

1. Cette fiche, et la section « Audit du 2026-09-03 » de l'INDEX.
2. `status` : script Lua, `LoadKey`, `load.go`, tests 1.
3. `connectorpool` : test 2.
4. `routing` : test 3.
5. `router-svc` : câblage, compteur, test 4 ; suppression de `connectorLoad`.
6. `docs` : `spec:417` précise « dérivée par le script de `PublishBind`, TTL 30 s » ; fiche → `tasks-done/`.

## Arbitrages que la spec ne tranche pas — écrits ici, pas décidés en silence

- **Sémantique de la charge.** `in_flight` est l'occupation de la fenêtre SMPP (0..`WindowSize × binds`),
  pas le lag Kafka du connecteur. `spec:417` dit « in-flight » : retenu. Conséquence assumée : à faible
  trafic tout vaut 0 et `least_loaded` se comporte comme aujourd'hui.
- **Départage à égalité.** UUID (déterministe, concentre tout sur une cible quand tout vaut 0) plutôt
  que round-robin. **Hors périmètre** de cette fiche ; à trancher avant de vendre `least_loaded` comme
  équilibrage. Suite à ouvrir si la campagne le montre.
- **TTL du cache** : 1 s, la moitié de la cadence de publication. **Binds `down`/`reconnecting`** :
  sommés comme les autres — leur fenêtre se vide d'elle-même.

## Definition of Done

- [ ] `make check` vert
- [ ] après un heartbeat, `GET connectorload:{id}` = Σ `in_flight` des champs vivants, avec TTL
- [ ] sur une route `least_loaded`, la cible change quand la charge publiée change, via le graphe réel de router-svc
- [ ] plus un `GET` par message (test de cache) ; l'absence de jauge est comptée
- [ ] chaque rouge lu, chaque mutation vue tomber, citée dans la PR
- [ ] `spec:417` corrigée ; `tasks-done/step-114.md` **non** réécrite (document daté)

## Hors périmètre

Le départage à égalité ; le lecteur du draineur de reroutage (`spec:927`) ; l'overlay `MGET`
périodique ; toute modification de `strategy.LeastLoaded`, de `snapshot.go` ou de la cadence du
heartbeat.
