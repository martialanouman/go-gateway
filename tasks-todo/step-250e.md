# step-250e — La table de routage exact n'a aucun écrivain : la portabilité des numéros ne marche pas

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-280 (fidélité de la mesure), step-410

## Ce qui a été constaté

Trouvé en step-250d, en cherchant comment semer une clé pour un test de chaos : **rien n'écrit
`exactroute:{msisdn}` en Redis.**

- `config-sync` ne connaît pas les routes exactes — il ne fait que coalescer des invalidations.
- L'Admin API les persiste en **Postgres seulement** (`internal/adminapi/exact_routes.go` ne touche
  jamais Redis), alors que ses endpoints `create/update/delete-exact-route` et `import-exact-routes`
  sont livrés et servis.
- `exact.EncodeTarget` n'a **aucun appelant hors tests**.

Conséquence en production : le Bloom, lui, est bien construit depuis Postgres (`LoadBloom`) et répond
« peut-être » sur un numéro connu. La lecture Redis qui suit répond alors toujours « clé absente », et
`Resolve` renvoie `(_, false, nil)` — que l'appelant lit comme « pas d'override » et qui le fait
retomber sur le routage déclaratif par préfixe.

**Le court-circuit L0 ne résout donc jamais.** Or c'est exactement ce que le préfixe ne sait pas faire :
la spec (§6.1, §724) pose le numéro exact comme *la* réponse à la **portabilité des numéros**, parce
qu'un préfixe suppose à tort que le préfixe identifie l'opérateur. Un numéro porté part aujourd'hui chez
son ancien opérateur.

## Pourquoi personne ne l'a vu

Le repli est silencieux et parfaitement légitime en apparence : « clé absente » est un cas nominal
(faux positif du Bloom, ou synchronisation pas encore faite), pas une erreur. Rien ne distingue « pas
d'override » de « override que personne n'a jamais écrit ». Les tests du paquet sèment la clé eux-mêmes
et passent ; l'intégration (`tasks-done/step-115.md`) fait de même.

Trois commentaires du code affirmaient que `config-sync` écrivait cette clé (« the Redis value
config-sync writes (step-105) »). Ils ont été corrigés en step-250d, la PR qui découvre une affirmation
fausse la corrigeant — mais **corriger le commentaire ne répare pas la fonctionnalité**.

## Périmètre

- Décider **qui** écrit, et le construire : `config-sync` (qui a déjà la boucle d'invalidation et le
  Redis), ou l'Admin API en écriture directe, ou une projection dédiée. Le choix n'est pas neutre :
  l'`import-exact-routes` est un import de masse asynchrone (une base MNP fait des millions de lignes),
  donc la voie doit supporter un remplissage en masse **et** une mise à jour unitaire.
- La **cohérence Bloom ↔ table** : le Bloom ne doit jamais promettre un numéro que la table n'a pas
  encore, sinon chaque possible-hit paie un aller-retour Redis pour rien. L'ordre de publication compte.
- La **suppression** : `delete-exact-route` doit retirer la clé, sans quoi un numéro re-porté garde son
  ancienne cible jusqu'à… jamais (la clé est écrite sans TTL).
- Un test de bout en bout : créer une route exacte par l'Admin API, puis prouver qu'un message pour ce
  numéro emprunte **le court-circuit L0** et non le déclaratif.

## Definition of Done

- [ ] `make check` vert
- [ ] une route exacte créée par l'Admin API est résolue par L0 en bout de chaîne, prouvée par un test
- [ ] la suppression retire la clé ; la mise à jour la remplace
- [ ] l'import de masse remplit la table (et le Bloom la reflète) sans dépasser le budget mémoire annoncé
      (mesuré : **~1,8 Mo par million**. Le « ~1,2 Mo » de la spec §724 et du commentaire de
      `bloom.go` correspondait à un taux de faux positifs de 0,01, jamais à celui du code, 0,001 —
      faux depuis deux jalons. Le taux est conservé et le chiffre corrigé : depuis cette step, un
      faux positif coûte une lecture Postgres et non un miss Redis, donc le taux serré se justifie
      davantage qu'au moment où il a été choisi. Épinglé par `TestBloomSizePerMillionEntries`.)
- [ ] la note `NOTE (step-250d)` de `internal/routing/exact/resolver.go` est retirée — c'est elle qui
      dit que cette step reste à faire

## Hors périmètre

La politique de panne Redis du L0 est déjà prouvée (step-250d,
`internal/routing/exact/chaos_integration_test.go`) et n'a pas à être refaite : elle porte sur la
lecture, qui s'exécute bel et bien à chaque faux positif du Bloom.

---

## Design arrêté

Point ouvert de la fiche : **qui écrit `exactroute:{msisdn}`**. Ni la spec §6.1, ni le guide
d'ingénierie §3.9, ni l'ADR-0004 ne l'attribuent — tous trois n'assignent à `config-sync` que le
*rafraîchissement du filtre de Bloom*, qui est livré et fonctionne. L'écrivain n'a pas été « oublié
dans le code » : il n'a jamais été fiché. Escaladé au modèle Fable, qui a tranché sans contredire la
spec.

**Décision : la clé n'est pas une projection, c'est un cache read-through. Le lecteur peuple, le plan
de contrôle invalide.**

- `router-svc` : Bloom → `GET` Redis → **hit** : retour. **Miss** : lecture Postgres par clé primaire
  → ligne trouvée : `SET key val EX ttl` puis retour ; aucune ligne (faux positif du Bloom) :
  `(Target{}, false, nil)` → repli L1/L2, **sans** cache négatif.
- Admin API : après le commit Postgres, `DEL exactroute:{msisdn}` sur create/update/delete/import.
  Elle n'écrit **jamais une valeur** de plan de données — elle dit seulement « ce que tu crois savoir
  sur ce numéro n'est plus vrai ». La frontière plan de contrôle / plan de données tient.
- `config-sync` ne bouge pas. Le test de chaos de step-250d reste vert **sans modification**.

### Pourquoi pas le write-through depuis l'Admin API

C'était l'intuition de départ, et l'une des trois voies que la fiche proposait. Trois objections la
tuent :

1. **L'orphelin Redis n'est pas inoffensif.** Le Bloom a des faux positifs : une clé orpheline (Redis
   l'a, Postgres non) qui tombe sur un faux positif est **routée vers la cible orpheline**, alors que
   `lookup-exact-route` répond « aucune route ». Route fantôme, invisible à l'outillage admin. Ce
   n'est pas une fuite mémoire, c'est un défaut de routage indétectable.
2. **Un invariant d'existence ne couvre pas la valeur.** Sur une modification de cible, un crash entre
   les deux écritures laisse le plan de données suivre une cible que la source de vérité n'a jamais
   acceptée — **définitivement**, faute de TTL et de réconciliateur. Idem pour deux écrivains
   concurrents (UI admin + import sur le même msisdn) : l'ordre des SET Redis et celui des commits
   Postgres peuvent s'inverser, et la divergence est permanente.
3. **Redis deviendrait la seule copie routable de la cible**, sans TTL et sans rien pour la
   reconstruire — le rôle que le guide d'ingénierie §4 interdit explicitement à Redis. Un failover
   vers un réplica en retard, un `FLUSHALL` de maintenance, un resharding, et le bug d'aujourd'hui
   revient en silence, avec le Bloom qui promet toujours. S'y ajoute la cohabitation : des millions de
   clés sans TTL sur le **même Redis que les soldes de facturation et les token-buckets** — en
   `allkeys-*` Redis évince les clés de routage sans bruit sous pression mémoire (retour du bug), en
   `volatile-*` elles ne sont jamais évincées et c'est la facturation qui prend l'OOM.

Le read-through efface en outre le **rattrapage de l'existant**, que le write-through aurait dû
traiter par un outil dédié : les lignes déjà en base deviennent routables au premier message qui les
vise. Et le même mécanisme couvre perte de Redis, failover, resharding et environnement neuf.

**Écartées également.** `config-sync` projecteur : l'événement qu'il reçoit (`{"reason":"admin"}`)
n'a **aucune granularité d'entité** — créer un client, modifier une règle anti-spam ou créer une route
exacte produisent le même message indistinct. Il faudrait soit un balayage complet de la table à
chaque mutation admin de n'importe quelle nature, soit un filigrane `updated_at` structurellement
aveugle aux **suppressions** (la ligne a disparu). Et il n'a aucune connexion Postgres : lui en donner
une change la nature du binaire, de relais opaque à projection métier. — Suppression pure de Redis,
façon opt-out : le précédent ne vaut pas ici, un numéro opt-out ne reçoit par définition presque aucun
trafic, un numéro porté reçoit du trafic normal (10 à 30 % des numéros en marché MNP mûr), et cela
contredirait la spec §6.1 qui nomme la clé.

### Règle d'ordre, et le seul sens de panne restant

**La source de vérité commet d'abord, toujours ; le cache suit.** Postgres commit → `DEL`. Un crash
entre les deux laisse une clé périmée **au plus le TTL**. Reste la fenêtre classique du cache-aside
(un lecteur lit Postgres, un écrivain commit et invalide, le lecteur écrit ensuite la valeur ancienne)
— elle dure une latence de requête et le TTL la borne ; elle est nommée ici plutôt que corrigée par un
« delayed double delete » que rien ne justifie aujourd'hui.

### TTL

Constante de paquet dans `exact` (**6 h, jitter ±10 %**), passée en paramètre de `NewResolver` pour
que les tests la raccourcissent. **Pas de nouveau champ de configuration** : `config.Redis` est
strictement connexionnel (URL, Timeout), et le dépôt loge déjà ce genre de réglage en constante de
paquet (`bloomFP`, `bloomPageSize`, `defaultCoalesceWindow`) — ce qui évite au passage le triple
couplage de la garde « section lue ⇒ section déclarée ». Le TTL n'est **pas** le mécanisme de
cohérence — le `DEL` l'est — c'est le filet quand le `DEL` échoue. Le jitter évite l'expiration
synchronisée d'une rafale peuplée à froid.

Coût en régime établi : une relecture par clé primaire, par msisdn actif et par TTL, plus les faux
positifs du Bloom — `bloomFP = 0.001`, soit **~8 req/s à 8 000 SMS/s**.

### Politique d'échec

- **Lecture** : erreur Redis **ou** erreur Postgres → erreur **non codée** → offset Kafka non commité
  → redélivrance. C'est la politique §16 déjà prouvée pour Redis (step-250d), étendue à Postgres pour
  la même raison : un numéro exact injoignable ne doit pas se dégrader en routage par défaut, qui
  enverrait sur le mauvais opérateur. La ligne Postgres est ajoutée à la matrice §16 **avec son
  test** — la règle de step-260c est que rien ne s'y écrit avant d'avoir sa preuve.
- **Invalidation** : non bloquante, log + compteur — même politique que `BalanceCacheInvalidator`
  (step-148) et que le middleware `config:changed`, défendable ici parce que le TTL borne la dérive et
  que l'upsert comme le `DEL` sont idempotents. Conséquence : **aucun code d'erreur neuf, aucun `503`
  ajouté à une opération, donc aucun changement de contrat et pas de bump `api/package.json`**.

### Où vit le code

Dans `internal/routing/exact`, à côté du lecteur : le `SET` de peuplement et le `DEL` d'invalidation
partagent `redisKey` et `EncodeTarget`, qui restent **non exportés**. L'Admin API ne connaît donc ni
la forme de la clé ni celle de la valeur — elle reçoit un `Invalidate(ctx, msisdns ...string)` derrière
une interface locale, sur le patron de `BalanceCacheInvalidator`. C'est exactement la propriété que
réclame `TestExactRouteRedisEncodingIsPinned` : « deux composants qui s'accordent sur un format que
rien n'ancre, c'est ainsi qu'ils dérivent en silence ».

### Suites ouvertes (à mesurer avant de décider)

La métrique `exact_route_lookup_total{outcome}` (`bloom_miss` · `redis_hit` · `pg_hit` · `pg_miss`)
est livrée ici pour les rendre décidables :

- **cache négatif** pour les faux positifs du Bloom — un numéro non porté à fort trafic qui tombe en
  faux positif frappe Postgres à chaque message, de façon déterministe jusqu'au prochain rebuild ;
- **réglage du TTL**, et `singleflight` par msisdn si la pointe à froid après une perte de Redis le
  justifie ;
- **profil L0 du banc de charge** : `test/load/` ne sème aujourd'hui aucune route exacte. Laissé à
  step-280, qui possède son profil et sa fidélité.
