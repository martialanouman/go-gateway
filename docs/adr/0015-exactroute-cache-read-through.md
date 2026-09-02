# ADR-0015 : `exactroute:{msisdn}` est un cache read-through, pas une projection

**Status:** Accepted
**Date:** 2026-09-02
**Deciders:** Équipe plateforme
**Réf spec:** §6.1, Appendix B · **Étend:** [ADR-0004](0004-routage-3-niveaux-numero-exact.md) (non remplacé)
**Réf code:** `internal/routing/exact` (resolver, invalidator) · `internal/adminapi/exact_routes.go`

## Context

ADR-0004 pose le routage à trois niveaux et adosse le court-circuit L0 à un filtre de Bloom en
mémoire, « réservant l'accès Redis aux rares *peut-être* ». Il confie à `config-sync` le
*rafraîchissement du Bloom*. Il ne dit **nulle part qui écrit** la clé `exactroute:{msisdn}` — et
aucune des sept fiches du jalon M7 ne l'a fiché non plus : step-101 nomme la clé dans son titre et ne
définit que son lecteur, step-100/102/103 s'arrêtent à Postgres, step-105/106 traitent de la
notification et du Bloom.

Résultat : **personne ne l'a écrite**. Pendant deux jalons, le Bloom a promis des numéros que la
lecture Redis ne trouvait jamais, `Resolve` répondait « pas d'override », et le message retombait sur
le matching par préfixe. Le court-circuit L0 ne résolvait donc jamais, et **la portabilité des numéros
— seule raison d'être de L0 — ne fonctionnait pas en production**. Trois commentaires du code
affirmaient entre-temps que `config-sync` écrivait cette clé ; ils décrivaient un mécanisme plausible
qui n'a jamais existé.

## Decision

La clé est un **cache read-through** sur `control_plane.exact_routes`, jamais une projection :

- le **lecteur** (`router-svc`) la peuple : sur un miss du cache il lit la table par clé primaire et
  écrit la clé avec un **TTL** (6 h, jitter ±10 %) ;
- le **plan de contrôle** (Admin API) ne fait que l'**invalider** — `DEL` après son commit Postgres,
  jamais un `SET`. Il ne dit jamais quelle est la nouvelle cible ;
- l'import de masse, dont le commit survient **après** sa réponse 202, **annonce lui-même** son
  changement de configuration après commit — le middleware générique publie au retour du handler,
  donc trop tôt pour lui ;
- `config-sync` reste le relais opaque qu'il est, et continue de déclencher le rechargement du Bloom.

## Options Considered

### Option A : cache read-through, invalidé par le plan de contrôle (retenue)
**Pros :** Redis reste reconstructible à tout instant depuis la source de vérité ; **rien à
rattraper**, les lignes déjà en base deviennent routables au premier message qui les vise ; le même
mécanisme couvre perte de Redis, failover, resharding et environnement neuf ; la frontière plan de
contrôle / plan de données tient (l'Admin n'écrit aucune valeur de routage).
**Cons :** le L0 gagne une seconde jambe réseau, qu'il faut borner et rendre fail-closed ; la fenêtre
classique du cache-aside subsiste.

### Option B : write-through depuis l'Admin API
**Pros :** immédiat et précis — le handler est le seul à savoir quel MSISDN a changé.
**Cons, rédhibitoires :** Redis deviendrait la **seule copie routable** de la cible, sans TTL et sans
rien pour la reconstruire — le rôle que le guide d'ingénierie §4 interdit explicitement à Redis. Un
failover, un `FLUSHALL` ou un resharding ramènerait le défaut en silence, avec le Bloom qui promet
toujours. S'y ajoutent deux dérives **permanentes** : une modification de cible interrompue laisse le
plan de données suivre une cible que la source de vérité n'a jamais acceptée, et deux écrivains
concurrents peuvent inverser l'ordre des écritures Redis et des commits Postgres. Enfin l'orphelin
n'est **pas** inoffensif : le Bloom ayant des faux positifs, une clé orpheline peut être lue et
routée alors que `lookup-exact-route` déclare le numéro inconnu. Et des millions de clés sans TTL
cohabiteraient avec les soldes de facturation : en `allkeys-*` Redis évincerait le routage sans
bruit, en `volatile-*` la facturation prendrait l'OOM.

### Option C : `config-sync` projette Postgres → Redis
**Cons :** l'événement qu'il reçoit n'a **aucune granularité d'entité** — créer un client produit le
même message qu'une route exacte. Il faudrait soit un balayage complet de la table à chaque mutation
admin de n'importe quelle nature, soit un filigrane `updated_at` structurellement aveugle aux
**suppressions**. Et il n'a aucune connexion Postgres : lui en donner une change la nature du binaire,
de relais opaque à projection métier.

### Option D : supprimer Redis du L0, confirmer le Bloom en Postgres
Le précédent existe — l'opt-out (§6.20) a la même architecture annoncée et confirme en base.
**Cons :** le profil de lecture n'a rien à voir. Un numéro opt-out ne reçoit par définition presque
aucun trafic ; un numéro porté reçoit du trafic normal, et 10 à 30 % des numéros le sont en marché MNP
mûr. Cela mettrait des milliers de requêtes ponctuelles par seconde sur la base du plan de contrôle,
sur le chemin critique MT. C'est l'option C d'ADR-0004, déjà écartée pour cette raison.

## Trade-off Analysis

B est la voie évidente et la mauvaise : elle donne à Redis un rôle d'autorité que l'architecture lui
refuse, et ses trois modes de dérive sont **permanents** faute de TTL et de réconciliateur. A obtient
le même résultat fonctionnel avec une propriété que B n'a pas — **le cache est toujours
reconstructible** — et supprime au passage le besoin d'un outil de rattrapage pour l'existant.

**Règle d'ordre :** la source de vérité commet d'abord, toujours ; le cache suit. Un crash entre les
deux laisse une clé périmée **au plus le TTL**.

**Résidu assumé :** la fenêtre classique du cache-aside (un lecteur lit Postgres, un écrivain commite
et invalide, le lecteur écrit ensuite la valeur ancienne). Elle s'ouvre en une latence de requête, et
sa *conséquence* dure jusqu'à un TTL. Elle est nommée ici plutôt que corrigée par un « delayed double
delete » que rien ne justifie aujourd'hui.

## Consequences

- **Plus facile :** aucun rattrapage de l'existant ; aucun réconciliateur ; le cache survit à toute
  perte de Redis.
- **Plus difficile :** le L0 a désormais **deux** jambes réseau, toutes deux fail-closed en rejeu
  (matrice §16). La voie Postgres exige de **retirer** le code d'erreur que `postgres.translate`
  attache — sinon `router.handle` enterrerait le message (CDR `rejected`, offset commité) au lieu de
  le redélivrer — et une **échéance** propre, sans laquelle un Postgres qui absorbe sans répondre
  fige la lane, donc la partition.
- **Garde-fou :** une valeur de cache illisible est traitée comme un miss et guérie depuis la table,
  jamais remontée en erreur : la remonter renverrait le message sur la même clé à chaque redélivrance.
- **Mesure :** `exact_route_lookups_total{outcome}` (`bloom_miss` · `redis_hit` · `cache_corrupt` ·
  `pg_hit` · `pg_miss`) rend décidables les deux suites différées — le cache négatif et le réglage du
  TTL.

## Action Items

1. [x] Read-through + TTL jitteré dans `internal/routing/exact` ; invalidation par l'Admin API.
2. [x] Politique de panne PostgreSQL du L0 écrite en §16 **avec** son test de chaos.
3. [ ] Cache négatif pour les faux positifs du Bloom — à décider sur `pg_miss`.
4. [ ] Réglage du TTL — à décider sur `pg_hit`.
