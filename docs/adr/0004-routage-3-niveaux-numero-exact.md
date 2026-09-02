# ADR-0004 : Routage à 3 niveaux avec court-circuit numéro exact

**Status:** Accepted
**Date:** 2026-07-14
**Deciders:** Équipe plateforme
**Réf spec:** §6.1, §7

## Context

Le routage doit gérer la **portabilité des numéros (MNP)** : le matching par préfixe MSISDN suppose que le préfixe identifie l'opérateur, ce qui est faux pour un numéro porté. Il faut aussi permettre une logique scriptée et un matching déclaratif classique, sans payer un coût réseau pour les ~99 % de messages sans cas particulier.

## Decision

Résolution de route à **trois niveaux, premier gagnant** : **L0** correspondance de numéro exact (`exact_routes`, court-circuit) → **L1** script de routage → **L2** matching déclaratif (préfixe-trie / MCC-MNC). Le niveau L0 est adossé à un **filtre de Bloom en mémoire** (jamais de faux négatif) : « absent » = pas d'override, sans appel réseau. Le court-circuit L0 saute **uniquement la résolution de route**, jamais les étapes de conformité (E.164, sender ID, opt-out, anti-spam) ni l'aval (segmentation, débit, crédit).

## Options Considered

### Option A : 3 niveaux avec numéro exact + Bloom (retenue)
| Dimension | Évaluation |
|---|---|
| Correction MNP | Résolue |
| Coût chemin chaud | Quasi nul (Bloom) |
| Complexité | Moyenne |

**Pros :** résout la portabilité ; coût quasi nul pour les 99 % sans override (Bloom en mémoire) ; combine exactitude (numéro exact), flexibilité (script) et simplicité (déclaratif).
**Cons :** trois chemins de résolution à maintenir ; table `exact_routes` volumineuse à alimenter (import MNP).

### Option B : matching par préfixe seul
**Pros :** simple.
**Cons :** **faux** en marché porté — route un numéro porté vers le mauvais opérateur. Rédhibitoire.

### Option C : lookup base systématique par message
**Pros :** toujours exact.
**Cons :** un accès réseau/base par message au débit cible ; inacceptable en latence.

## Trade-off Analysis

Le préfixe seul (B) est incorrect ; le lookup systématique (C) est trop coûteux. La combinaison numéro-exact-avec-Bloom résout la correction **et** la performance : le Bloom transforme « chercher un override » en une vérification mémoire sans faux négatif, réservant l'accès Redis aux rares « peut-être ». La règle absolue — **le raccourci ne saute jamais la conformité** — est un invariant testable.

## Consequences

- **Plus facile :** router correctement en marché porté ; garder le chemin chaud rapide.
- **Plus difficile :** maintenir `exact_routes` (imports MNP en masse, rafraîchissement du Bloom via config-sync).
- **Garde-fou :** si la cible d'un numéro exact est indisponible, on retombe sur L1/L2 plutôt que dead-letter.

## Action Items

1. [x] `internal/routing` : Bloom en mémoire des clés `exact_routes`, rafraîchi par config-sync.
2. [x] Test d'invariant (b) : un message routé L0 traverse toutes les étapes de conformité.
3. [x] Import MNP asynchrone (`POST /admin/exact-routes/import`).

---

## Amendement du 2026-09-02 (step-250e) — `exactroute:{msisdn}` est un cache, pas une projection

**Ce que cet ADR n'avait pas tranché.** Il pose le Bloom en mémoire et « l'accès Redis aux rares
*peut-être* », et confie à `config-sync` le *rafraîchissement du Bloom*. Il ne dit nulle part **qui
écrit** la clé Redis. Aucune des fiches M7 ne l'a fiché non plus. Résultat : personne ne l'a écrite.
Pendant deux jalons, le Bloom a promis des numéros que la lecture Redis ne trouvait jamais, `Resolve`
répondait « pas d'override », et **le court-circuit L0 ne résolvait jamais** — la portabilité, seule
raison d'être de L0, ne fonctionnait pas en production.

**Décision.** La clé est un **cache read-through**, pas une projection :

- le **lecteur** (`router-svc`) la peuple — sur un miss du cache il interroge `exact_routes` par clé
  primaire et écrit la clé avec un **TTL** (6 h, jitter ±10 %) ;
- le **plan de contrôle** (Admin API) ne fait que l'**invalider** : `DEL` après son commit Postgres,
  jamais un `SET`. Il ne dit jamais quelle est la nouvelle cible ;
- `config-sync` reste le relais opaque qu'il est, et continue de déclencher le rechargement du Bloom.

**Pourquoi pas un write-through depuis l'Admin API.** Il ferait de Redis la **seule copie routable**
de la cible, sans TTL et sans rien pour la reconstruire — le rôle que le guide d'ingénierie §4
interdit explicitement à Redis. Un failover, un `FLUSHALL` ou un resharding ramènerait le défaut en
silence, avec le Bloom qui promet toujours. S'y ajoutent deux dérives permanentes : une modification
de cible interrompue laisse le plan de données suivre une cible que la source de vérité n'a jamais
acceptée, et deux écrivains concurrents peuvent inverser l'ordre des écritures Redis et des commits
Postgres. Enfin, un orphelin Redis n'est **pas** inoffensif : le Bloom ayant des faux positifs, une
clé orpheline peut être lue et routée alors que `lookup-exact-route` déclare le numéro inconnu.

**Résidu assumé.** La fenêtre classique du cache-aside subsiste : un lecteur peut lire Postgres, un
écrivain commiter et invalider, puis le lecteur écrire la valeur d'avant. Elle dure une latence de
requête et le TTL la borne. Elle est nommée ici plutôt que corrigée par un « delayed double delete »
que rien ne justifie aujourd'hui.

**Conséquences.** Plus rien à rattraper : les lignes déjà en base deviennent routables au premier
message qui les vise, et le même mécanisme couvre perte de Redis, failover, resharding et
environnement neuf. En contrepartie, le L0 a désormais **deux** jambes réseau, toutes deux
fail-closed en rejeu (matrice §16) — la voie Postgres exige de **retirer** le code d'erreur que
`postgres.translate` attache, faute de quoi le message serait enterré au lieu d'être redélivré.
