# ADR-0013 : `cancelled` signifie « jamais parti » — arbitrage par jeton à vainqueur unique

**Status:** Accepted
**Date:** 2026-08-07
**Deciders:** Équipe plateforme
**Réf spec:** §6.22 ; step-209 ; ADR-0009 (dont cet ADR ferme la conséquence « Course intrinsèque »)
**Réf code:** `internal/cancel`, `internal/connectorpool/connectorpool.go`, `internal/outcome`

## Context

ADR-0009 a livré l'annulation avec une course explicitement assumée, et un corollaire « à revoir en
M4 » qui ne l'a jamais été :

> **Course intrinsèque (hors périmètre)** — une annulation concurrente d'un `submit_sm` déjà parti
> reste possible ; le CDR enregistre alors `cancelled` (rang 60) bien que le message ait quitté la
> file. Corollaire à revoir en **M4** : `cancelled` (rang 60) supersède `delivered`/`failed` (40/50),
> donc un DLR terminal arrivant après une annulation en course serait masqué.

L'annulabilité se décide sur le **statut lu dans la projection CDR** (`internal/cancel/cancel.go`) :
`accepted` ⇒ autorisée. Or `cdr` est un `ReplacingMergeTree(version)` dont la version est le rang du
statut, et `cancelled` vaut 60 — au-dessus de `delivered` (40) et `failed` (50). Le rang max gagne
**quel que soit l'ordre d'insertion**. Une ligne `cancelled` écrite par erreur est donc définitive.

**step-201c a élargi la fenêtre d'un facteur ~1000.** L'`enroute` n'est plus écrit synchrones par le
connecteur : il est publié sur `mt.outcome` et projeté par un consommateur dédié
(`internal/outcome`). Un message déjà sur le fil lit `accepted` pendant toute la durée de ce lag —
quelques dizaines de ms en régime normal, mais borné seulement par l'alerte de lag à **30 s** sous
saturation ClickHouse. Dans cette fenêtre, un `cancel_sm` est **accepté** pour un message qui sera
livré, et le rang 60 enterre les `enroute` et `delivered` qui suivent.

`GET /messages/{id}` répond alors `cancelled` **pour toujours** sur un message livré à l'abonné et
facturé. Le contrat public en était réduit à documenter la limite dans la description de
`MessageStatus`.

L'argent n'est pas touché : la facturation suit le grand livre réserve/capture, idempotent par
`message_id`, qui ne lit jamais ce statut.

## Decision

### 1. `cancelled` signifie « le message n'est jamais parti »

Ce n'est pas une décision neuve : §6.22 dit « annule un message **pas encore envoyé** au SMSC ; s'il a
déjà été soumis, `ESME_RCANCELFAIL` », et ADR-0009 parle du « mécanisme "pas encore envoyé" ». On
l'inscrit ici parce que step-209 a montré que le code ne l'honorait pas.

Conséquence directe : écrire `cancelled` sur un message dispatché est **faux**, pas seulement mal
classé. Toute solution qui se contente de reclasser la ligne (la démoter sous `delivered`, ajouter un
état provisoire, résoudre le statut à la lecture) laisse une donnée fausse dans `cdr` — donc fausse
pour toute l'analytique qui le lit. La ligne ne doit pas être écrite.

### 2. La clé `cancel:{message_id}` devient un jeton à vainqueur unique

La décision quitte la projection retardée pour la seule ressource que les deux processus partagent
déjà, et de façon atomique. `SET … NX GET` en une commande, une seule clé (le `message_id` reste le
hash tag Cluster) :

| Appelant | Commande | Ancienne valeur | Décision |
|---|---|---|---|
| Connecteur, avant `submit_sm` | `SET cancel:{id} "dispatched" NX GET EX 5min` | absente | jeton pris → **envoie** |
| | | `"dispatched"` | sa propre reprise après crash → **envoie** |
| | | `"cancel"` | annulé → écrit `cancelled`, **n'envoie pas** |
| Canceller (`cancel_sm`) | `SET cancel:{id} "cancel" NX GET EX 72h` | absente | gagné → ligne `cancelled`, `ESME_ROK` |
| | | `"cancel"` | répétition → `ESME_ROK`, idempotent |
| | | `"dispatched"` | **perdu → `ESME_RCANCELFAIL`, AUCUNE ligne** |

La dernière ligne est le correctif. Le reste est le comportement d'aujourd'hui, réexprimé.

### 3. Le `GET` est structurel, pas un confort

Sans lui, le connecteur ne distingue pas un jeton `cancel` de **son propre** jeton posé juste avant un
crash. Après redélivrance Kafka il conclurait « annulé » et écrirait une ligne `cancelled` sur un
message ni envoyé ni annulé — le même bug, à l'envers.

### 4. Deux TTL, deux gardes qui se recouvrent

`cancel` garde 72 h (il doit survivre au `validity_period` maximal d'un SMS). `dispatched` vit
**5 minutes**.

Le jeton n'a pas à couvrir toute la vie du message, seulement la fenêtre où la projection ment. Au-delà
de 5 minutes, `mt.outcome` a écrit `enroute` depuis longtemps — l'alerte de lag borne ce retard à 30 s,
soit 10× de marge — et la lecture CDR du Canceller refuse l'annulation avant même de toucher Redis. Les
deux gardes se composent ; l'invariant à tenir est que **le TTL du jeton dépasse le seuil de l'alerte
de lag**, ce qui est écrit dans le commentaire de la constante.

### 5. Un jeton inconnu n'est pas un jeton libre

Seul `HolderNone` — l'absence de valeur — autorise un appelant à avancer. Le Canceller refuse sur toute
autre valeur ; le connecteur honore comme une annulation tout ce qui n'est ni libre ni son propre
`HolderDispatched`.

Ce n'est pas de la défensive théorique. La clé était un simple drapeau dans le build précédent, de
valeur `"1"` et de TTL 72 h : **pendant un déploiement progressif**, un message annulé juste avant la
bascule la porte encore. La lire comme « libre » mettrait un message annulé sur le fil et ferait écrire
la ligne fautive — la régression exacte que cet ADR ferme. Le défaut va donc vers le refus, comme le
reste de la décision.

## Options Considered

### Option A : jeton à vainqueur unique sur `cancel:{message_id}` (retenue)
**Pros :** ferme la course à la source — la ligne fausse n'est jamais écrite plutôt que rendue
récupérable ; aucun nouveau statut, aucune migration ClickHouse, aucun rang modifié, donc **aucune ligne
historique rejugée** ; le chemin chaud échange une lecture Redis contre une écriture, à nombre
d'allers-retours constant ; `ESME_RCANCELFAIL` est exactement ce que §6.22 prescrit.
**Cons :** une clé Redis par message dispatché au lieu d'une par message annulé (~2,4 M clés à
8 000 SMS/s sous TTL 5 min, ~300 Mo) ; un `cancel_sm` qui répondait `ESME_ROK` en mentant répond
désormais `ESME_RCANCELFAIL` — changement observable sur la surface SMPP.

### Option B : statut provisoire `cancelling` au rang 12
Le Canceller écrirait `cancelling` sous `enroute` ; seul le connecteur écrirait le `cancelled` terminal.
**Pros :** conserve le retour visuel immédiat après un `cancel_sm`.
**Cons :** ne ferme pas la course, la rend récupérable — la ligne fausse est toujours écrite ; impose
une migration `Enum8` sur `cdr` **et** `cdr_events` ; ajoute un état visible du client (contrat public,
bump `api/package.json`, tableau de bord) ; un message jamais consommé reste `cancelling` à vie.

### Option C : résoudre le statut à la lecture depuis `cdr_events`
`get-message` lirait la timeline append-only (step-185) au lieu du rang collapsé.
**Pros :** ne touche pas le chemin d'écriture.
**Cons :** corrige l'affichage sans corriger la donnée — `cdr` reste faux, donc l'analytique reste
fausse ; touche un chemin de **lecture** partagé par `get-message`, `list-messages`, `search-messages`
et l'export, très au-delà du périmètre ; alourdit un chemin que §6.22 désigne déjà comme vecteur de
polling.

## Consequences

- **Plus facile :** le CDR redevient vrai sans règle de résolution plus riche qu'un rang scalaire ; le
  contrat public perd le paragraphe qui documentait la limite ; le multi-segment partiellement parti —
  laissé hors périmètre par ADR-0009 — se trouve couvert au passage, le jeton étant porté par le
  `message_id` que tous les segments partagent.
- **Plus difficile :** le connecteur écrit dans Redis sur le chemin chaud au lieu d'y lire, et la
  consommation mémoire Redis devient proportionnelle au débit dispatché plutôt qu'au débit annulé. Le
  TTL du jeton est le levier ; il ne peut pas descendre sous le seuil de l'alerte de lag `mt.outcome`.
- **Biais assumé :** le jeton est pris là où la lecture du flag l'était, donc avant le contrôle de
  reroute et avant l'attente AIMD. Quelques annulations légitimes sont refusées (message rerouté, ou en
  attente de throttle). Un faux négatif coûte un `ESME_RCANCELFAIL` à l'ESME et n'écrit aucune ligne
  fausse ; le faux positif inverse est le bug qu'on corrige. En cas de doute, refuser.
- **Trou résiduel documenté :** le connecteur reste **fail-open** sur erreur Redis (l'annulation est
  best-effort ; on ne fige pas toute la livraison sortante sur une panne Redis). Si son jeton échoue et
  que Redis se rétablit avant l'arrivée du `cancel_sm`, le Canceller gagne un jeton indu et la ligne
  fausse revient — borné aux pannes Redis partielles, et non poursuivi.
- **Pré-existant, hors périmètre :** `connectorpool.go` teste l'expiration **avant** l'annulation, donc
  un message annulé *et* périmé lit `expired` et jamais `cancelled`. Inchangé par cet ADR.
- **Traçabilité :** `tasks-done/step-209.md`, ADR-0009 (dont la conséquence « Course intrinsèque » est
  close par celui-ci ; le reste d'ADR-0009 — l'annulation réservée au canal SMPP — reste `Accepted` et
  n'est pas remplacé), spec §6.22.
