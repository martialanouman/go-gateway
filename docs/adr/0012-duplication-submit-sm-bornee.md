# ADR-0012 : La duplication d'un `submit_sm` est assumée et **bornée**, pas seulement tolérée

**Status:** Accepted
**Date:** 2026-08-02
**Deciders:** Équipe plateforme
**Réf spec:** §1.2 (durabilité) ; §6.7 (sémantique de livraison) ; step-201 (mesure) ; step-201c (mise en œuvre)

## Context

La spec autorise la duplication **par implication**, jamais explicitement. §6.7 dit :

> « L'exactement-une-fois n'est pas garanti de bout en bout (SMPP est au moins une fois) ; **remise au
> moins une fois au SMSC**, clés d'idempotence disponibles côté client. La facturation est idempotente
> par `message_id`. »

et la matrice NFR §1.2 : « Aucune perte de message après accusé de réception (remise au SMSC **au moins
une fois**) ».

« Au moins une fois » autorise logiquement deux envois. Mais le texte **ne dit jamais qu'un abonné peut
recevoir deux fois le même SMS**, ne borne pas le phénomène, et les deux mitigations qu'il nomme ne le
couvrent pas :

- les **clés d'idempotence client** (`Idempotency-Key`, fenêtre 24 h) vivent à la frontière REST
  d'ingestion : elles protègent du rejeu **du client**, pas d'une redélivrance interne de `mt.routed` ;
- l'**idempotence de facturation par `message_id`** protège l'argent, pas le combiné.

Les quatre invariants du projet portent sur le corps qui ne fuit pas, la conformité sur route exacte,
l'idempotence de facturation et `max_sessions`. **Aucun ne porte sur l'unicité de l'envoi.**

Le seul texte du dépôt qui traite frontalement « une redélivrance = un duplicata SMS » est du **code** :
`internal/connectorpool/settle/settle.go:1-9` — « a propagated error would redeliver the record and
re-submit the SMS (a duplicate) ». C'est précisément pour cela que la facturation est délibérément
fail-open, avec `billing.Reaper` (step-190) comme filet.

Deux faits ont rendu la question urgente plutôt que théorique.

**1. Le run de référence de step-201 a mesuré le goulot.** Le `connector-pool-svc` écrit le CDR **par
message** au retour du `submit_sm_resp` — quatre allers-retours ClickHouse, synchrones, avant le commit
d'offset. Il sort **192–330 `submit_sm/s`** quand l'ingestion en accepte 1 200, et aucun levier de
capacité ne le déplace (×4 sur le pool de binds achète ×1,39). La correction évidente — batcher
l'écriture par poll — **multiplierait le rayon de duplication par 10³** : un échec du `Send` fait
redélivrer tout le batch, soit jusqu'à ~1 000 records par partition, dont chacun a déjà été envoyé au
SMSC. Pire, le pire cas est corrélé au pire moment : la cause d'un échec de `Send` est la saturation
ClickHouse, celle-là même qui creuse le backlog et remplit les polls.

**2. Le rayon actuel n'est ni choisi ni connu.** Il est fixé par `FetchMaxPartitionBytes`, que le dépôt
**ne règle nulle part** : la valeur en vigueur est le défaut franz-go de 1 MiB (`kgo/config.go:676`),
soit ~1 000 records par partition à ~0,7–1 Ko le record. Personne ne l'a décidé.

Autrement dit : le système duplique déjà, dans une mesure que personne n'a choisie, sur une garantie que
la spec n'a jamais écrite.

## Decision

**La duplication d'un `submit_sm` est une propriété assumée du système, documentée et bornée.**

1. **Elle est nommée dans la spec.** §6.7 cesse de la laisser à l'implication : le texte dit qu'un
   abonné peut recevoir deux fois le même message, dans quelles circonstances, et sous quelle borne.

2. **Sa cause est réduite à une seule.** step-201c retire ClickHouse de la section critique post-envoi :
   le pool ne fait plus qu'un produce Kafka, et un consommateur dédié projette le CDR — le patron que le
   dépôt applique déjà à la ligne `accepted`. La seule chose fail-closed après le `submit_sm` devient un
   produce, dont le domaine de panne est **décorrélé** de la saturation ClickHouse. La fenêtre
   résiduelle est un crash entre le `submit_sm` et l'ack du produce.

3. **Elle est bornée, par la bonne grandeur.** Le rayon n'est pas la taille d'une écriture mais le
   **nombre de `submit_sm` effectués depuis le dernier commit d'offset**. `FetchMaxPartitionBytes` est
   exposé en `KAFKA_FETCH_MAX_PARTITION_BYTES` et posé à **56 KiB**, soit **~250 SMS dupliqués par
   partition et par crash de pod** — moins d'une demi-seconde de trafic par partition à la cible NFR de
   8 000 msg/s sur 12 partitions.

   **Ces octets sont COMPRESSÉS**, et le chiffre en dépend : `max.partition.fetch.bytes` borne des
   batches stockés, franz-go compresse en snappy par défaut, et rien ici ne le surcharge. Mesuré sur
   500 records `mt.routed` réalistes — 621 o bruts, **221 o compressés**, ratio 2,81× — d'où les 56 KiB.
   Le défaut franz-go de 1 MiB portait ~4 750 records, pas ~1 000.

   Le nombre de messages est une **estimation pour un trafic typique**, ni majorant ni minorant : des
   corps qui compressent mieux l'augmentent, de l'UCS-2 ou du binaire le réduisent. Le re-mesurer avant
   de republier le chiffre fait partie de l'engagement.

4. **Le CDR ne peut pas être sacrifié pour l'éviter.** Le fail-open sur l'écriture CDR — le réflexe
   cohérent avec `settle` — est **interdit** : `billing.Reaper` règle chaque réservation orpheline
   « against the message's recorded CDR outcome » (`reaper.go:29-32`), et `TestReaperNeverReleasesBlind`
   prouve que sans ligne CDR la réservation est **laissée intacte**. Perdre une ligne `enroute`
   bloquerait du crédit client à vie et détruirait le filet qui rend le fail-open de `settle`
   acceptable.

## Options Considered

### Option A : projection + borne explicite (retenue)
**Pros :** la correction de débit et la correction de risque sont le même geste — le batching atterrit
là où la redélivrance est inoffensive ; aucune ligne CDR n'est perdue (Kafka est le spool durable,
`cdr` est un `ReplacingMergeTree` qui dédoublonne le rejeu) ; un amplificateur de gravité disparaît
(aujourd'hui un `i/o timeout` ClickHouse n'est pas reconnu par `isLinkDrop`, fait tomber tout le cycle
de dial et **parque le pod** jusqu'à un reconfigure Admin) ; le rayon devient un chiffre choisi.
**Cons :** le statut `enroute` devient asynchrone (lag de projection) ; un topic et un consommateur de
plus ; la fenêtre résiduelle n'est pas nulle.

### Option B : batcher l'écriture CDR sur place, fail-closed
**Pros :** changement minimal, aucun composant nouveau.
**Cons :** multiplie le rayon par 10³, précisément au moment où il se déclenche. Rejetée.

### Option C : batcher sur place, fail-open sur le CDR
**Pros :** aucun SMS dupliqué ; cohérent en apparence avec le précédent `settle`.
**Cons :** casse le reaper de facturation (point 4 de la Decision) — du crédit client bloqué à vie, et
sans reaper CDR pour rattraper. Rejetée.

### Option D : garde persistante « déjà envoyé » avant chaque envoi
**Pros :** réduit la fenêtre à « crash entre le submit et le flag ».
**Cons :** garde ClickHouse en section critique, ajoute un aller-retour réseau par message là où on
cherche à en retirer, et n'a pas de bonne réponse quand le store de gardes tombe (fail-open ⇒ on
retombe sur le problème pendant la panne ; fail-closed ⇒ nouveau point d'arrêt total). Reste valable
plus tard en défense en profondeur si la borne s'avère insuffisante.

## Consequences

- **Plus facile :** le rayon de duplication est un **chiffre réglable et documenté**, plus un défaut de
  bibliothèque que personne n'a choisi. Le débit sortant cesse d'être plafonné par ClickHouse. Une
  panne d'écriture d'observabilité ne met plus un pod du plan de données hors service.
- **Plus difficile :** le statut `enroute` devient **asynchrone**. C'est une latence, pas un mensonge —
  le treillis de statuts est monotone, on montre un état antérieur *vrai* — et le contrat ne change pas
  de classe : la ligne `accepted` est **déjà** une projection asynchrone, `delivered`/`failed` arrivent
  par DLR. L'OpenAPI publique le documente comme « dernière projection, pas état temps réel », et le lag
  est rendu **alertable** au seuil de 30 s (voir ci-dessous). Aucune fraîcheur n'est promise au client.
- **Ce qui reste vrai et n'est pas résolu :** un `submit_sm` n'est transactionnel avec aucun store.
  **Aucune conception ne supprime la fenêtre**, seule sa borne est un choix. Cet ADR borne, il ne
  garantit pas l'unicité.
- **Engagement :** un opérateur peut désormais répondre « au plus ~250 par partition et par crash » à
  la question « combien d'abonnés peuvent recevoir deux fois le même SMS ? ». C'est un engagement
  vis-à-vis des opérateurs et des abonnés, et c'est ce qui justifie que cette décision soit un ADR
  ratifié et non un commentaire de code.
- **Traçabilité :** `tasks-todo/step-201c.md` (`D1`, `D2`, `D4`, `D13`, `D14`), `tasks-done/step-201.md` (la mesure
  qui a révélé le goulot), §6.7 de la spec technique et §10 du guide d'ingénierie renvoient à cet ADR.

## Surveiller le lag de statut

Le seuil de 30 s porte sur une **durée**, la mesure disponible est un **nombre de records** : l'alerte
fait la division. Expression de référence, à poser telle quelle :

```promql
queue_depth_records{queue="mt.outcome"} / rate(cdr_outcome_projected_total[5m]) > 30
# for: 2m
```

« Le backlog représente plus de 30 secondes de travail au débit de projection courant. » Les deux séries
sont exposées par `router-svc` sur son port ops.

Pourquoi cette forme, et pas une jauge d'âge en secondes : le numérateur est lu **du broker** par une
boucle indépendante du projecteur (`pollQueueDepth`), et le dénominateur appartient au projecteur. Les
trois états se lisent correctement — sain (quotient bas), saturé (le backlog monte, le quotient monte),
**mort** (`rate → 0`, quotient `+Inf`, l'alerte part). Une jauge `..._lag_seconds` posée par le
projecteur lui-même se figerait à sa dernière valeur saine dès qu'il s'arrête : elle mentirait
exactement dans le cas qu'on surveille. Topic vide et projecteur oisif donnent `0/0 = NaN`, qui ne
déclenche rien.

Le `for: 2m` couvre deux ticks de poll (5 s) plus le lissage du `rate`, pour ne pas alerter sur une
rafale de trafic absorbée en quelques secondes.

**Cette alerte n'est pas encore déployée, et sa règle ne vit pas dans ce dépôt.** Les règles Alertmanager
sont de l'infrastructure (guide §13 ; step-207 les met explicitement hors périmètre) : step-201c livre les
deux séries, câblées et testées — le lag est **alertable** — et la checklist de mise en production
(guide §15, déroulée par step-208) porte l'item qui vérifie que la règle existe avant le go-live. Dans
l'intervalle, le seuil n'est surveillé que depuis un tableau de bord.
