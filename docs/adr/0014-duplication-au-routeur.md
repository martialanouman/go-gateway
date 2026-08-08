# ADR-0014 : la duplication a **deux** causes, pas une — la seconde est au routeur, et elle est bornée par la même grandeur

**Status:** Accepted
**Date:** 2026-08-08
**Deciders:** Équipe plateforme
**Réf spec:** §1.2 (durabilité) ; §6.7 (sémantique de livraison) ; step-201d (mesure et correctif)
**Étend:** ADR-0012 (qui reste vrai : il traite le pool de connecteurs, et rien d'autre)
**Réf code:** `internal/router/router.go`, `internal/storage/kafka/consumer.go`

## Context

ADR-0012 a fait de la duplication d'un `submit_sm` un **engagement chiffré** plutôt qu'un commentaire de
code : « au plus ~250 messages par partition et par crash de pod », borné par
`KAFKA_FETCH_MAX_PARTITION_BYTES`. C'est cet engagement qui autorise un opérateur à répondre à la
question « combien d'abonnés peuvent recevoir deux fois le même SMS ? ».

Deux textes en ont tiré une conclusion que l'ADR ne portait pas.
`docs/specification-technique-passerelle-sms.md` §6.7 : « La cause résiduelle est **unique** : un crash
entre le `submit_sm` déjà parti sur le fil et l'accusé du produce Kafka qui enregistre son issue. » Le
guide d'ingénierie §10 dit la même chose. ADR-0012 ne parle que du **pool de connecteurs** ; ce sont la
spec et le guide qui ont généralisé à toute la passerelle.

**Il y a une seconde cause, en amont, et elle existait déjà.** `router-svc` publie `mt.routed`
**segment par segment** avant de commiter son offset (`internal/router/router.go`, boucle de fan-out) :
une interruption au milieu d'un fan-out republie à la redélivrance les segments déjà publiés, et le pool
les re-soumet. Elle n'a jamais été nommée parce qu'elle est petite — **zéro** pour du trafic
mono-segment, **≤ N−1 segments d'un seul message** en multipart.

Le **crash** du routeur, lui, n'a jamais été petit : rien n'est commité depuis le dernier poll, donc tout
le lot est rejoué et republié — **~un poll de `mt.inbound` par partition**, exactement la grandeur
qu'ADR-0012 a choisie pour le pool. Ce fait est vrai depuis M2 et n'était écrit nulle part.

step-201d a rendu la question actuelle plutôt que théorique : la mesure a établi que ~97 % du temps par
message du routeur est passé bloqué sur un `ProduceSync` acks=all émis depuis une goroutine unique, et
que la sortie sature vers 1 200 `submit_sm/s`. Le correctif est de paralléliser la boucle de
consommation — et **le choix de l'unité de parallélisme décide du rayon de duplication**.

## Decision

**Le routeur consomme par lot avec une goroutine par PARTITION, et cette unité est choisie précisément
parce qu'elle n'élargit pas le rayon.**

1. **La cause amont est nommée, et son crash est borné par la même grandeur que l'aval.** Un incident au
   routeur peut republier jusqu'à un poll de `mt.inbound` par partition ; un incident au pool peut
   re-soumettre jusqu'à un poll de `mt.routed` par partition. Une seule variable les règle toutes deux :
   `KAFKA_FETCH_MAX_PARTITION_BYTES`.

2. **La lane est la partition, et c'est ce qui laisse le rayon intact.** Une lane possède *tous* les
   records de sa partition. Quand l'un échoue, la goroutine qui s'arrête est **la seule** qui aurait pu
   toucher les records au-dessus : rien au-dessus de l'échec n'a jamais été publié. `committablePrefix`
   commite exactement le préfixe, et il n'y a rien de déjà publié à redélivrer. Le rayon sur **faute**
   reste ce qu'il était : ≤ N−1 segments d'un message.

   Sharder par **clé de compte** aurait parallélisé tout aussi bien et cassé exactement cela : les autres
   lanes auraient continué à publier au-dessus de l'échec, et leurs succès — non committables — auraient
   été rejoués. Le rayon de faute serait passé à un poll par partition, rejoignant le rayon de crash.
   C'est un prix qu'on ne paie pas avant d'avoir mesuré qu'on en a besoin.

3. **L'unité de la borne diffère entre les deux étages, et le chiffre ne se transporte pas.** Au pool, un
   record dupliqué est un `submit_sm`, donc un segment sur un combiné. Au routeur, un record dupliqué est
   un **message**, donc 1..N segments. Pour du trafic mono-segment les deux coïncident ; pour du multipart
   le rayon amont vaut N fois son compte de records.

4. **Les 56 KiB ≈ 250 records d'ADR-0012 ont été mesurés sur `mt.routed`, pas sur `mt.inbound`.** Un
   record `mt.inbound` porte le **corps entier** là où un `mt.routed` porte les octets d'un segment : le
   nombre de messages que 56 KiB compressés représentent n'est pas le même des deux côtés. ADR-0012
   s'était engagé à re-mesurer avant de republier un chiffre ; cet engagement vaut ici aussi. **Tant que
   ce n'est pas mesuré, la borne amont s'énonce en polls, pas en messages.**

5. **Ce qu'un opérateur peut répondre.** « Au plus un poll de `mt.inbound` par partition et par incident
   au routeur, plus un poll de `mt.routed` par partition et par incident au pool. » Les deux étages,
   la même variable, aucun chiffre inventé.

## Options Considered

### Option A : lane par partition (retenue)
**Pros :** rayon de duplication **inchangé** ; aucun nouveau réglage à exposer, à défaut, à borner ; le
groupe d'ordonnancement qu'exige `kafka.BatchHandler` et la partition sur laquelle `committablePrefix`
raisonne deviennent la **même chose**, au lieu de deux notions à garder cohérentes à la main.
**Cons :** le parallélisme est plafonné par le nombre de partitions assignées au pod, et il retombe à une
lane si autant de pods que de partitions rejoignent le groupe — un couplage avec l'autoscaling à traiter
en step-207. Le levier reste `KAFKA_TOPIC_PARTITIONS`.

### Option B : shard par clé de compte
**Pros :** parallélisme indépendant du nombre de partitions et du nombre de pods ; §1.6 préservé (la clé
*est* l'`account_id`).
**Cons :** porte le rayon de **faute** de « ≤ N−1 segments d'un message » à « un poll par partition », et
ouvre une boucle de redémarrage duplicante : une faute **collante** — ClickHouse indisponible sur le
chemin de rejet, un record indécodable — republie à chaque tour ce que les autres lanes ont fait avant
d'échouer, au rythme du CrashLoopBackOff. Écartée **pour l'instant** : elle reste la suite naturelle si
une courbe montre que les partitions ne suffisent pas, et elle méritera alors son propre ADR.

### Option C : arrêt anticipé des autres lanes à la première faute
Annuler un `ctx` dérivé au premier échec divise le rayon *espéré* sans changer son ordre de grandeur, et
rend les résultats non déterministes — donc les tests plus faibles. Rejetée.

### Option D : ne rien paralléliser
Conserve le rayon, conserve aussi le plafond de ~1 200 `submit_sm/s` par pod que step-201d a mesuré, très
en dessous de la cible de 8 000 soutenus. Rejetée.

## Consequences

- **Plus facile :** le débit du routeur cesse d'être un plafond à une goroutine — mesuré à ×2,8 de 1 à
  8 lanes, et le critère d'état stationnaire passe désormais à 2 400 msg/s là où il échouait. Et la
  question « combien de duplicatas ? » a maintenant une réponse aux **deux** étages.
- **Plus difficile :** le parallélisme du routeur est désormais une propriété de la **topologie Kafka**,
  pas un réglage du service. Élargir un topic est un acte d'exploitation délibéré (`make kafka-topics`),
  et un HPA qui monte à autant de pods que de partitions annule le gain.
- **Ce qui reste vrai et n'est pas résolu :** un `submit_sm` n'est transactionnel avec aucun magasin.
  Aucune conception ne supprime la fenêtre ; cet ADR, comme 0012, **borne** — il ne garantit pas
  l'unicité. Et la borne est « par incident », jamais « par unité de temps » : rien ne borne le *taux*
  d'incidents, et une faute collante reste le cas à surveiller.
- **À faire :** mesurer la taille compressée d'un record `mt.inbound` typique, sur le patron du
  commentaire de `internal/config/config.go` (`FetchMaxPartitionBytes`), pour que le point 4 porte un
  chiffre. Appartient à step-201b.
- **Traçabilité :** `tasks-done/step-201d.md` (`D11`, `D12`), ADR-0012, §6.7 de la spec technique et §10
  du guide d'ingénierie — les deux derniers corrigés par cet ADR.
