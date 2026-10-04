# step-400 — `billing.events` durable : de l'affichage à la détection

> **Jalon :** M11, dette découverte après coup (§15 `docs/plan-execution-passerelle.md`) · **Statut :** LIVRÉE
> **Dépend de :** step-143, step-184 · **Bloque :** l'alerting métier du tableau de bord (dépôt séparé), **avec step-401**

## But
Produire un flux **durable** d'événements de facturation, pour que le BFF du tableau de bord puisse
**détecter** une transition et pas seulement l'afficher.

## Le constat
step-184 promettait `stream-billing-alerts` avec « solde bas / plancher MO / disjoncteur ouvert ». Seul
`mo_floor_reached` a été livré : `low_balance` et le disjoncteur n'ont **aucun seuil configuré**, ni en base
ni en config. Ils ont été retirés du contrat plutôt que laissés comme une promesse creuse.

La vérification a élargi le problème. `docs/specification-technique-tableau-de-bord.md:416` :

> **Métriques de domaine métier** (`account.reputation`, `billing.mo_floor_reached`) — `evaluation_owner =
> bff`. […] Évaluées sur une **source durable** (topic Kafka `billing.events` ou pull réconciliateur depuis
> `billing-svc`) avec un **curseur/offset persisté**, de sorte qu'un redémarrage/basculement rejoue les
> transitions manquées au lieu de les perdre ; le flux WS sert l'affichage, jamais l'unique détection.

Deux conséquences :

1. **Le seuil ne nous appartient pas.** Il vit par règle dans `alert_rules.condition_json`, côté BFF —
   table absente de `db/schema_passerelle_sms.sql`. En inventer un dans la passerelle créerait un second
   lieu de vérité pour la même alerte.
2. **Mais la source durable, si — et elle n'existe pas.** Le topic `billing.events` n'est déclaré nulle part
   (`internal/storage/kafka/topics.go`). Donc `mo_floor_reached`, que nous émettons pourtant, ne peut pas
   davantage servir de détection : `metrics.stream` est best-effort par construction (producteur séparé,
   `MaxBufferedRecords(256)`, rejet plutôt que blocage — et c'est délibéré, une alerte ne doit jamais
   retarder un appel de facturation).

Ce qui manque n'est donc pas un seuil. C'est un chemin durable.

## Arbitrages à trancher (dans la fiche, avant tout code)
- **Outbox transactionnel vs producteur direct.** L'événement naît d'une transition constatée dans le cœur
  Lua/Postgres. Un producteur direct depuis `billing-svc` peut perdre l'événement si le pod meurt entre la
  transition et la production ; un outbox en base le garantit au prix d'un relais. Le curseur du BFF rejoue
  ce qui est **dans** le topic — il ne rattrape pas ce qui n'y est jamais entré.
- **Ce que porte l'événement** : la transition seule, ou l'état (solde) au moment de la transition ? Un
  consommateur qui rejoue depuis un vieil offset ne doit pas réagir à un solde périmé.
- **`mo_floor_reached` migre-t-il, ou reste-t-il dupliqué ?** Le garder sur les deux flux donne l'affichage
  immédiat (WS) et la détection fiable (topic), au prix d'une double émission à dédoublonner côté BFF.
- **Rétention du topic vs curseur du BFF** : une rétention plus courte que la fenêtre de panne tolérée
  reperd exactement ce que le curseur devait sauver.
- **Périmètre des événements** : plancher MO seul, ou aussi réserve refusée (`insufficient_credit`),
  changement de `balance_scope`, top-up ? Chaque ajout est un contrat de plus.

## Hors périmètre
Le seuil `low_balance` côté passerelle (il appartient à `alert_rules`, côté BFF). Les règles Alertmanager
pour les métriques d'infrastructure (§6.8, `evaluation_owner = alertmanager`). La visibilité des rejets du
flux temps réel → step-210.

## Le constat élargi (04/10/2026)

**`RecordMO` n'a aucun appelant en production.** `mo-dlr-router-svc` n'a pas de client de facturation
(`grep pb.NewBillingClient cmd/` : router-svc et connector-pool-svc seulement), alors que la spec lui
impose le comptage MO (`specification-technique-passerelle-sms.md` §531, §544 ; guide §94). step-143 a
livré le cœur sans le câblage. Ce que step-400 livre ne transporte donc rien en production tant que
**step-401** n'est pas mergée : step-400 ne débloque l'alerting métier du BFF qu'avec elle.

## Design arrêté

Arbitré le 04/10/2026 : spec d'abord (`specification-technique-tableau-de-bord.md:417`, seule exigence),
puis Fable, qui a tranché chaque point sans heurter la spec.

1. **Périmètre (A).** step-400 livre le chemin durable, prouvé par le gRPC `RecordMO` de billing-svc. Le
   câblage MO de `mo-dlr-router-svc` est **step-401**, une step à part : un défaut de câblage et une
   décision d'architecture ne partagent ni service ni fichier.
2. **Outbox transactionnel.** Table `control_plane.billing_events_outbox` (`id uuid DEFAULT uuidv7()` —
   c'est l'`event_id` —, `owner_type`, `owner_id`, `customer_id`, `balance_after`, `floor`, `created_at`).
   La ligne s'insère **dans la transaction de `RecordDurable`**, après la réclamation : un rejeu
   (`claimed == 0`) n'en produit jamais, un franchissement en produit exactement une. Le porteur est un
   champ `MOFloorReached *int` (le plancher) sur `cp.LedgerEntry`, posé par `RecordMO` quand `crossed`.
   Écartés : le producteur direct après commit (c'est la fenêtre de perte de `grpcserver.go:157`) ; un
   relais lisant `billing_ledger` (~250 M lignes/jour, et « franchissement » n'est pas un attribut d'une
   ligne) ; le CDC (une infrastructure neuve pour un événement rare).
3. **Relais** *(sa transaction et son verrou sont retirés par l'amendement de revue ci-dessous)*. Une goroutine de billing-svc, au rythme du replieur (1 s, `runFold`), sur chaque réplique :
   `SELECT … FOR UPDATE SKIP LOCKED LIMIT n` → `Produce` (producteur durable existant, acks=all
   idempotent, synchrone) → `DELETE` → commit. Au-moins-une-fois côté topic ; le BFF dédoublonne par
   `event_id`. Un Kafka coupé accumule des lignes, il ne touche **aucun** appel de facturation : Kafka reste
   hors de la readiness de billing-svc (`wiring.go:352`). Jauge `billing_events_outbox_lag_seconds`, âge
   de la plus vieille ligne, comme `foldLag` : un relais bloqué ne perd rien, mais il doit se voir.
4. **Contenu : transition + état.** JSON `{v:1, event_id, event:"mo_balance_floor_reached", customer_id,
   owner_type, owner_id, direction:"mo", balance_after, floor, occurred_at}`. `occurred_at` (= `created_at`)
   permet au BFF qui rejoue un vieil offset d'écarter une transition périmée ; `balance_after`/`floor`
   nourrissent la notification sans relecture.
5. **Nom et clé.** `mo_balance_floor_reached`, le nom de la spec (§835 passerelle, §357 tableau de bord) :
   le topic est un contrat neuf. Clé = `owner_type:owner_id` (celle du compteur Redis), `customer_id` en
   en-tête (`HeaderCustomerID`). Le WS garde `mo_floor_reached` : le renommer casserait un contrat publié
   → `debts/alerte-ws-mo-floor-nommee-hors-spec.md`.
6. **Dupliqué, pas migré.** Le WS reste l'affichage ; le topic est la **seule** source de détection.
   Le BFF ne crée jamais de notification depuis le WS (§417), donc rien à dédoublonner entre les deux.
7. **Rétention : l'exploitant** (ADR-0018). `billing.events` entre dans `kafkaprovision.Topics()`
   (partitions seulement) ; l'exigence — rétention ≥ fenêtre de panne tolérée du BFF, 7 jours
   recommandés — s'écrit dans la spec §3.3. Un offset hors plage est l'affaire du BFF (pull réconciliateur).
8. **Périmètre des événements : le plancher MO seul.** `insufficient_credit` est un flux, pas une
   transition ; top-up et `balance_scope` sont déjà tracés (grand livre, audit). Personne ne les consomme.
9. **Un topic de transitions, pas un journal du grand livre.** step-284 a écarté `billing.events` comme
   journal ; le nom est libre, et ce rôle-là ne doit pas renaître ici.
10. **Couplage avec step-285b.** Son `Batcher` regroupe toute entrée portant `MessageID` **et**
    `BalanceAfter` — l'entrée `mo_charge` les porte. Le lot n'écrit pas l'outbox : **une entrée dont
    `MOFloorReached != nil` prend le chemin unitaire.** Celle des deux steps qui merge en second ajoute la
    condition et son test (franchissement → une ligne d'outbox).

**Amendements de revue (04/10/2026, arbitrés par Fable) :**
- **Point 3 : le relais ne tient ni transaction ni verrou.** La revue a montré que la tx `FOR UPDATE`
  restait ouverte pendant le `Produce`, que `KAFKA_PRODUCE_TIMEOUT` ne borne pas une requête en vol
  (`internal/storage/kafka/producer.go:62-72`), et qu'un broker coupé gardait donc en permanence une des
  10 connexions du pool partagé avec le chemin chaud, et un xid qui retient l'horizon de vacuum —
  l'inverse de « Kafka coupé ne touche aucun appel de facturation ». Désormais : `SELECT … ORDER BY id
  LIMIT 100` (lecture simple) → `Produce` hors transaction → `DELETE … WHERE id = ANY(ids publiés)`. Chaque
  réplique peut publier le même événement : des doublons de plus, rares (un franchissement de plancher),
  absorbés par le dédoublonnage `event_id` déjà exigé. Une ligne insérée entre le SELECT et le DELETE
  attend la passe suivante. Écartés : borner la tx (`idle_in_transaction_session_timeout`), qui garde la
  connexion otage pendant la panne ; un bail (`claimed_until`), une colonne et un état à expirer pour des
  doublons que le consommateur absorbe déjà.
- **La passe est bornée** comme celle du replieur (10 lots). La boucle est celle de `Folder`, partagée.
- **La jauge se lit au scrape** (`prometheus.NewGaugeFunc`), pas en fin de passe : un `Produce` en vol
  n'a pas de borne sur un broker dégradé, et une jauge posée par la passe bloquée resterait figée. Elle
  rend NaN si Postgres ne répond pas, et monte aussi si la boucle n'a jamais démarré.
- **Ordre entre propriétaires non garanti** entre répliques ou après un échec partiel : `occurred_at`
  le rend lisible au consommateur.
- **Point 10 appliqué ici** : step-285b a mergé la première (#255), step-400 porte donc la garde
  (`BillingBatcher.RecordDurable` écarte une entrée dont `MOFloorReached != nil`) et son test
  (`TestBatcherKeepsTheFloorEvent`, vu rouge sans la garde : 0 ligne d'outbox au lieu de 1).

## Fichiers touchés
`db/schema_passerelle_sms.sql` + `migrations/0028_*` · `internal/controlplane/billing.go` (champ) ·
`internal/storage/postgres/billing.go` + requêtes sqlc · `internal/billing/` (`RecordMO`, relais) ·
`internal/storage/kafka/topics.go` · `kafkaprovision.Topics()` · `kafkatest` · `cmd/billing-svc` (câblage,
boucle) · spec §3.3 · plan §1.6.

## Definition of Done
- [x] franchissement du plancher → exactement une ligne d'outbox, dans la tx du `mo_charge`
  (`TestRecordDurableWritesTheFloorEventInTheSameTransaction`, `TestRecordMOFloorStopsAndAlertsOnce`)
- [x] rejeu du même `message_id` → aucune ligne de plus (même test : second `RecordDurable`, `applied=false`)
- [x] le relais publie sur `billing.events` (intégration, vrai broker) puis supprime la ligne
  (`TestEventRelayLandsTheCrossingOnBillingEvents`)
- [x] Kafka coupé : la ligne attend et la jauge monte (`TestEventRelayKeepsTheCrossingWhileKafkaIsDown`) ;
  `RecordMO` ne touche pas Kafka, par construction. La reprise après coupure n'a pas de test qui enchaîne
  les deux : chaque passe relit la file, ce que prouvent séparément le test coupé et le test au broker vivant.
- [x] mutations vues tomber (18, dont 3 refaites faute d'avoir compilé ; une survivante attendue : un
  `FOR UPDATE` hors transaction se libère seul — la régression réelle, une transaction remise autour de
  la publication, tombe) · revue en deux axes puis
  contre-revue des correctifs · coupe (boucle partagée avec `Folder`, constructeur supprimé) ·
  `make check` vert (04/10/2026)
