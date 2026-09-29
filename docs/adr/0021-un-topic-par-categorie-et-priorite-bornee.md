# ADR-0021 : Un topic par catégorie de trafic ; l'OTP et le transactionnel passent d'abord, dans une part bornée

**Status:** Accepted
**Date:** 2026-09-29
**Deciders:** Équipe plateforme. Arbitrages utilisateur du 29/09/2026 : un topic par type de trafic, moins de
partitions pour l'OTP et le transactionnel que pour le marketing ; l'OTP et le transactionnel sont délivrés en
priorité, avec un débit limité pour garder le canal fluide.
**Réf spec:** passerelle §5.1 (pipeline MT), §6.4 (débit), §6.8 (connecteurs) ; guide §4.1 ; ADR-0012,
ADR-0014, ADR-0020 ; step-282, step-283 ; `debts/pas-de-file-prioritaire-sur-un-connecteur-partage.md`

## Context

ADR-0020 donne à chaque message une catégorie, `otp | transactional | marketing`, lue sur son sender ID. Elle
fixe le `priority_flag` envoyé au SMSC et permet de réserver des connecteurs (`priority_tier`). Elle ne règle
pas l'attente **dans** la passerelle, qui se joue à deux étages :
- **au routeur** : `mt.inbound` est une file unique. Avec step-282 (clé `message_id`), une rafale marketing
  occupe toutes les partitions devant un OTP ;
- **au pool** : `mt.routed` est une file unique, consommée par un consumer group par connecteur
  (`cmd/connector-pool-svc/wiring.go:289`). Sur un connecteur partagé, un OTP attend derrière tout le
  backlog marketing, au débit plafonné du SMSC.

Un seul consommateur sur plusieurs topics ne suffit pas. Le routeur et le pool traitent un lot entier avant de
relancer un poll : `handleBatch` attend toutes ses voies (`internal/router/router.go:122`) et
`batchHandler` tous ses shards (`internal/connectorpool/lifecycle.go:274`). Un lot qui mêle marketing et OTP
fait donc attendre l'OTP jusqu'au dernier marketing du lot.

Enfin, une priorité stricte sans borne est dangereuse :
- **la famine** : un flot d'OTP bloque le marketing indéfiniment ;
- **la fraude au trafic gonflé** (*SMS pumping*) : des bots déclenchent des OTP vers des numéros surtaxés.
  Ces messages ont l'air de vrais OTP, donc `category_mismatch` (ADR-0020) ne les voit pas, et une file
  prioritaire sans plafond accélérerait l'attaque.

## Decision

**1. Un topic par catégorie, aux deux étages.**

| Étage | Topics | Clé |
|---|---|---|
| Ingestion → routeur | `mt.inbound.otp`, `mt.inbound.transactional`, `mt.inbound.marketing` | celle de step-282 |
| Routeur → pool | `mt.routed.otp`, `mt.routed.transactional`, `mt.routed.marketing` | `message_id` (inchangée) |

`mt.inbound` et `mt.routed` disparaissent. Le nombre de partitions se règle par topic avec le levier
existant `KAFKA_TOPIC_PARTITIONS_OVERRIDES`. Le marketing garde la largeur actuelle (12) ; l'OTP et le
transactionnel en reçoivent moins (valeur de départ : 4, que step-409 mesurera). C'est possible parce que
`mt.routed` est clé par `message_id` et non par connecteur : aucune partition n'est attachée à un connecteur.

Les segments d'un même message ont la même catégorie, donc le même topic, la même clé et le même shard. L'ordre
UDH (§7.3) est inchangé.

Les topics hors du chemin chaud restent uniques : `mt.reroute-park`, `mt.dead-letter`, `mt.outcome`. Le
drainer de parking et `mt-replay` republient sur le `mt.routed.<catégorie>` que porte le record. La catégorie
est un champ de `mt.routed`, et y est déjà exigée pour la priorité effective (ADR-0020).

**2. Le choix du topic à l'ingestion est une lecture, pas une autorisation.** Le service d'ingestion choisit le
topic en lisant la catégorie du `source_addr` dans un instantané de `sender_ids`. **L'autorisation du sender ID
reste au routeur**, à sa place dans le pipeline : un expéditeur inconnu part sur `mt.inbound.marketing` et
le routeur le rejette (`ErrSenderIDNotAuthorized`, ADR-0020). Un instantané en retard envoie un message sur
une mauvaise voie, jamais au-delà d'un contrôle. Le routeur recalcule la catégorie depuis son propre instantané
et publie sur le `mt.routed.<catégorie>` qu'il a calculé.

**3. Débit par sender ID, à l'admission.** `rate_limits.entity_type` accepte `sender_id`. Le seau se vérifie
avant l'ACK, avec celui du compte (step-283) : `429` en REST, `ESME_RTHROTTLED` en SMPP. C'est l'engagement
contractuel d'un flux. Il plafonne une fraude au trafic gonflé sans toucher aux autres flux du client, et c'est
lui qui empêche un flot d'OTP de s'emparer de la part prioritaire. Un sender ID sans ligne `rate_limits`
n'a pas de plafond propre ; il reste soumis au plafond de son compte.

**4. Un consommateur par topic, au routeur comme au pool.** Chaque topic a son client Kafka et son consumer
group (`router-svc-otp`, `connector-pool-<id>-otp`…). Un lot marketing ne retient donc plus jamais un OTP.
Voies par partition, commit par préfixe et halte de voie ne changent pas. Les bornes de duplication
d'ADR-0012 et ADR-0014 (un poll par partition et par incident) valent pour chaque topic.

**5. Au pool, priorité stricte bornée par bind.** Les trois consommateurs d'un connecteur alimentent, pour
chaque bind, trois files bornées en mémoire. Un ordonnanceur par bind y puise dans l'ordre
**OTP → transactionnel → marketing**, avec une seule borne : **tant que du marketing attend, il obtient au
moins `CONNECTOR_MARKETING_MIN_SHARE` des envois du bind** (défaut 0,4). La règle ne gaspille rien : la part
que l'OTP et le transactionnel n'utilisent pas revient au marketing, et inversement. Chaque envoi consomme
toujours un jeton du seau du connecteur (le plafond technique ne change pas). Une file pleine bloque son
consommateur, et c'est la backpressure de §6.4 : les messages restent dans Kafka.

L'OTP passe avant le transactionnel sans borne entre eux. Ce qui borne l'OTP, c'est le débit par sender ID
(point 3), pas l'ordonnanceur.

**6. Ce qui ne change pas.** Le pipeline de conformité est intact (invariant b). Le `priority_flag` sortant
et `priority_tier` d'ADR-0020 restent ; `priority_tier` devient l'outil des réservations explicites
(connecteurs haut de gamme pour l'OTP), plus le seul moyen d'obtenir une priorité.

## Options Considered

### Option A : topics par catégorie, un consommateur par topic, priorité bornée par bind (retenue)
| Dimension | Évaluation |
|---|---|
| Complexité | Moyenne : 4 topics de plus, 3 consommateurs par service, un ordonnanceur par bind |
| Coût à chaud | Une lecture d'instantané à l'ingestion ; aucun appel réseau neuf |
| Isolation | Complète entre catégories aux deux étages ; bornée au bind par la part minimale |
| Garanties existantes | Voies, commit par préfixe, bornes de duplication : inchangés par topic |

**Pour :** isolation réelle aux deux étages, dimensionnement indépendant par catégorie, famine impossible par
construction, fraude plafonnée à l'entrée.
**Contre :** trois fois plus de consommateurs et de groupes à surveiller (lag, rééquilibrages) ; l'ingestion
doit charger un instantané de `sender_ids` qu'elle n'avait pas.

### Option B : un topic, plusieurs catégories, un consommateur qui pause le marketing
(`PauseFetchTopics` / `PauseFetchPartitions`.) Un lot déjà reçu reste mélangé, et la barrière de lot retient
l'OTP de ce lot. Pour pauser, il faut observer l'arrivée d'OTP, qui est justement ce qu'on ne peut pas lire sans
consommer. Écartée.

### Option C : un topic unique, ordonnancement équitable au routeur
Réordonner entre comptes ou catégories dans une partition casse le commit par préfixe contigu, sur lequel
reposent les bornes de duplication d'ADR-0012 et ADR-0014. Écartée.

### Option D : priorité stricte sans part minimale
Le marketing peut mourir de faim, et une fraude sur l'OTP passe en tête. Écartée : la part minimale est la
seule ligne qui l'empêche.

### Option E : parts fixes entre catégories (seaux séparés au connecteur)
Simple à raisonner, mais la part inutilisée est perdue : un connecteur à 60 % prioritaire sans OTP à envoyer
plafonne le marketing à 40 %. Écartée au profit de la règle d'A, qui ne gaspille rien.

## Trade-off Analysis

A achète l'isolation au prix du nombre de pièces : trois consommateurs par service au lieu d'un, et un
ordonnanceur par bind. Les alternatives moins chères (B, C) butent toutes deux sur la même propriété : le lot
et la partition sont les unités d'ordre, de commit et de duplication bornée. Toute priorité qui les partage
entre catégories casse l'une des trois. Séparer les topics laisse ces unités intactes et déplace la priorité
là où elle ne casse rien : dans le choix, par bind, du prochain `submit_sm`.

La part minimale et le débit par sender ID se complètent. La part protège le marketing contre l'OTP. Le
débit protège le canal contre un expéditeur. Sans la part, la famine devient possible ; sans le débit par
sender ID, un OTP frauduleux consomme toute la part prioritaire.

## Consequences

- **step-282 se réduit** : la question d'équité entre gros et petits clients ne se pose plus que dans
  `mt.inbound.marketing`. La clé `message_id` et les seaux de compte à l'admission (step-283) y suffisent
  probablement ; step-282 le mesure avant de conclure.
- **`debts/pas-de-file-prioritaire-sur-un-connecteur-partage.md` est payée** par la step qui implémente cet ADR.
- **L'ordre du pipeline s'écrit autrement.** Deux lectures passent avant l'ACK : le seau de débit (step-283,
  point 3) et le choix du topic. Aucune étape de conformité ne bouge, mais la ligne « ordre du pipeline » de
  `CLAUDE.md` et §5.1 doivent le dire.
- **La contrainte d'HPA du routeur change de forme.** Aujourd'hui, `maxReplicas` doit rester sous le nombre
  de partitions de `mt.inbound` (`deploy/k8s/router-svc.yaml:109`). Demain, un pod au-delà des 4 partitions OTP
  n'a pas de voie OTP, ce qui est sans conséquence, puisque la contrainte vaut pour le topic le plus large.
- **Plus dur :** trois fois plus de groupes à surveiller. Les alertes de lag doivent être par catégorie :
  un lag OTP est un incident, un lag marketing est de la backpressure.
- **Migration :** aucune production n'existe encore (environnement de test seulement). Les anciens topics sont
  supprimés au déploiement, sans drain. Si une production existait, il faudrait drainer `mt.inbound` et
  `mt.routed` avant de basculer.
- **À revoir** si une part minimale unique ne convient pas à tous les connecteurs. Il faudrait alors une
  colonne par connecteur, qu'on n'ajoute pas sans cas réel.

## Action Items

1. [ ] Contrat Admin : `sender_id` dans l'enum `entity_type` des limites de débit (bump MINEUR), déclaré
   **avant** l'implémentation. Schéma et migration : la contrainte `CHECK` de `rate_limits.entity_type`.
2. [ ] Kafka : les six topics dans `internal/storage/kafka` et `kafkaprovision`, largeurs par
   `KAFKA_TOPIC_PARTITIONS_OVERRIDES` ; retrait de `mt.inbound` et `mt.routed`.
3. [ ] Ingestion (REST, SMPP) : instantané de `sender_ids` (catégorie), choix du topic, seau `sender_id`
   avant l'ACK.
4. [ ] Routeur : un consommateur par topic ; publication sur `mt.routed.<catégorie>`.
5. [ ] Pool : un consommateur par topic et par connecteur ; ordonnanceur par bind avec
   `CONNECTOR_MARKETING_MIN_SHARE` ; drainer et `mt-replay` republient selon la catégorie du record.
6. [ ] Observabilité : lag et latence d'attente par catégorie ; alerte sur le lag OTP.
7. [ ] Spec §5.1, §6.4 ; guide §4.1 ; `CLAUDE.md` (ordre du pipeline) ; `deploy/k8s` (groupes, HPA).
8. [ ] step-282 : réduire son périmètre au topic marketing.
9. [ ] Spec du tableau de bord : limite de débit par sender ID sur l'écran des sender IDs ; lag et
   latence d'attente par catégorie au trafic temps réel (§6.3) et par connecteur ; alerte de lag OTP dans
   `alert_rules` (métrique d'infrastructure, évaluée par Alertmanager) ; état « refusé à l'admission »
   (`rate_limited`, jamais un CDR) expliqué là où l'opérateur cherche un message manquant.
10. [ ] Une fiche de step qui porte cet ADR, après step-283 et ADR-0020, et qui paie la dette de la file
   prioritaire.
