# step-282 — `mt.inbound` n'est plus partitionné par compte

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-280 · **Bloque :** step-287
> Décision humaine du 29/09/2026 (goulot 2 de step-280) ; unité faute de multiple de dix libre.

## Pourquoi
`EncodeInbound` clé `mt.inbound` par compte (`internal/pipeline/wire.go:64`, guide §4.1) : un compte est
une partition, donc une voie du routeur. Sur le VPS, un client a tout mis sur la partition 8 des 12.
L'ordre d'arrivée par compte n'est pas une exigence produit (décision humaine) ; l'ordre qui compte — les
segments UDH d'un même message sur un même bind (§7.3) — se joue sur `mt.routed`, clé par message, et n'est
pas touché.

## À arbitrer (spec → Fable → humain)
- **Clé** : `message_id` (répartition uniforme) — et ce que ça change pour le rejeu, le `mt-replay`, le
  dead-letter et toute lecture qui suppose un compte par partition (à inventorier en tête de step).
- **Équité entre campagnes** (demande humaine : « répartir les fenêtres d'envoi entre les campagnes en
  cours ») : une clé aléatoire ne la donne pas — une campagne soumise d'abord occupe toutes les partitions
  devant les autres. Faut-il un ordonnancement par compte (tour de rôle pondéré) au routeur, ou suffit-il
  de la clé aléatoire plus les token-buckets par compte existants ? Chiffrer les deux avant de choisir.
- La migration : un changement de clé en production mélange ancien et nouveau partitionnement le temps
  d'un drain — acceptable si l'ordre n'est plus exigé, à écrire.
- Guide §4.1 et spec à amender ; ADR si la clé change de nature.

## Design arrêté

- **Clé = `message_id`.** Inventaire des lecteurs qui supposeraient un compte par partition : aucun.
  L'idempotence REST se joue avant Kafka (`internal/idempotency`). Le rejeu, `mt-replay` et le
  dead-letter publient sur `mt.routed`, qui est déjà clé par message. Le débit et la réserve au routeur
  sont en Lua atomique, donc déjà sûrs sous voies concurrentes. Un message pré-segmenté par le client
  (UDHI) voyage en un seul record, et ses `submit_sm` frères portent chacun leur `message_id` : ils
  n'étaient déjà pas ordonnés sur `mt.routed`.
- **Pas d'ordonnancement au routeur.** Un tour de rôle par compte réordonne dans une partition, ce qui
  casse le commit par préfixe contigu, et c'est l'option C d'ADR-0021, écartée. L'équité entre
  catégories est portée par ADR-0021 (un topic par catégorie). Celle entre comptes d'une même catégorie
  l'est par la clé aléatoire et les seaux de compte avant l'ACK (step-283). Cela remplace l'action 7
  d'ADR-0020. La mesure qu'ADR-0021 demande (« probablement suffisant ») est faite par la campagne de
  step-287, qui dépend de cette step et de step-283. Aucun code ne se chiffre ici.
- **Migration** : aucune production (ADR-0021). Pendant un déploiement, anciens et nouveaux records se
  mélangent le temps d'un drain, ce qui est sans effet puisque l'ordre par compte n'est plus exigé.
- **Pas d'ADR neuve** : ADR-0021 nomme déjà la clé « celle de step-282 ». On amende le guide §4.1,
  la spec §3.3 et §6.8 et le plan §1.6.

## Definition of Done
- [x] design arrêté dans la fiche, guide §4.1 amendé
- [x] un test prouve qu'un seul compte remplit toutes les partitions de `mt.inbound` — rouge lu sur le code actuel
- [x] l'ordre des segments d'un même message sur `mt.routed` reste gardé (test existant vert)
