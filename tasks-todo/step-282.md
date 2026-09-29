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

## Definition of Done
- [ ] design arrêté dans la fiche, guide §4.1 amendé
- [ ] un test prouve qu'un seul compte remplit toutes les partitions de `mt.inbound` — rouge lu sur le code actuel
- [ ] l'ordre des segments d'un même message sur `mt.routed` reste gardé (test existant vert)
