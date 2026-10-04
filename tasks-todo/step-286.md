# step-286 — Une seule porte pour baisser le solde MT : Redis, avant Postgres

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-284, step-285b, step-285c · **Bloque :** step-410
> Ouverte par la revue de step-284 (30/09/2026), décision humaine : une step dédiée plutôt que la PR2 de
> step-284 ; unité faute de multiple de dix libre.

## Pourquoi
Redis décide du crédit (`reserve.lua`, plancher atomique) et Postgres l'enregistre. Mais certains chemins
baissent le solde, ou remboursent le cache, **sans passer par cette porte**. La revue de step-284 a relevé
trois dépassements possibles pour un client prépayé strict, tous **antérieurs** à step-284 (ADR-0022 ne les
crée ni ne les aggrave) :

1. **Doublon concurrent → crédit fantôme.** A passe `reserve.lua` puis écrit son débit durable ; B, doublon,
   voit `held`, ne trouve pas encore l'entrée, et répare (écrit le débit). Le claim de A perd contre B →
   `applied=false` → `undoReserveCacheDebit` : `release.lua` trouve la réserve vivante et rembourse le cache.
   Le débit est durable, le cache montre `credits` de trop jusqu'au DEL suivant (≤ 10 min).
   `internal/billing/billing.go` (cas `reserved`, `!applied`) et le cas `held`. Le « rejeu inter-partition »
   que ce undo vise ne se distingue pas de ce cas.
2. **Transfert admin sans les réserves en vol.** La garde de `Transfer` lit le durable ; une réserve débitée
   dans Redis mais pas encore durable n'y est pas. Solde 100, réserve de 100 en vol, transfert de 100 :
   les deux passent, la source finit à −100. `internal/storage/postgres/billing.go` (`Transfer`).
3. **Fenêtre entre le commit admin et l'invalidation.** Après le commit d'un transfert, le cache chaud garde
   l'ancien solde jusqu'à `InvalidateBalanceCaches` (`internal/adminapi/billing.go`, `invalidate`) ; une
   invalidation qui échoue n'est qu'un Warn, et l'exposition dure jusqu'au TTL.

Et deux mineurs :
- **Bord de purge de la réparation `held`** : le champ en vol de l'essai d'origine est purgé à `holdTTL`
  (horloge de l'application) alors que la réparation peut committer jusqu'à 4 s plus tard, sans incrémenter
  `billing:seq` ; une réhydratation dans l'intervalle surestime.
- **Interblocage sur une ligne `balances` neuve** : `LockBalance` ne verrouille pas une ligne absente ; le
  replieur l'insère puis attend le verrou de l'autre jambe → 40P01, erreur admin ou repli rejoué (pas
  d'argent en jeu).

## Piste (à arbitrer spec → Fable → humain)
Toute baisse du solde MT passe d'abord par un script Redis qui débite le cache atomiquement (comme une
réserve), puis par Postgres ; le undo d'un `!applied` ne rembourse que si la réserve est bien la sienne.

## Definition of Done
- [ ] design arrêté + amendement d'ADR-0022
- [ ] un rouge déterministe par dépassement (1, 2, 3), lu avant correctif
- [ ] le test de charge de step-284 étendu aux doublons et aux transferts concurrents, sans dépassement
