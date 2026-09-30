# ADR-0022 : Le grand livre reste synchrone, le solde durable est replié en différé

**Status:** Proposed
**Date:** 2026-09-30
**Deciders:** Équipe plateforme. Décision utilisateur du 29/09/2026 (goulot 3 de step-280) : rendre l'écriture
durable du solde asynchrone sans dépassement ; design validé le 30/09/2026.
**Réf spec:** passerelle §6.9 ; ADR-0002, ADR-0010 ; step-142b, step-280, step-284

## Context

Réserve et capture passaient par `AdjustBalance` (`INSERT … ON CONFLICT DO UPDATE`) sur **une** ligne
`control_plane.balances` par propriétaire, capture comprise à delta 0. Le verrou de ligne est tenu jusqu'au
commit : sur le VPS, 6-7 tx en attente et 369 `submit_sm/s` pour un client.

Le plancher ne dépend pas de Postgres : `reserve.lua` l'applique atomiquement dans Redis. Mais le cache expire
(10 min, step-142b) et se réhydrate depuis le solde durable, qui ne reflète les réserves en cours que parce que
l'écriture est synchrone. Une réhydratation sur un durable en retard rendrait du crédit déjà réservé.

## Decision

1. **La réclamation d'idempotence et le grand livre restent synchrones** (§6.9 : chaque réserve est
   journalisée). Dans la même tx, le mouvement insère un delta dans `control_plane.balance_deltas`,
   append-only, au lieu de modifier `balances`. Types concernés : `reserve`, `capture`, `release`, `refund`,
   `mo_charge` ; un delta nul n'est pas inséré. `topup`, `adjustment` et `transfer`, rares, restent sur
   `AdjustBalance`.
2. **Le solde durable est `balances.credits + SUM(balance_deltas)`**, lu en un seul statement. Un delta est
   soit en attente, soit replié, jamais les deux : toute lecture, réhydratation comprise, voit chaque
   mouvement committé. Aucun filigrane, aucun blocage ; le TTL du cache est conservé.
3. **Un replieur** par réplique de billing-svc déplace les deltas dans `balances` en un statement atomique
   (`DELETE … SKIP LOCKED RETURNING` puis upsert groupé, ordonné par propriétaire). Son retard est une
   métrique alertée ; il ne conditionne aucune décision de crédit.
4. **`balance_after` est la valeur calculée par Redis** au moment de la décision de crédit, le seul vrai
   point de sérialisation. Relire `balances + SUM(deltas)` à chaque réserve sommerait tous les deltas en
   attente du client. Les chemins rares sans valeur Redis (cache froid, replay, release sans hold) relisent
   la valeur unifiée. `balance_after` n'est pas monotone par `created_at` entre entrées concurrentes (ce
   n'était déjà pas le cas) et peut porter la dérive du cache, bornée par son TTL ; `SUM(credits) == solde`
   reste vrai.
5. billing-svc se déploie en `Recreate` : une réplique antérieure lirait `balances` sans les deltas.

## Consequences

- Plus aucune sérialisation par client sur le chemin chaud ; il reste un aller-retour Postgres par réserve,
  borné par le pool. Si `RESERVE_TIMEOUT` (step-285) venait de la latence et non du verrou, cet ADR ne le lève pas.
- Aucun client ne peut dépasser, prépayé strict compris : pas de régime distinct pour l'overdraft.
- Une table à forte rotation : autovacuum réglé par table dans la migration.
- Inchangé : transfer et change-scope se sérialisent sur `balances` entre eux et avec le replieur, pas avec
  le chemin chaud ; le vrai point de sérialisation reste Redis, et la fenêtre entre leur commit et
  l'invalidation du cache préexiste.

## Alternatives rejetées

- **Journal Kafka + écrivain unique par client** : son retard n'est pas interrogeable à la réhydratation, qui
  devrait attendre un filigrane.
- **Agrégation en mémoire** : un crash perd des débits.
- **Cache sans expiration** : perd la borne de divergence de step-142b.
- **Solde strié en N lignes** : reste synchrone et plafonné par un verrou.
