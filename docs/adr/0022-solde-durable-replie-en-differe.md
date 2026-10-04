# ADR-0022 : Le grand livre reste synchrone, le solde durable est replié en différé

**Status:** Accepted (04/10/2026 : aucune attente de verrou sous charge mono-client, step-284)
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
   journalisée). Dans la même tx, `RecordDurable` — le chemin de l'Accountant : réserve, capture, libération,
   MO — insère un delta dans `control_plane.balance_deltas`, append-only, au lieu de modifier `balances` ; un
   delta nul n'est pas inséré. Topup et Transfer, rares, restent sur `AdjustBalance`.
2. **Le solde durable est `balances.credits + SUM(balance_deltas)`**, lu en un seul statement. Un delta est
   soit en attente, soit replié, jamais les deux : toute lecture, réhydratation comprise, voit chaque
   mouvement committé. Aucun filigrane, aucun blocage ; le TTL du cache est conservé.
3. **Un replieur** par réplique de billing-svc déplace les deltas dans `balances` en un statement atomique
   (`DELETE … SKIP LOCKED RETURNING` puis upsert groupé, ordonné par propriétaire). Son retard est une
   métrique (`billing_balance_deltas_lag_seconds`, à alerter au-delà de 30 s) ; il ne conditionne aucune
   décision de crédit.
4. **`balance_after` est la valeur calculée par Redis** au moment de la décision de crédit, le seul vrai
   point de sérialisation. Relire `balances + SUM(deltas)` à chaque réserve sommerait tous les deltas en
   attente du client. Les chemins rares sans valeur Redis (cache froid, replay, release sans hold) relisent
   la valeur unifiée. `balance_after` n'est pas monotone par `created_at` entre entrées concurrentes (ce
   n'était déjà pas le cas) et peut porter la dérive du cache, bornée par son TTL ; `SUM(credits) == solde`
   reste vrai.
5. **La réhydratation soustrait les réserves en vol.** `reserve.lua` débite Redis avant le commit
   durable : réhydrater depuis le seul durable rendrait ces débits (défaut antérieur à cet ADR, révélé par
   le test de charge de step-284). Chaque réserve s'inscrit dans un HASH par propriétaire
   (`billing:inflight:mt:…`, champ `message_id`), retirée après son commit ; la réhydratation soustrait ce
   HASH, lu avant le durable. Le fail-closed de §6.9 prend la forme d'une sous-estimation transitoire, pas
   d'un blocage. Une réhydratation lente ne doit pas non plus écrire une valeur calculée avant un débit :
   un compteur de débits par propriétaire (`billing:seq:mt:…`), incrémenté par `reserve.lua` et par toute
   écriture admin qui baisse le solde, est lu avant tout le reste ; le `SET NX` est refusé s'il a bougé.
6. **Ordre de déploiement : migration, puis admin-api-svc, puis billing-svc en `Recreate`.** Une version
   antérieure lit `balances` sans les deltas. admin-api-svc (transfert, change-scope, soldes) doit donc
   savoir les lire avant que billing-svc commence à en écrire, et billing-svc ne doit jamais mêler les deux
   versions. Prix : chaque déploiement coupe billing-svc entièrement (le PDB ne protège que des évictions).
   Pendant la coupure le routeur ne committe pas ses offsets : les messages facturés attendent, sans rejet
   ni envoi non facturé, et repartent au retour — un backlog, celui que step-285 traite. Seule la transition
   vers les deltas l'exige : `debts/billing-svc-reste-en-recreate-apres-la-transition.md`.

## Consequences

- Plus aucune sérialisation par client sur le chemin chaud ; il reste un aller-retour Postgres par réserve,
  borné par le pool. Si `RESERVE_TIMEOUT` (step-285) venait de la latence et non du verrou, cet ADR ne le lève pas.
- Aucun client ne peut dépasser, prépayé strict compris : pas de régime distinct pour l'overdraft.
- Une table à forte rotation : autovacuum réglé par table dans la migration.
- Le Redis de facturation doit tourner en `maxmemory-policy noeviction` (le défaut de Redis) : une éviction du
  compteur de débits ou du HASH des réserves en vol rouvrirait le dépassement que ces clés ferment.
- Transfer et change-scope verrouillent `balances` dans l'ordre du replieur, puis lisent le solde dans un
  statement **suivant** : sous READ COMMITTED, un statement qui attend ce verrou relit la version récente de
  la ligne mais garde son instantané des deltas, et compterait deux fois un delta tout juste replié.
- Inchangé : transfer et change-scope se sérialisent sur `balances` entre eux et avec le replieur, pas avec
  le chemin chaud ; le vrai point de sérialisation reste Redis, et la fenêtre entre leur commit et
  l'invalidation du cache préexiste.

## Alternatives rejetées

- **Journal Kafka + écrivain unique par client** : son retard n'est pas interrogeable à la réhydratation, qui
  devrait attendre un filigrane.
- **Agrégation en mémoire** : un crash perd des débits.
- **Cache sans expiration** : perd la borne de divergence de step-142b.
- **Solde strié en N lignes** : reste synchrone et plafonné par un verrou.
