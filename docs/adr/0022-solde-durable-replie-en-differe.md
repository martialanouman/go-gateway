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
   un compteur de débits par propriétaire (`billing:seq:mt:…`), incrémenté par `reserve.lua` — par où passe
   toute baisse du solde MT, transfert admin compris (amendement step-286) —, est lu avant tout le reste ; le
   `SET NX` est refusé s'il a bougé.
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
  le chemin chaud ; le vrai point de sérialisation reste Redis. La fenêtre entre leur commit et
  l'invalidation du cache est fermée par l'amendement step-286 pour le seul chemin qui baisse un solde MT.

## Amendement step-286 (04/10/2026) : une seule porte pour baisser le solde MT

Trois dépassements antérieurs à cet ADR passaient à côté de `reserve.lua`. Arbitrage Fable du 04/10/2026.

- **Toute baisse d'un solde MT passe par `reserve.lua` avant Postgres.** La source d'un transfert admin
  (`direction=mt`) est débitée dans Redis, plancher 0, sous une réservation et un champ en vol propres au
  transfert, puis la tx `Transfer` s'exécute, bornée comme l'écriture durable d'une réserve. Succès → la
  réservation et le champ sont retirés. Toute autre issue → invalidation de la source (DEL + INCR du compteur),
  puis retrait : la réhydratation est juste que la tx ait committé ou non, à la fenêtre près d'un commit qui
  aboutit côté serveur après l'expiration de la borne (même ambiguïté que l'écriture d'une réserve). Un
  transfert bloqué plus de 4 s sur un verrou échoue et se rejoue. C'est `reserve.lua`, et non la
  garde durable de `Transfer` (inchangée), qui compte désormais les réserves en vol. L'invalidation
  post-commit des deux jambes reste ; pour la source elle est redondante, et la fenêtre entre commit et
  invalidation ne peut plus la surestimer. Un rejeu d'un transfert déjà appliqué reçoit 402 au lieu de 409 si
  la source est passée sous le montant : la porte Redis précède la réclamation Postgres (aucun argent en jeu).
- **Un champ en vol par essai.** Chaque appel de `reserve.lua` et de `repair.lua` écrit un champ unique
  (`{message_id}:{nonce}`), que seul son auteur retire avant la purge des champs périmés : un essai qui a annulé son débit ne peut plus effacer
  le champ d'un nouvel essai qui a réservé le même message entre-temps.
- **Un undo ne rembourse que sa propre réservation.** Un doublon qui trouve la réservation d'un essai encore
  en vol et pas d'entrée durable répare le débit : `repair.lua`, tant que la réservation tient, pose
  atomiquement une marque `billing:repaired:{message_id}` et son propre champ en vol, avant le commit.
  L'essai d'origine, battu (`applied=false`), voit la marque, la consomme et ne rembourse pas : le débit
  durable est celui de sa réservation. Sans marque (rejeu après expiration, capture ou libération), le
  remboursement en place reste juste. Un doublon qui trouve l'entrée durable ne touche plus à Redis.
- **Résidus acceptés** (sous-estimations, bornées par le TTL du cache) : une marque non consommée quand
  l'essai d'origine confirme son succès par la relecture de l'entrée, ou quand il committe entre la lecture
  de l'entrée par le doublon et sa marque, ou encore quand il committe le premier après la marque (il ne
  passe pas par l'undo) ; une marque laissée par une réparation dont le commit échoue (elle
  ne distingue pas un échec d'un ack perdu, donc ne la retire jamais).
- **Résidu connu, non nouveau** : une réplique dont l'horloge avance peut purger le champ d'un essai quelques
  secondes avant l'expiration de sa réservation ; une réparation dans cette dérive, suivie d'une réhydratation
  avant son propre champ, surestimerait. C'est la dépendance aux horloges déjà assumée par la purge.
- **`LockBalance` crée la ligne absente** (à 0) au lieu de ne rien verrouiller : sans elle, le replieur
  l'insérait au milieu d'une tx admin et l'interblocage (40P01) suivait. Une ligne à 0 vaut une ligne absente.
- **Non corrigé, inatteignable** : une `Release` d'une réservation sans entrée durable écrirait un `release`
  orphelin, mais aucun appelant ne tient un succès de `Reserve` sans entrée durable, et le reaper part des
  réserves durables. Exiger la réserve à la libération ouvrirait une course réelle avec un commit tardif.

## Alternatives rejetées

- **Journal Kafka + écrivain unique par client** : son retard n'est pas interrogeable à la réhydratation, qui
  devrait attendre un filigrane.
- **Agrégation en mémoire** : un crash perd des débits.
- **Cache sans expiration** : perd la borne de divergence de step-142b.
- **Solde strié en N lignes** : reste synchrone et plafonné par un verrou.
