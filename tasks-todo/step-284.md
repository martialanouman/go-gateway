# step-284 — L'écriture durable du solde devient asynchrone, le plancher reste atomique

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-280 · **Bloque :** step-287
> Décision humaine du 29/09/2026 (goulot 3 de step-280) ; unité faute de multiple de dix libre.

## Pourquoi
Réserve **et** capture font une écriture durable synchrone (`RecordDurable` → `AdjustBalance … ON
CONFLICT DO UPDATE`, `internal/billing/billing.go:274` et `:382`) sur **une** ligne
`control_plane.balances` par client. Sur le VPS : 6-7 transactions en attente de verrou, captures au-delà
de 200 ms, 369 `submit_sm/s` pour un client — et vraisemblablement les `RESERVE_TIMEOUT` de step-285.

Décision humaine : rendre cette écriture asynchrone **sans risquer de dépassement de solde, sauf pour les
clients qui en ont le droit** (overdraft, postpayé sans plafond dur).

## Ce qui tient déjà
Le plancher est appliqué atomiquement par `reserve.lua` dans Redis, avant toute écriture durable : le
refus « solde insuffisant » ne dépend pas de Postgres. C'est ce qui rend l'asynchrone possible.

## À arbitrer (spec → Fable → humain)
- **Le piège de la réhydratation** : le cache de solde expire (10 min, `billing.go:65-69`) et se
  réhydrate depuis le solde durable, qui ne reflète aujourd'hui les réserves en cours que **parce que**
  l'écriture est synchrone. En asynchrone, une réhydratation sur un durable en retard rendrait du crédit
  déjà réservé : dépassement. Options : réhydrater durable + deltas non encore persistés ; bloquer la
  réhydratation tant que le filigrane de persistance n'a pas rattrapé ; ou ne plus faire expirer le cache
  (et borner la divergence autrement — la raison d'être du TTL, step-142b).
- **Le transport** : agrégation par client en mémoire puis écriture groupée, ou journal durable
  (`billing.events`, step-400) consommé par un écrivain unique par client. Une perte de processus ne doit
  perdre aucun débit (invariant c : idempotence par `message_id`).
- **Panne Redis** : aujourd'hui fail-closed ; ce que devient la reconstruction du cache.
- Plan §6.9/§13, ADR-0010 et le modèle du grand livre (réserve débite, capture crédite 0, libération
  rembourse) à relire : la somme du grand livre doit rester égale au solde, en différé.

## Definition of Done
- [ ] design arrêté + ADR
- [ ] invariant c vert sous double livraison, et un test de réhydratation pendant un retard de persistance
      qui échoue sur une implémentation naïve (rouge lu)
- [ ] aucun dépassement pour un client prépayé strict sous charge concurrente (test d'intégration)
- [ ] rejouer la mesure mono-client de step-280 : plus de verrou en attente dans `pg_stat_activity`

## Design arrêté
Arbitrage : spec §6.9 → Fable (option A retenue, 30/09/2026) → validation humaine du 30/09/2026. ADR-0022.

- **Synchrone, dans la tx de `RecordDurable`** : claim `billing_idempotency` + ligne du grand livre + un delta
  dans `control_plane.balance_deltas` (append-only). Invariant c inchangé ; un crash ne perd aucun débit.
- **Tri par chemin** : `RecordDurable` (seul appelant : l'Accountant — reserve, capture, release, mo_charge)
  → delta, aucun si 0 (la capture ne touche plus de ligne partagée) ; `applyEntry` (Topup, Transfer) →
  `AdjustBalance` synchrone, comme aujourd'hui. (Réarbitré par Fable : pas de switch sur `entry_type`.)
- **Lecture unifiée** : toute lecture durable (réhydratation, `Balances`, transfer, change-scope, `balance_after`)
  = `balances.credits + SUM(deltas)` en UN statement. Un delta est replié ou en attente, jamais les deux :
  la réhydratation voit toute réserve committée. Pas de filigrane, pas de blocage ; TTL 10 min conservé.
- **Replieur** : `FoldOnce` = un statement `DELETE … FOR UPDATE SKIP LOCKED RETURNING` → `INSERT INTO balances
  … GROUP BY … ORDER BY owner ON CONFLICT DO UPDATE`. Boucle 1 s, lot 5 000, une par réplique ; 40P01 → log,
  tick suivant. Métrique de retard (âge du plus vieux delta), alerte > 30 s. Autovacuum agressif sur la table.
- **Transfer / change-scope** : gardent `FOR UPDATE` sur `balances` (sérialise admin ↔ admin ↔ replieur), lisent
  la valeur unifiée ; jambes du transfer ordonnées par propriétaire. Pas de verrou consultatif : le point de
  sérialisation avec le chemin chaud est Redis, aujourd'hui comme demain.
- **`balance_after`** = la valeur que Redis vient de calculer (`reserve.lua`, `capture.lua` res[2],
  `release.lua` « released », `recordmo.lua`), portée par `cp.LedgerEntry.BalanceAfter *int` : « solde après,
  dans l'ordre des décisions de crédit ». `nil` → lecture unifiée, chemins rares seulement (cache froid,
  replay non appliqué, release sans hold). Relire l'unifié à chaque réserve sommerait tous les deltas en
  attente du client (~8 000 lignes par réserve à 8 000/s) : pire que le verrou retiré. (Réarbitrage Fable du
  30/09.) Topup/Transfer : l'unifié, relu dans la tx après `AdjustBalance`.
- **MO** : même chemin, aucun code dédié.
- **Déploiement** : billing-svc en `strategy: Recreate` — une ancienne réplique qui lit `balances` seul
  réhydraterait sans les deltas.
- **Écartés** : journal Kafka (retard non interrogeable à la réhydratation), agrégation mémoire (perte sur
  crash), cache sans TTL (perd la borne de step-142b), solde strié (reste un plafond de verrou, synchrone),
  PR préalable « capture à delta 0 » (couverte par PR2, une mesure VPS de plus).

**PR** : 1. design + ADR-0022 + §6.9 · 2. migration + sqlc + repo + `FoldOnce` + tests (ne se déploie pas sans
3) · 3. boucle + métrique + alerte + `Recreate` · 4. mesure step-280.

**Test rouge** (replieur arrêté) : topup 10 → reserve 6 → DEL du cache → reserve 6 ⇒ `ErrInsufficientCredit` ;
la lecture naïve (`balances` seul) réhydrate 10 et accepte. Puis `FoldOnce` ⇒ solde 4, deltas vides.
