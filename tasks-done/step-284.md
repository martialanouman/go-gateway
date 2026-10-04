# step-284 — L'écriture durable du solde devient asynchrone, le plancher reste atomique

> **Jalon :** M12 · **Statut :** FAIT
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
- [x] design arrêté + ADR
- [x] invariant c vert sous double livraison, et un test de réhydratation pendant un retard de persistance
      qui échoue sur une implémentation naïve (rouge lu)
- [x] aucun dépassement pour un client prépayé strict sous charge concurrente (test d'intégration)
- [x] rejouer la mesure mono-client de step-280 : plus de verrou en attente dans `pg_stat_activity`

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

### Amendement du 30/09 — les réserves en vol à la réhydratation (PR2b)
La Task 6 (test de charge de la DoD) a montré un défaut **préexistant sur `main`** : `reserve.lua` débite Redis
**avant** que `RecordDurable` committe. Un DEL du cache (TTL, `dropBalanceCache`, invalidation admin après
un topup) fait réhydrater depuis un durable qui ignore ces débits en vol : 63 réserves de 1 acceptées pour
50 crédits sur `main`, 59-61 sur la branche. Sous charge réelle : ~débit × latence durable crédits par
réhydratation. Arbitrage Fable (deux tours : un compteur remis à 0 était faux — un décrément tardif mange
l'incrément d'une réserve postérieure) :
- `billing:inflight:mt:{owner_type}:{owner_id}`, HASH `message_id → "credits:ts_ms"`. `reserve.lua` fait le
  HSET dans le script qui débite. Go fait HDEL (`defer`, ctx détaché) sur tout chemin après `reserved`.
  **Pas** après la réparation du chemin `held` (recommandée par Fable, écartée) : sur une double livraison
  concurrente, l'essai d'origine est encore en vol, et retirer son champ avant son commit rouvrirait le
  dépassement. Un champ laissé par un crash expire à `holdTTL`.
- `rehydrate` lit le HASH **avant** le solde durable, somme les champs de moins de `holdTTL`, et fait
  `SET NX (durable − en vol)`. Un commit entre les deux lectures est soustrait deux fois : sous-estimation
  conservatrice, guérie à la réhydratation suivante. Les champs plus vieux que `holdTTL` (crash entre
  `reserve.lua` et le HDEL) sont purgés.
- L'écriture durable de la réserve, réparation `held` comprise, est bornée côté billing-svc
  (`reserveDurableTimeout`, bien sous `holdTTL`) : `RESERVE_TIMEOUT` n'a pas de plafond, et un commit plus long que l'âge de purge rouvrirait
  le trou.
- **Réhydratation périmée** (revue, rouge déterministe, présent sur `main`) : une réplique qui a lu le durable
  puis stalle peut faire son `SET NX` après qu'une autre a réchauffé le cache, débité, et que le cache a été
  supprimé : elle ressuscite le crédit. Arbitrage Fable : `billing:seq:mt:{owner_type}:{owner_id}`, compteur
  de **débits** sans TTL, incrémenté par `reserve.lua` avec le débit ; la réhydratation le lit **avant** tout le
  reste et son `SET NX` (un script) est refusé si le compteur a bougé — la réserve retente. Règle : **quiconque
  baisse le solde durable hors de `reserve.lua` incrémente le compteur** (transfert et topup admin :
  `billing.InvalidateBalanceCaches`, qui incrémente et supprime dans un script). Une version bumpée par la
  réhydratation ne suffit pas (R1 réchauffe avant la lecture de R0, puis les débits suivent : rien ne bouge).
- **Hors périmètre, step-286** (décision humaine du 30/09) : trois dépassements préexistants relevés au 2ᵉ tour
  de revue — doublon concurrent qui rembourse le cache, garde du transfert sans les réserves en vol, fenêtre
  entre le commit admin et l'invalidation.
- Hors périmètre : même course sur le compteur MO → `debts/compteur-mo-reydrate-sans-ses-debits-en-vol.md`.

**PR** : 1. design + ADR-0022 + §6.9 · 2. migration + sqlc + repo + `FoldOnce` + tests (ne se déploie pas sans
3) · 3. boucle + métrique + alerte + `Recreate` · 4. mesure step-280.

**Test rouge** (replieur arrêté) : topup 10 → reserve 6 → DEL du cache → reserve 6 ⇒ `ErrInsufficientCredit` ;
la lecture naïve (`balances` seul) réhydrate 10 et accepte. Puis `FoldOnce` ⇒ solde 4, deltas vides.

## Journal de la mesure (30/09/2026, VPS de test, image `v0.0.1-sha-cf28500ffff7`)

Protocole de step-280 : un client (`run.sh seed … 0 1`), `k6 sustained off 10m`, relevé `run.sh observe`
toutes les 10 s. **Aucun verdict.** `waiting_on_balances` est resté à 0 sur tout le run, mais ce zéro est
**creux** : le routeur était en CrashLoopBackOff (réserve en `DeadlineExceeded`, step-285), si bien que
presque aucune réserve n'a atteint billing-svc (~1 900 envois en 10 min). La DoD « plus de verrou en attente
sous la charge mono-client de step-280 » n'est pas prouvée : elle attend step-285, et se rejoue dans
step-287. ADR-0022 reste `Proposed` jusque-là.

## Journal — verdict (04/10/2026)

Le zéro du 30/09 était creux ; step-285 a rendu le routeur stable et step-285b/285c ont rejoué la charge
mono-client sur le VPS (backlog d'un client, k6 `sustained`, puis 18 échantillons de `pg_stat_activity`
espacés de 10 s sur les backends clients actifs). **Aucune attente `Lock:*` dans aucun des trois runs** :

| Run | Réserves/s | Attentes relevées (échantillons) |
|---|---|---|
| step-285b (`979cfd5`) | 294 | CPU 29 · `IO:WalSync` 4 · `Client:ClientRead` 3 |
| step-285c, référence (`83a55b5` sur `aa2744c`) | 301 | CPU 39 · `Client:ClientRead` 5 · `LWLock:BufferContent` 2 · `IO:WalSync` 2 |
| step-285c (`aa2744c`) | 951 | CPU 47 · `LWLock:WALWrite` 9 · `Client:ClientRead` 5 · `IO:WalSync` 4 |

Avant step-284 : 6-7 transactions en attente du verrou de la ligne `balances` du client. Le goulot est passé
du verrou au WAL. Le protocole diffère de step-280 (`run.sh observe` comptait les sessions en attente sur
`balances`) mais le prélèvement est plus large : toute attente de verrou, sur toute table, y serait apparue.

Tests de la DoD relancés sur `main` (`5740270`) : `TestIdempotencyInvariantC`,
`TestRehydrationSeesUnfoldedReserves` (le rouge du design), `TestStrictPrepaidNeverOverdrawsWhileFolding`,
`TestRehydrationSubtractsInFlightReserves`, `TestStaleRehydrationCannotResurrect*` : verts.

ADR-0022 passe à `Accepted`.
