# step-286 — Une seule porte pour baisser le solde MT : Redis, avant Postgres

> **Jalon :** M12 · **Statut :** LIVRÉE
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

## Design arrêté
Arbitrage : spec §6.9 + ADR-0022 → Fable (04/10/2026, approuvé avec six modifications, intégrées ci-dessous).
Règle : **toute baisse d'un solde MT passe par `reserve.lua` avant Postgres** ; un undo ne rembourse le cache
que si le débit durable qui l'a battu n'est pas celui de sa propre réservation.

**D1 — Doublon concurrent (dépassement 1, et le mineur « bord de purge »).**
- Le chemin `held` de `Reserve` passe d'abord par `repair.lua` : si `billing:reservation:{id}` existe encore,
  il pose la marque `billing:repaired:{id}` (PX holdTTL) et le champ en vol `{id}:repair` = `credits:now`,
  **avant** le `ReserveEntry`. Réservation disparue → `continue` dans la boucle de 3 essais (`reserve.lua`
  rendra `reserved`), jamais une erreur.
- Puis `ReserveEntry` / `RecordDurable` comme aujourd'hui ; HDEL de `{id}:repair` en `defer` détaché. Sur
  erreur durable, la réparation relit `ReserveEntry` (ack perdu → succès), comme le chemin `reserved`.
- `release.lua` reçoit une 3ᵉ clé optionnelle, la marque : présente → la **consomme** (DEL) et rend
  `repaired`, sans rembourser ni supprimer la réservation. `undoReserveCacheDebit` la passe ; `repaired` est
  un no-op strict (pas de `dropBalanceCache`). `Release` ne la passe pas.
- Ordre : la marque précède le commit de la réparation, et l'essai d'origine n'a `applied=false` qu'après ce
  commit → il la voit toujours. La marque n'est **jamais** supprimée sur un échec de la réparation (elle ne
  distingue pas échec réel et ack perdu) : résidu = sous-estimation bornée par le TTL du cache.
- Pas d'`INCR seq` dans `repair.lua` : le champ de l'essai d'origine couvre le débit jusqu'au HSET de la
  réparation, et la réparation exige la réservation vivante.
- Écarté : comparer `created_at` ou l'âge du champ à l'heure de l'essai (un rejeu après release ou capture
  rapide passerait pour une réparation ; dépend des horloges). Écarté : invalider sur tout `!applied` (une
  réhydratation par rejeu post-capture, des milliers sur une redistribution Kafka).

**D2 — Transfert (dépassements 2 et 3).**
- `direction=mt` seulement : `(*Accountant).DebitTransfer(ctx, owner, key, credits, write)` débite la source avec
  `reserve.lua` tel quel, plancher 0 **explicite** (un transfert ne consomme jamais de découvert), réservation
  `billing:transfer-hold:{idem}`, champ en vol `transfer:{idem}`. La boucle cold/réhydratation est
  **factorisée** avec `Reserve`, pas copiée. admin-api-svc construit un `billing.Accountant` (même Redis de
  facturation, `postgres.BillingRepo`) ; interface consommateur à une méthode dans `adminapi/deps.go`.
- `write` = la tx `Transfer` (garde durable conservée). `applied=true` → DEL réservation + HDEL champ, le
  cache source reste débité. Toute autre issue (erreur, `applied=false`, ack perdu) → DEL réservation + HDEL
  champ + `InvalidateBalanceCaches(source)` : la réhydratation `durable − en vol` est juste que la tx ait
  committé ou non. Nettoyage sur `context.WithoutCancel`.
- `insufficient` → 402 avant Postgres. `held` → 409 `ErrIdempotencyConflict` **sans rien toucher** (seul
  l'essai qui a obtenu `reserved` nettoie, sinon il retirerait le champ du gagnant avant son commit) ;
  message : « idempotency_key déjà en vol » (un succès retire la réservation : un rejeu après succès passe
  par `reserved` → `applied=false` → 409 du handler).
- Le dépassement 3 disparaît : le seul chemin admin qui baisse un solde MT (la source d'un transfert) est
  débité avant le commit. Topup (≥ 1) et change-scope (soldes nuls) ne baissent rien ; MO inchangé.
  L'invalidation post-commit des deux jambes reste (destination : cache périmé bas, conservateur).
- Contrat : le 402 du transfert sort déjà (`humaerr.FromError`) mais n'est pas déclaré → déclaré dans
  `mutErrs` du transfert et `api/openapi-admin.yaml`, bump mineur de `api/package.json`.

**D3 — Interblocage sur ligne `balances` absente.** `LockBalance` devient
`INSERT … VALUES (…, 0) ON CONFLICT DO UPDATE SET credits = balances.credits` : verrouille la ligne neuve ou
existante dans l'ordre du replieur. Requête seule, aucune migration ; une ligne à 0 apparaît pour une jambe
absente (inoffensif : absent vaut déjà 0).

**Hors correctif, documenté dans l'ADR** : une `Release` de réservation sans entrée durable écrirait un
`release +c` orphelin, mais aucun appelant ne la rend atteignable (le reaper part des réserves durables) ;
exiger la réserve sur `released`/`cold` ouvrirait une vraie course.

**Rouges (lus avant correctif)** : (1) doublon orchestré — le `RecordDurable` de A bloqué jusqu'au commit de
la réparation de B — puis cache == durable − en vol ; (2) réserve de 100 en vol (`RecordDurable` bloqué),
transfert de 100 → 402 ; (3) transfert dont l'invalidation échoue, cache source chaud → une réserve du
montant transféré est refusée ; (D3) deux tx orchestrées `lockBalances(absent, Y)` contre le repli → pas de
40P01. DoD : `TestStrictPrepaidNeverOverdrawsWhileFolding` étendu aux doublons et transferts concurrents.

**PR** : a. design + amendement ADR-0022 + les rouges · b. D1 · c. D2 + contrat · d. D3 + test de charge.
Réalisé en **une PR** à commits par unité : une PR de rouges seuls casserait `main`.

### Amendement de revue (05/10/2026, Fable)
- **Champ en vol unique par appel de script** (`{id}:{nonce}`, `{id}:repair:{nonce}`, `transfer:{key}:{nonce}`) :
  le HDEL différé d'un essai annulé effaçait le champ d'un nouvel essai du même message (dépassement, rouge
  `TestUndoneAttemptKeepsItsHandsOffARetry`). Côté transfert, l'entrelacement est inatteignable (l'échec
  invalide avant son HDEL, la nouvelle tentative réhydrate en voyant encore le champ) : nonce par uniformité.
- **Chemin `held` inversé** : `ReserveEntry` d'abord ; trouvée → solde, sans toucher Redis ; absente →
  `repair.lua` (réservation absente → `continue` ; HSET du champ ; SET de la marque PX holdTTL) →
  `RecordDurable` (relecture sur erreur). Sinon un doublon après le commit, le cas courant, laissait une
  marque qui privait un rejeu post-capture de son remboursement.
- `write` de `DebitTransfer` borné à `reserveDurableTimeout` ; rejeu d'un transfert appliqué → 402 si la
  source est passée sous le montant, accepté et noté dans l'ADR.
- Une première proposition de supprimer le contrôle de la réservation dans `repair.lua` a été refusée par
  Fable (contre-exemple : l'undo de l'essai d'origine précède la réparation → cache > durable) ; testé par
  `TestRepairYieldsToAnUndoneHold`.

## Definition of Done
- [x] design arrêté + amendement d'ADR-0022
- [x] un rouge déterministe par dépassement (1, 2, 3), lu avant correctif
- [x] le test de charge de step-284 étendu aux doublons et aux transferts concurrents, sans dépassement
