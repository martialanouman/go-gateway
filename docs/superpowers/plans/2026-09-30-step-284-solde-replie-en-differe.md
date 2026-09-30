# step-284 — Solde durable replié en différé : plan d'implémentation

> **Pour l'exécutant :** TDD strict (rouge lu → vert → mutation vue tomber). Cases `- [ ]` à cocher.

**Goal :** retirer le verrou de ligne `control_plane.balances` du chemin chaud de facturation sans qu'une
réhydratation puisse rendre du crédit déjà réservé.

**Architecture :** `RecordDurable` insère un delta dans `balance_deltas` (append-only) au lieu d'`AdjustBalance` ;
toute lecture durable = `balances + SUM(deltas)` en un statement ; un replieur déplace les deltas dans
`balances`. `balance_after` vient de Redis sur le chemin chaud.

**Spec :** `tasks-todo/step-284.md` § Design arrêté · `docs/adr/0022-solde-durable-replie-en-differe.md`

## Contraintes globales

- Schéma : `db/schema_passerelle_sms.sql` **et** `migrations/0024_*.{up,down}.sql` ensemble ; `make generate` (sqlc).
- Tables qualifiées `control_plane.` dans les requêtes (sqlc v1.30).
- Tests d'intégration : `pgtest.Pool(t)` / `redistest.Client(t)` ; UUID frais par test (jamais de littéral).
- Zéro commentaire évident ; une ligne de doc par symbole exporté (`revive`).
- Ne jamais lancer de mutations en parallèle de `make check`.

## Fichiers

| Fichier | Rôle |
|---|---|
| `db/schema_passerelle_sms.sql` §22 | table `balance_deltas`, `balances` devient la projection repliée |
| `migrations/0024_balance_deltas.{up,down}.sql` | idem, autovacuum par table |
| `internal/storage/postgres/queries/billing.sql` | `InsertBalanceDelta`, `GetBalance`/`GetBalanceForUpdate` unifiés, `AdjustBalance` rend l'unifié, `FoldBalanceDeltas`, `OldestBalanceDelta` |
| `internal/storage/postgres/billing.go` | `RecordDurable` → delta ; `Balance`, `Transfer`, `ChangeBalanceScope` unifiés ; `FoldOnce`, `OldestPendingDelta` |
| `internal/controlplane/billing.go` | `LedgerEntry.BalanceAfter *int` |
| `internal/billing/billing.go` | passe la valeur Redis ; commentaires 1-12, 66-71 réécrits |
| `cmd/billing-svc/{main,wiring}.go` | boucle `runFold`, métriques (PR3) |
| `deploy/k8s/billing-svc.yaml` | `strategy: Recreate` (PR3) |

---

## PR2 — le durable différé

### Task 1 : table `balance_deltas` et requêtes

**Files :** schéma §22, `migrations/0024_balance_deltas.{up,down}.sql`, `queries/billing.sql`, `sqlcgen/` (généré).

- [ ] **Schéma + migration up** (texte identique dans les deux) :

```sql
CREATE TABLE control_plane.balance_deltas (
  id         uuid NOT NULL DEFAULT uuidv7() PRIMARY KEY,
  owner_type text NOT NULL CHECK (owner_type IN ('customer','smpp_account')),
  owner_id   uuid NOT NULL,
  direction  text NOT NULL CHECK (direction IN ('mt','mo')),
  credits    integer NOT NULL CHECK (credits <> 0),
  created_at timestamptz NOT NULL DEFAULT now()
) WITH (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000);
CREATE INDEX balance_deltas_owner_idx ON control_plane.balance_deltas(owner_type, owner_id, direction);
```

  Down : `DROP TABLE control_plane.balance_deltas;` — **destructif si des deltas sont en attente** : le down
  doit d'abord replier (`INSERT … ON CONFLICT` depuis un `DELETE … RETURNING` de toute la table, même forme que
  `FoldBalanceDeltas` sans `LIMIT`), puis dropper.

- [ ] **Requêtes** :

```sql
-- name: InsertBalanceDelta :exec
INSERT INTO control_plane.balance_deltas (owner_type, owner_id, direction, credits)
VALUES (@owner_type, @owner_id, @direction, @credits);

-- name: GetBalance :one
SELECT (SELECT b.credits FROM control_plane.balances b
         WHERE b.owner_type = @owner_type AND b.owner_id = @owner_id AND b.direction = @direction) AS folded,
       (SELECT sum(d.credits) FROM control_plane.balance_deltas d
         WHERE d.owner_type = @owner_type AND d.owner_id = @owner_id AND d.direction = @direction)::bigint AS pending;

-- name: GetBalanceForUpdate :one
-- idem, avec FOR UPDATE dans la sous-requête sur balances ; jamais ErrNoRows.

-- name: FoldBalanceDeltas :one
WITH moved AS (
  DELETE FROM control_plane.balance_deltas
  WHERE id IN (SELECT id FROM control_plane.balance_deltas ORDER BY id LIMIT @lim FOR UPDATE SKIP LOCKED)
  RETURNING owner_type, owner_id, direction, credits
), upserted AS (
  INSERT INTO control_plane.balances (owner_type, owner_id, direction, credits)
  SELECT owner_type, owner_id, direction, sum(credits)::int FROM moved
  GROUP BY owner_type, owner_id, direction
  ORDER BY owner_type, owner_id, direction
  ON CONFLICT (owner_type, owner_id, direction)
  DO UPDATE SET credits = control_plane.balances.credits + excluded.credits, updated_at = now()
  RETURNING 1
)
SELECT count(*) FROM moved;

-- name: OldestBalanceDelta :one
SELECT created_at FROM control_plane.balance_deltas ORDER BY id LIMIT 1;
```

  `AdjustBalance` : `RETURNING credits + COALESCE((SELECT sum(credits) FROM control_plane.balance_deltas d WHERE …), 0)`
  pour que `balance_after` de Topup/Transfer soit l'unifié.

- [ ] `make generate` ; `go build ./...` ; commit `feat(billing): table balance_deltas (step-284)`.

### Task 2 : `RecordDurable` écrit un delta, `Balance` lit l'unifié — le rouge de la fiche

**Files :** `internal/storage/postgres/billing.go`, `internal/controlplane/billing.go`,
test `internal/billing/billing_integration_test.go` (+ `internal/storage/postgres/billing_integration_test.go`).

- [ ] **Test repo (rouge 1)** — `TestRecordDurableLeavesBalancesRowUntouched` : topup 100 via `Topup`, puis
  `RecordDurable(reserve, -3)` ; assert : ligne `balances` toujours à 100 (SQL direct), une ligne dans
  `balance_deltas` à −3, `Balance` = 97, capture à 0 → **aucune** ligne de delta ajoutée. Rouge attendu :
  `balances = 97` (AdjustBalance synchrone).
- [ ] **Implémentation** : dans `RecordDurable`, remplacer `AdjustBalance` par `InsertBalanceDelta` (si
  `Credits != 0`) ; `balance_after` = `*entry.BalanceAfter` si non nil, sinon `r.balanceTx(ctx, qtx, …)` (lecture
  unifiée dans la tx, voit son propre insert). Le chemin `claimed == 0` lit aussi l'unifié. `Balance` :
  `found = folded != nil || pending != nil`, valeur = somme.
  Ajouter `BalanceAfter *int` à `cp.LedgerEntry` (doc : « solde après, vu par Redis ; nil → relu »).
- [ ] **Test Accountant (rouge 2, exigé par la DoD)** — `TestRehydrationSeesUnfoldedReserves` : harnais à 10
  crédits, replieur jamais appelé ; `Reserve(6)` OK ; `dropCachedBalance` ; `Reserve(6)` sur un autre
  message ⇒ `errs.ErrInsufficientCredit`. Le lire rouge avec `Balance` **naïf** (`folded` seul) — étape
  intermédiaire assumée : RecordDurable déjà en deltas, `Balance` pas encore unifié. Puis unifier ⇒ vert.
- [ ] Mettre à jour `TestBillingRepoRecordAndIdempotency` si une assertion lit la ligne `balances`.
- [ ] Mutation : repasser `GetBalance` sur `folded` seul ⇒ rouge 2 retombe. Restaurer (cp avant).
- [ ] Commit `feat(billing): la réserve n'écrit plus la ligne de solde (step-284)`.

### Task 3 : `balance_after` porté depuis Redis

**Files :** `internal/billing/billing.go`, test `internal/billing/billing_integration_test.go`.

- [ ] **Test** — `TestLedgerBalanceAfterIsTheCreditDecision` : harnais 100 ; `Reserve(3)` rend 97 ; la ligne
  `reserve` du grand livre porte `balance_after = 97`. Puis un delta étranger injecté en SQL (−5, même
  propriétaire, sans passer par Redis) ; `Reserve(2)` : `balance_after = 95` (Redis), **pas** 90 (unifié).
  Rouge : 90 (Task 2 relit l'unifié).
- [ ] **Implémentation** : `a.entry(...)` prend un `balanceAfter *int`. Reserve « reserved » → `&newBalance`
  (res[1]) ; Capture « captured » → res[2] si non vide ; Release « released » → res[1] ; RecordMO « charged » →
  `&newBalance`. Tous les autres chemins → `nil`. `resolveTerminal` reçoit la valeur.
- [ ] Réécrire le commentaire de `defaultBalanceCacheTTL` et la doc de paquet (la réhydratation est cohérente
  parce que la lecture unifiée voit chaque delta committé, ADR-0022).
- [ ] Mutation : passer `nil` au lieu de `&newBalance` dans Reserve ⇒ le test tombe.
- [ ] Commit.

### Task 4 : Transfer et ChangeBalanceScope voient les deltas

**Files :** `internal/storage/postgres/billing.go`, test `billing_admin_integration_test.go`.

- [ ] **Tests (rouges)** :
  - `TestTransferOverdrawGuardSeesPendingDeltas` : source topup 10, `RecordDurable(reserve, −8)` ; transfer de 5
    ⇒ `ErrInsufficientCredit`. Rouge : accepté (lit 10).
  - `TestChangeScopeRefusesPendingDeltas` : aucune ligne `balances`, un seul `RecordDurable(release, +3)` ⇒
    solde unifié 3 ; change-scope ⇒ `ErrConflict`. Rouge : accepté (lit 0).
- [ ] **Implémentation** : `GetBalanceForUpdate` unifié (Task 1). `Transfer` : verrouiller les deux
  propriétaires **dans l'ordre** `(owner_type, owner_id)` avant tout `applyEntry` (même ordre que le replieur),
  puis la garde sur la source.
- [ ] Mutation : garde sur `folded` seul ⇒ les deux tombent.
- [ ] Commit.

### Task 5 : `FoldOnce`

**Files :** `internal/storage/postgres/billing.go`, test `internal/storage/postgres/billing_fold_integration_test.go`.

- [ ] **Test** — `TestFoldOnceMovesDeltasWithoutChangingTheBalance` : deux propriétaires, 3 deltas chacun ;
  unifié avant = unifié après `FoldOnce(ctx, 5000)` ; `balance_deltas` vide pour eux ; `balances` = unifié.
  Deuxième `FoldOnce` ⇒ aucun changement. Variante lot : `FoldOnce(ctx, 2)` ne replie que 2 deltas, l'unifié
  reste exact.
- [ ] **Test concurrence** — 4 goroutines `FoldOnce` pendant que 4 goroutines insèrent 200 deltas via
  `RecordDurable` ; à la fin, `FoldOnce` jusqu'à 0 ⇒ `balances` = somme attendue exacte (aucun delta perdu ni
  compté deux fois). Tolérer 40P01 dans `FoldOnce` (rejouer).
- [ ] **Implémentation** : `FoldOnce(ctx, limit int) (folded int64, err error)` et
  `OldestPendingDelta(ctx) (time.Time, bool, error)`.
- [ ] Mutation : retirer `SKIP LOCKED` ou le `DELETE` ⇒ le test de concurrence ou d'exactitude tombe.
- [ ] Commit.

### Task 6 : aucun dépassement en prépayé strict sous charge (DoD)

**Files :** `internal/billing/concurrency_test.go` (ou nouveau `fold_concurrency_integration_test.go`).

- [ ] **Test** — `TestStrictPrepaidNeverOverdrawsWhileFolding` : harnais 50 crédits ; 20 goroutines × 10
  réserves de 1 ; en parallèle une goroutine `FoldOnce` en boucle et une goroutine qui `DEL` le cache toutes
  les 5 ms (réhydratations forcées). Assert : réserves acceptées ≤ 50, `Balance` final ≥ 0, et
  `SUM(ledger.credits)` = `Balance`.
- [ ] Rouge lu : avec `GetBalance` naïf (mutation), le test dépasse. Restaurer.
- [ ] Vérifier `TestIdempotencyInvariantC` et toute la suite `internal/billing` + `internal/storage/postgres` verts.
- [ ] Commit ; revue (axes disjoints dont **code en trop**) → coupe → `make check` → PR2.

---

## PR3 — la boucle, la mesure du retard, le déploiement

### Task 7 : `runFold` et métriques

**Files :** `cmd/billing-svc/main.go`, `cmd/billing-svc/wiring.go`, `deploy/k8s/billing-svc.yaml`, test wiring.

- [ ] Constantes `foldInterval = time.Second`, `foldBatch = 5000` (pas de config : aucune valeur ne varie par
  déploiement aujourd'hui).
- [ ] `runFold(ctx, repo, lag prometheus.Gauge, logger)` sur le modèle de `runReap` : à chaque tick,
  `FoldOnce` en boucle tant qu'il rend un lot plein (rattrapage), puis `OldestPendingDelta` → gauge
  `billing_balance_deltas_lag_seconds` (0 si vide). Erreur ⇒ Warn, tick suivant.
- [ ] Test : la boucle démarre avec le superviseur (même garde que les autres tickers) et la gauge est
  enregistrée (garde de labels `metrics`).
- [ ] `deploy/k8s/billing-svc.yaml` : `strategy: {type: Recreate}` + test de garde `internal/deploy` si
  les manifests en ont un pour la stratégie.
- [ ] Guide d'ingénierie, catalogue de métriques : ligne `billing_balance_deltas_lag_seconds`, seuil 30 s.
- [ ] Déplacer la fiche `git mv tasks-todo/step-284.md tasks-done/` seulement en PR4.

## PR4 — la mesure

- [ ] Rejouer la mesure mono-client de step-280 sur le VPS (l'utilisateur lance les commandes sensibles via `!`) :
  `pg_stat_activity` sans `Lock`/`transactionid` en attente sur `balances`, débit mono-client consigné.
- [ ] ADR-0022 → `Accepted` ; DoD cochée ; `git mv tasks-todo/step-284.md tasks-done/` ; INDEX.
