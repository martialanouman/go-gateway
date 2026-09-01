# step-260b — Failover Postgres : ce que le billing fait quand l'autorité disparaît

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-260 · **Bloque :** step-270

## Pourquoi cette fiche existe

step-260 portait deux pannes indépendantes — le drain de pod et le failover Postgres. Le drain est
livré. Le failover part ici : autre sous-système, autre outillage, et **un outil de coupure qui
n'existe pas encore**.

## La prémisse de step-260 était fausse — le dire avant de commencer

step-260 annonçait que « billing réhydrate les soldes **depuis le grand livre** ». Le code ne fait pas
ça. `GetBalance` lit `control_plane.balances`, une table de solde **matérialisé** maintenue dans la même
transaction que l'insertion au grand livre (`AdjustBalance` applique un delta signé). **Il n'existe
aucune reconstruction par `SUM(billing_ledger)` en production** — les seuls `SUM` du dépôt sont dans
deux tests.

Ce n'est pas un défaut : l'invariant « le solde EST la somme du grand livre » est tenu par la
transaction, pas par un recalcul. Mais le critère d'acceptation doit être réécrit avant d'être testé,
sinon on teste une phrase plutôt qu'un système. **Trancher d'abord** : faut-il une reconstruction
depuis les entrées (une requête, un repo, une méthode sur `LedgerStore`), ou le critère devient-il
« le cache Redis se réhydrate correctement depuis l'autorité durable après une coupure » ?
Recommandation : la seconde. La première n'a d'utilité qu'en réparation d'une divergence, et rien
n'établit qu'elle survienne.

## Ce que l'exploration a déjà établi

**Ce qui va bien.** `Reserve` est fail-closed dans les **deux** cas, cache froid et cache chaud : un
échec durable déclenche la compensation du débit spéculatif (`undoReserveCacheDebit`) puis renvoie
l'erreur. Et l'erreur est **non codée**, donc `errs.CodeOf` la classe en faute transitoire et le routeur
rejoue au lieu d'écrire un CDR `rejected` — la bonne issue. Seul le chemin cache-froid est testé
aujourd'hui, avec un faux (`internal/billing/failclosed_test.go:42`).

**Deux défauts présumés, à confirmer sur une vraie coupure :**

1. **`Release` laisse le cache crédité et le grand livre vide.** `release.lua` recrédite le cache, puis
   `resolveTerminal` échoue sur Postgres et la fonction retourne **avant** la branche de nettoyage
   (atteinte seulement si `outcome == outcomeYielded`). Solde caché > solde durable jusqu'à expiration
   du TTL de 10 min, et un `Reserve` peut passer sur ce crédit fantôme. Aggravant : `settle.Settler`
   est fail-open et n'en propage rien.
2. **`Capture` supprime le hold Redis avant de consulter Postgres.** `capture.lua` fait son `DEL` en
   premier ; si le durable échoue ensuite, le hold est perdu. La reprise est correcte une fois Postgres
   revenu (le montant se relit depuis `ReserveEntry`), et le reaper rattrape la réservation orpheline —
   mais **le reaper ne corrige pas le cache** de (1).

**Ce qui manque comme outillage.** Il n'existe **aucun `pgtest.Cuttable`** ; `internal/testutil/pgtest`
n'expose que `Config` et `Pool` et n'importe pas `tcpproxy`. À écrire par symétrie exacte avec
`redistest.go:118-145`, avec trois différences à ne pas ignorer :

- `pgtest.start()` applique les migrations : le pool coupable doit passer par le **conteneur partagé
  déjà migré** (partir de `Config(t)`), jamais par un second conteneur.
- **Le harnais billing câble `pgtest.Pool(t)` en dur** (`billing_integration_test.go:51`). Il accepte un
  `*redis.Client` injecté — c'est ce qu'exploite step-250 — mais pas de pool. Il faut le paramètre
  symétrique, **et lire les assertions par un second pool non coupé**, sinon la vérification meurt avec
  la dépendance.
- `pgxpool` pré-chauffe des connexions (`MinConns` ≥ 2) et a un health-check : `Cut()` les ferme toutes
  et les nouvelles acquisitions redialent vers un proxy en accept-then-close. Construire **avant** de
  couper, comme le veut la discipline `redistest`.

**§16 n'a aucune ligne PostgreSQL**, ni pour billing ni pour l'auth de bind SMPP, l'auth REST ou les
snapshots du routeur. Et sa ligne « Facturation (globale) — fail-open par défaut » est **fausse pour la
réserve**, qui est fail-closed avec rejeu : elle confond trois politiques (réserve, règlement,
autorisation externe §6.10). La seule politique Postgres écrite du dépôt est un commentaire de câblage
dans `cmd/billing-svc/wiring.go:328`.

## Design arrêté

**Le critère d'acceptation, réécrit.** La prémisse « réhydratation depuis le grand livre » est fausse et
le reste : il n'existe aucun `SUM(billing_ledger)` en production. Retenu — la seconde option de la
recommandation ci-dessus : *« le cache se réhydrate correctement depuis l'autorité durable, et rien ne
passe pendant la panne »*. Aucune reconstruction par les entrées n'est ajoutée : elle ne servirait qu'à
réparer une divergence dont rien n'établit qu'elle survienne.

**`Reserve` a trois voies durables, pas deux.** `"cold"` (rehydrate), `"reserved"` (le miroir durable
d'un débit déjà pris au cache) et **`"held"`** — la troisième, que la fiche ne nommait pas, est celle
qu'emprunte une redélivrance pendant la panne. Les trois sont testées.

**L'assertion qui compte n'est pas « erreur non codée ».** Contrairement à la voie Redis, une erreur
Postgres EST codée : `postgres.translate` enveloppe une panne pgx non reconnue dans `errs.ErrInternal`.
C'est sans conséquence, parce que le code ne survit pas au gRPC : `grpcerr.Status` en fait un
`*status.Error`, dont `errs.CodeOf` ne tire rien, et le routeur rejoue. La seule confusion qui coûterait
est une panne prenant le masque d'un refus de fonds — `grpcserver.go` répond `reserved=false` pour
`errs.ErrInsufficientCredit` seul, et le routeur en fait un rejet **définitif**. C'est donc ça qui est
asserté, sur les deux voies où c'est atteignable. L'asymétrie du code est épinglée par un tripwire qui
dit, dans son message, qu'il n'est pas une règle.

**Défaut 1 corrigé, défaut 2 documenté.** `Release` lâche désormais le cache que `release.lua` vient de
recréditer quand le durable n'atterrit pas — le geste exact de la branche `outcomeYielded` et de
`undoReserveCacheDebit`, factorisé en `dropBalanceCache` sur les quatre sites. `Capture` garde son `DEL`
en première mutation : le rejeu reconstruit le montant depuis `ReserveEntry`, et compenser rouvrirait la
course de double-capture que le `DEL` atomique existe pour fermer. Un test le prouve au lieu de le
supposer.

**§16 : une seule ligne PostgreSQL, celle qui est prouvée.** Il en manque trois autres (bind SMPP, clés
API REST, snapshots du routeur). Le `[MUST]` de §16 exige documentée **et** testée ; les écrire sans
test aurait refait la faute que step-250d répare. Elles partent en **step-260c**, avec ce que
l'exploration a déjà établi de chacune. La ligne « Facturation (globale) », elle, est scindée en trois —
réserve, règlement, autorisation externe — parce que le code sépare ce qu'elle confondait.

**Deux règles de harnais, payées cher.** Le pool sous test se construit **avant** la coupure (le
constructeur de production pingue à chaud), et les assertions se lisent par un **second pool non coupé** :
la mutation qui repointe `verify` sur le pool coupé fait tomber les trois tests, ce qui est la preuve que
la garde porte. `MinConns = 2` dans `CuttableConfig` est de la fidélité, pas du mécanisme — mesuré : à
zéro les tests passent quand même et la panne reste immédiate. Le commentaire le dit, parce que la
première version affirmait le contraire.

**Ce que le tour de mutations a trouvé en plus.** `TestReleaseYieldsToCaptureReconcilesCache`, le garde
BLOQUANT-1, ne gardait rien : sa mise en scène passait par `Accountant.Capture`, qui supprime le hold, si
bien que `release.lua` répondait `no_reservation` et ne touchait jamais au solde caché — le crédit
fantôme que le test prétendait attraper ne pouvait pas se produire. La capture gagnante est désormais
écrite directement au grand livre, et le test tombe quand on retire la réconciliation. Corrigé ici parce
que c'est la branche que cette PR vient de refactorer.

## Périmètre

- `pgtest.Cuttable` / `CuttableConfig` (+ le point d'injection dans le harnais billing).
- `Reserve` sous coupure réelle : les **deux** branches, cache froid et cache chaud ; l'erreur reste
  non codée ; rien d'écrit durablement ; idempotence à la redélivrance — avec la **seconde**
  redélivrance de step-250, la première ne prouvant rien.
- `Release` et `Capture` sous coupure : constater, puis **corriger** la divergence de (1).
- §16 : ajouter les lignes PostgreSQL manquantes et corriger la ligne « Facturation ».

## Definition of Done

- [ ] `make check` vert · invariant (c) tenu après la fenêtre de panne
- [ ] chaque politique prouvée sur un **vrai** Postgres coupé, pas un faux `LedgerStore`
- [ ] la mutation « la coupure ne compte pas » vue tomber sur chaque test
- [ ] §16 corrigée (lignes PostgreSQL ; ligne Facturation scindée en réserve / règlement / externe)

## Hors périmètre

Le drain de `supervisor.Ordered` reste **non borné globalement** : il attend `<-dones[i]` sans timeout,
donc un composant qui ignore son contexte bloque jusqu'au SIGKILL du kubelet. Aucun composant actuel ne
l'exhibe. À traiter ici si l'occasion se présente, sinon à ficher.

Manifests, PDB, `terminationGracePeriodSeconds` → step-270.
