# step-260c — Les trois politiques PostgreSQL hors facturation

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-260b · **Bloque :** —

## Pourquoi cette fiche existe

step-260b a livré `pgtest.Cuttable` et écrit la **première** ligne PostgreSQL de la matrice §16 —
celle du billing, la seule qu'elle prouvait. Il en manque trois, et le `[MUST]` de §16 exige
« documentée **et** testée ». Les écrire sans les prouver aurait refait exactement la dette que
`step-250d` répare : une ligne dans un document daté, que plus personne ne relit.

L'outillage existe désormais (`pgtest.Cuttable` / `CuttableConfig`, symétriques de `redistest`), donc
chacune de ces trois lignes coûte un test, pas une infrastructure.

## Les trois lignes

| Sous-système | Politique observée dans le code | Où |
|---|---|---|
| Auth de bind SMPP | **fail-closed** `ESME_RSYSERR`, **aucun cache** — chaque bind lit la base | `internal/smppserver/bind.go:34-40` → `postgres.BindRepo.BindCredentialBySystemID` |
| Auth REST (clés API) | **fail-closed** 500, aucun cache de principal, sur **chaque** requête authentifiée | `internal/restapi/auth.go:38-42` → `postgres.APIKeyRepo.PrincipalByAPIKeyHash` |
| Snapshots de config du routeur | **dégradé masqué** : le dernier snapshot reste servi, la config devient périmée | `internal/config/watcher.go:130-132`, `internal/routing/snapshot.go:260-266` |

Les deux premières sont des **dépendances dures sur le chemin chaud sans aucun cache** : un Postgres
qui tombe ferme l'ingress SMPP *et* l'API REST. C'est un fait d'exploitation qui mérite d'être écrit,
pas seulement testé.

La troisième n'est ni fail-open ni fail-closed, et c'est ce qui la rend intéressante : le vrai risque
n'est pas le refus, c'est la **config périmée** — un opt-out retiré qui continue de s'appliquer, un
disjoncteur non rafraîchi. Au **boot** en revanche Postgres est une dépendance dure
(`cmd/router-svc/wiring.go:737-765`, retry backoff : le pod ne devient jamais ready). Les deux moitiés
doivent être dans la ligne.

## Périmètre

Un test de chaos par politique, **dans le paquet qui porte la politique** (`docs/strategie-de-test-passerelle.md`
§4.8). Forme éprouvée : construire → contrôle avec Postgres UP → `Cut()` → assertion → `Resume()` →
retour au nominal, la mutation « la coupure ne compte pas » vue tomber sur chacun.

Pour les snapshots, l'assertion structurante n'est pas « ça refuse » mais **« ça sert encore l'ancien »** :
un rebuild échoué ne doit ni vider le snapshot, ni y glisser un `nil`, ni faire tomber le pod.

## Ce que step-260b a laissé et qu'il faut reprendre ici

**Le Postgres *lent* plutôt que coupé.** `tcpproxy` ne sait que sévérer, jamais ralentir, et deux
chemins ne se révèlent que sous latence :

- `withTerminalLock` (`internal/billing/billing.go:709`) renvoie une erreur **codée** `errs.ErrConflict`
  quand un porteur dépasse sa section critique de 4 s. Un Postgres lent produirait donc un rejet
  définitif là où il faudrait un rejeu — le contraire exact de la politique que §16 vient d'écrire.
- `settle`'s `defaultSettleTimeout` de 200 ms expire pendant que billing-svc écrit encore (sa section
  critique va jusqu'à 4 s). Le settler compte un échec et abandonne alors que le terminal s'écrit
  quand même : la panne est invisible côté appelant.

Aucun des deux n'est atteignable avec l'outillage actuel. Les couvrir demande un proxy qui **retarde**
plutôt qu'il ne coupe — une extension de `tcpproxy`, à peser contre le fait qu'un test qui attend 4 s
coûte au budget CI.

## Depuis step-250e — une quatrième voie Postgres, déjà écrite en §16

La matrice §16 porte désormais une **deuxième** ligne PostgreSQL, ajoutée avec son test de chaos :
« PostgreSQL (routage L0, numéro exact) ». Depuis que `exactroute:{msisdn}` est un cache read-through,
un possible-hit du Bloom que le cache n'a pas interroge la table durable — donc **un Postgres coupé
bloque aussi la résolution L0, en rejeu**. Ce n'est pas une des trois voies que cette fiche recense,
c'est une quatrième, et elle est déjà prouvée : ne pas la ré-ouvrir, mais en tenir compte dans le
recensement pour ne pas la déclarer manquante.

Le piège consigné par step-250e vaut aussi ici : `postgres.translate` attache `errs.ErrInternal` à
toute panne d'infra, et une erreur **codée** fait enterrer le message (CDR `rejected`, offset commité)
au lieu de le redélivrer. Toute voie qu'on veut fail-closed en rejeu doit retirer ce code — et le
vérifier sur un Postgres réellement coupé, un faux renvoyant un `errors.New` nu ne le montrant pas.

## Design arrêté

Arrêté le 2026-09-11, après relecture du code des trois voies et contre-examen adverse. Trois
arbitrages, et deux corrections à cette fiche elle-même.

### Ce que cette fiche affirmait de faux sur le boot

Elle annonce, pour les snapshots, « au boot Postgres est une dépendance dure
(`cmd/router-svc/wiring.go:737-765`, retry backoff : le pod ne devient jamais ready) ». **C'est deux
comportements différents confondus en un.** `newRouterApp` appelle `openStores` **avant**
`loadBootSnapshots`, et `postgres.NewPool` (`internal/storage/postgres/pool.go:26-56`) pingue à chaud
**sans réessayer** : un Postgres injoignable au démarrage du process fait sortir `newRouterApp` en
erreur, donc **CrashLoopBackOff**, pas un pod qui pend. La boucle de `loadWithRetry`
(`wiring.go:805-833`, 500 ms → 30 s, sans plafond d'essais) n'est atteinte que si Postgres tombe
*entre* l'ouverture du pool et le chargement des snapshots. Redis, lui, passe bien par `loadWithRetry`
dès l'ouverture (`wiring.go:124`) : l'asymétrie est réelle et la ligne §16 doit la porter.

La moitié « fail-fast » est **déjà prouvée** par `TestNewRouterAppReportsAnUnreachablePostgres`
(`cmd/router-svc/wiring_test.go:53-66`) : la ligne §16 la cite, aucun test neuf. Seule la boucle de
retry n'a aucune couverture.

### Arbitrage 1 — le Postgres lent : fiché, pas couvert

La DoD exige que la question soit tranchée. Elle l'est, et sur une **prémisse corrigée** : les deux
chemins que la section « Ce que step-260b a laissé » désigne ne sont pas ce qu'elle en dit.

- `withTerminalLock` (`internal/billing/billing.go:709-736`) ne rend **pas** `errs.ErrConflict` au
  porteur qui dépasse 4 s. Il le rend au **waiter** qui n'a rien obtenu après `terminalLockWait`
  = 2 × 5 s = 10 s. Le porteur lent, lui, reçoit un `DeadlineExceeded` de pgx à
  `terminalCriticalTimeout`, que `translate` code en `ErrInternal`.
- Et sur le chemin terminal, **personne ne lit le code** : `settle.Settler` échoue ouvert sur *toute*
  erreur (`settle.go:37-41`), `billing.Reaper` rejoue à la passe suivante sur *toute* erreur
  (`reaper.go:215-225`). Le « rejet définitif là où il faudrait un rejeu » que la fiche redoute
  **n'existe pas dans le code**.
- `defaultSettleTimeout` n'a jamais eu besoin d'un proxy retardateur : `settle.WithTimeout`
  (`settle.go:63`) est une option publique, et un faux client gRPC qui dort suffit à reproduire
  « le settler abandonne pendant que le terminal s'écrit ». La politique correspondante — « Facturation
  — règlement : fail-open, `billing.Reaper` réconcilie » — est **déjà** en §16, avec son test.

Étendre `tcpproxy` ici serait donc payer une infrastructure pour un chemin qui n'en a pas besoin et un
autre dont la branche redoutée n'est pas atteignable par simple latence (le porteur rend son verrou à
4 s, bien avant les 10 s du waiter). La seule question qui reste ouverte est l'**équivalence
« lent ≡ coupé »** : elle part en **step-396**, avec son coût réel (un relais retardateur, ~30 lignes,
plus un test qui doit tenir un porteur bloqué **plus de 10 s**) et la prémisse ci-dessus écrite noir
sur blanc — sans quoi la fiche repartirait sur la fausse piste.

### Arbitrage 2 — le test des snapshots vit dans `cmd/router-svc`

`docs/strategie-de-test-passerelle.md` §4.8 veut le test « dans le paquet qui porte la politique ».
Candidat naturel : `internal/config`, puisque le log-et-garde est à `watcher.go:128-133`. **Refusé.**
Ce que §16 va écrire n'est pas « le Watcher survit à une erreur » — c'est générique, ignorant de ce
qu'est « l'état », et déjà prouvé par `TestWatcherRebuildFailureKeepsRunning` (`watcher_test.go:121`).
C'est « **les routes restent servies, ni `nil`, ni vides** », et cela se décide dans la closure de
rebuild de `cmd/router-svc/wiring.go:704-753`. Un test dans `internal/config` devrait fabriquer une
fausse closure : il testerait le faux — exactement la faute que step-250e a payée. Le précédent existe
déjà : `cmd/router-svc/chaos_test.go` fait « graphe bâti depuis la config, puis coupure », et
`routerApp` expose `routes` et `watcher` pour ça.

### Arbitrage 3 — le rebuild partiel : une note en §16, et le vrai défaut en fiche

Constat vérifié : la closure swappe les routes (`wiring.go:709`) **avant** quatre `return err`
possibles (Bloom exact, opt-out, scripts, crédit, contenu). Chaque étape est individuellement atomique
— `Bloom.Reload` et le garde d'opt-out ne swappent qu'en succès — donc un échec laisse un **préfixe
appliqué**, jamais un état vide. Le commentaire `wiring.go:688-690` (« each component keeps its current
state on its own build failure ») est vrai par composant et faux pour l'ensemble.

Mais le défaut qui compte est ailleurs : **le watcher ne rejoue jamais de lui-même**
(`watcher.go:130-132` — un log, puis l'attente du prochain tick, aucun timer de retry). Un rebuild
totalement raté expose à la même « config périmée sans borne » qu'un rebuild partiel, qui n'en est
qu'un aggravant. La ligne §16 nomme les deux ; le correctif (réarmer le timer avec backoff sur `rerr`)
part en **step-395**, parce que §16 décrit ce que le code **fait**, et que le test doit prouver ça, pas
le correctif.

Corollaire pour le test : **aucune assertion sur l'état du Bloom**. Figer un préfixe d'application
comme s'il était une politique transformerait un accident en contrat.

### Ce que les tests prouvent, voie par voie

| Voie | Où vit le test | Contrôle lien debout | Sous coupure | Retour |
|---|---|---|---|---|
| Bind SMPP | `internal/smppserver` | bind valide → `ESME_ROK` ; **et** mauvais mot de passe → `ESME_RINVPASWD` | jamais `ESME_ROK` (ce serait un cache) ; `ESME_RSYSERR`, pas `ESME_RINVPASWD` — y compris pour un `system_id` inexistant | rebind `ESME_ROK` |
| Clés API REST | `internal/restapi` | clé valide → 200 ; **et** clé bidon → 401 | jamais 200 ; jamais 401 ; **500** `internal_error` — y compris pour la clé bidon | 200 |
| Snapshots (runtime) | `cmd/router-svc` | le hot-reload amène réellement la route B | sert **encore B** — ni la route C insérée, ni `ErrNoRoute`, ni `nil`, ni panique ; `Run` ne retourne pas ; le log est la seule trace | la route C est prise |
| Snapshots (boot, retry) | `cmd/router-svc` | — | `loadSnapshotWithRetry` ne retourne pas | il rend son résolveur |

Le second contrôle de chaque ligne — le mauvais mot de passe, la clé bidon, le hot-reload réellement
observé — est ce qui rend la coupure **observable** : sans lui, « pas `ESME_RINVPASWD` » et « sert
encore l'ancien » passeraient aussi bien Postgres debout.

### Extension retenue — la readiness des deux services du chemin chaud

`docs/plan-execution-passerelle.md` §1.5 lie explicitement la readiness aux politiques de panne, et les
deux services enregistrent `postgres.PingCheck` (`cmd/rest-api-svc/wiring.go:189`,
`cmd/smpp-server-svc/wiring.go:303`). Leur ligne §16 serait incomplète sans ça : comme toutes les
répliques partagent le même Postgres, elles quittent le load balancer **ensemble** — la dégradation
devient une indisponibilité totale. Deux tests de plus, sur le patron de
`cmd/router-svc/chaos_test.go`, et **aucun conteneur neuf** : les deux paquets `cmd/` démarrent déjà
Postgres et Redis dans leurs tests de câblage.

### Mutations à jouer (aucune ligne §16 avant de les avoir vues tomber)

1. **Neutraliser `Cut()`** sur chacun des six tests : tous doivent tomber bruyamment.
2. **Repointer le consommateur du pool coupable sur `pgtest.Pool`** : le test meurt alors avec la
   dépendance qu'il prétend observer, et doit tomber (mémoire `hollow-test-fixtures`).

## Definition of Done

- [ ] `make check` vert
- [ ] les 3 politiques prouvées sur un Postgres **réellement coupé**, chacune dans son paquet
- [ ] pour chacune, la mutation « la coupure ne compte pas » (neutraliser `Cut()`) vue tomber
- [ ] §16 gagne les 3 lignes — **écrites après le test, jamais avant**
- [ ] la question du Postgres lent tranchée : soit couverte, soit fichée avec son coût

## Hors périmètre

Les quatre politiques Redis → **step-250d**. Manifests et PDB → **step-270**.
