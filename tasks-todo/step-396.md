# step-396 — Le Postgres *lent* : mesurer l'équivalence « lent ≡ coupé »

> **Jalon :** Dette ouverte par step-260c · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

`internal/testutil/tcpproxy` ne sait que **sévérer** : `Cut()` ferme les connexions vivantes et
accepte-puis-ferme les suivantes. Toutes les politiques PostgreSQL et Redis de §16 sont donc prouvées
contre une panne **franche**. La panne qui arrive le plus en production n'est pas celle-là : c'est la
dépendance qui répond, mais trop tard.

## Ce que cette fiche N'EST PAS — la prémisse de step-260c, corrigée

`tasks-done/step-260c.md` désignait deux chemins « que seule la latence révèle ». **Les deux sont mal
décrits**, et c'est pour ça que cette fiche existe séparément plutôt que d'avoir été traitée là-bas.

- **`withTerminalLock` (`internal/billing/billing.go:707-732`) ne rend pas `errs.ErrConflict` au
  porteur lent.** Il le rend au **waiter** qui n'a rien obtenu après `terminalLockWait` = 2 × 5 s
  = 10 s. Le porteur, lui, voit sa section critique annulée à `terminalCriticalTimeout` (4 s) et reçoit
  un `DeadlineExceeded` que `postgres.translate` code en `ErrInternal`. Comme le `defer` rend le verrou
  à ce moment-là, un Postgres simplement lent ne fait pas attendre un waiter 10 s : **la branche
  `ErrConflict` n'est probablement pas atteignable par la latence seule.** Le premier travail de cette
  fiche est de trancher ça, pas de construire l'outillage.
- **Le « rejet définitif là où il faudrait un rejeu » n'existe pas sur ce chemin.** La règle « erreur
  codée ⇒ offset commité ⇒ message enterré » est celle de `router.handle`, sur la voie de la
  **réserve**. Sur la voie **terminale**, personne ne lit le code : `settle.Settler` échoue ouvert sur
  *toute* erreur (`settle.go:116-121` et `:143-147`) et `billing.Reaper` rejoue à la passe suivante sur *toute* erreur
  (`reaper.go:215-225`).
- **`defaultSettleTimeout` n'a jamais eu besoin d'un proxy retardateur.** `settle.WithTimeout`
  (`settle.go:63`) est une option publique : un faux client gRPC qui dort au-delà du délai reproduit
  « le settler abandonne pendant que le terminal s'écrit » sans le moindre conteneur. Et la politique
  correspondante — « Facturation — règlement : fail-open, `billing.Reaper` réconcilie » — est **déjà**
  en §16 avec son test.

## Ce qui reste, et qui est la vraie question

**Une politique prouvée sous coupure est-elle prouvée sous latence ?** `Cut()` produit une socket morte
immédiate ; un Postgres lent produit un contexte qui expire. Ce ne sont pas les mêmes erreurs, elles ne
traversent pas `translate` de la même façon, et les chemins de *réparation* (compensation de cache,
écriture de rattrapage) se déclenchent sur l'expiration bien plus souvent que sur la coupure — c'est
exactement ce que la revue de step-260b avait trouvé avec `dropBalanceCache`, réparant sur le contexte
mort de l'appelant.

## Périmètre

1. **Trancher d'abord** : la branche `ErrConflict` de `withTerminalLock` est-elle atteignable sans
   arrêter le porteur autrement ? Si non, elle sort du périmètre et la fiche le dit.
2. Étendre `tcpproxy` d'un relais **retardateur** (`Slow(d)` / `Normal()`, symétrique de
   `Cut`/`Resume`) : une trentaine de lignes autour de `io.Copy`, sans toucher à `Cut`.
3. Rejouer **une** politique déjà prouvée sous coupure — la réserve billing est la candidate, c'est la
   plus riche — avec un Postgres lent, et comparer. L'objectif n'est pas d'ajouter une ligne à §16 mais
   de savoir si les lignes existantes tiennent sous l'autre forme de panne.

## Le coût, pour que la décision soit informée

Un test qui doit tenir un porteur bloqué **plus de 10 s** est un test qui coûte 10 s au budget CI
(`go test -race -timeout 10m ./...`). C'est supportable, ce n'est pas gratuit, et ça n'achète pas une
politique manquante : ça achète une **preuve d'équivalence**. C'est pourquoi step-260c a fiché plutôt
que payé.

## Hors périmètre

Les politiques elles-mêmes : elles sont écrites et testées. Cette fiche ne rouvre §16 que si la mesure
montre qu'une ligne ne tient pas sous latence.
