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

## Definition of Done

- [ ] `make check` vert
- [ ] les 3 politiques prouvées sur un Postgres **réellement coupé**, chacune dans son paquet
- [ ] pour chacune, la mutation « la coupure ne compte pas » (neutraliser `Cut()`) vue tomber
- [ ] §16 gagne les 3 lignes — **écrites après le test, jamais avant**
- [ ] la question du Postgres lent tranchée : soit couverte, soit fichée avec son coût

## Hors périmètre

Les quatre politiques Redis → **step-250d**. Manifests et PDB → **step-270**.
