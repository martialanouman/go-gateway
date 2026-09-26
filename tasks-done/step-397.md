# step-397 — La moitié de l'Admin API exige un scope que son contrat ne déclare pas

> **Jalon :** Dette du tableau de bord · **Statut :** LIVRÉE
> **Dépend de :** step-390 (toutes les surfaces Admin servies) · **Bloque :** step-410

## Pourquoi cette fiche existe

step-149 avait posé la règle en corrigeant 10 opérations de facturation : *un endpoint sécurisé qui
cache ses échecs d'auth publie un mensonge ; l'intention du contrat — la cohérence — est la source de
vérité, pas ses coquilles.* step-330 l'a appliquée à 7 opérations de plus, et en essayant d'en faire
une garde permanente a mesuré le reste : **50 des 110 opérations servies exigent un scope que
`api/openapi-admin.yaml` ne déclare pas.**

Concrètement, ajouter au test qui compare déjà les codes et les schémas
(`TestGeneratedSpecMatchesTheContractForEveryM1Operation`, `internal/adminapi/contract_test.go`) la
comparaison qui manque :

```go
reflect.DeepEqual(cOp["security"], gOp["security"])
```

fait passer la suite au rouge sur 50 opérations d'un coup : `security` vaut `<nil>` côté contrat et
`[map[OperatorBearer:[admin:read]]]` côté servi.

Familles entières : exact-routes, suppressions, opt-out keywords, inbound numbers & keywords,
antispam rules, routing scripts, rate plans, billing providers, plus `get-billing-ledger`,
`get-message-trace`, `list-unrouted-mo`, `check-suppression`, `change-balance-scope`,
`assign-inbound-number`, `assign-routing-script`.

**Ce que ça coûte.** Le tableau de bord vit dans un dépôt séparé et génère ses clients depuis ce YAML
(`@martialanouman/gateway-api-contracts`). Pour ces 50 opérations il croit qu'un jeton opérateur
quelconque suffit : ni les scopes requis, ni les `401`/`403` que le service renverra vraiment n'y
sont. Un opérateur à scope partiel reçoit un 403 que son client ne sait pas lire, et l'UI ne peut pas
distinguer « pas le droit » de « pas connecté ».

**Ce qui tient le sol en attendant.** `TestEveryGeneratedOperationRequiresAScope` (posé par step-330)
empêche le cas grave — une opération servie **sans aucun scope**, donc publique, parce que
`auth.Middleware` ne lit que `ctx.Operation().Security` et que huma ne lui donne jamais le bloc
`security:` global du document. Ce qu'il ne voit pas, c'est le désaccord entre le publié et l'exigé.

**Pourquoi maintenant et pas avant.** Cette fiche dépend de step-390 : tant que des surfaces Admin
restent à servir, la liste des 50 bouge. Les steps 340→390 déclarent leur `security:` en même temps
qu'elles servent leurs opérations — elles n'alimentent pas cette dette, elles ne la réduisent pas non
plus.

## Périmètre

Rendre le contrat honnête sur les 50, puis fermer la classe pour de bon.

- **Le YAML** : `security: [ { OperatorBearer: [ admin:read|admin:write ] } ]` sur chacune des 50, avec
  le scope que le code exige déjà — c'est `scopeSecurity(...)` dans `internal/adminapi/` qui fait foi,
  pas une relecture d'intention. Plus les réponses `401` et `403` là où elles manquent, sur le modèle
  des voisines (`get-customer` : 200, 404, 401, 403, 422).
- **Le bump** : additif, donc `oasdiff` ne classe rien `ERR` — bump **mineur** de `api/package.json`,
  comme 2.0.0 → 2.1.0 en step-149 et 4.2.1 → 4.3.0 en step-330. À confirmer par `make contracts`, pas
  à supposer.
- **La garde définitive** : ajouter la comparaison `security` à
  `TestGeneratedSpecMatchesTheContractForEveryM1Operation`, puis **retirer**
  `TestEveryGeneratedOperationRequiresAScope`, qui n'était que le sol posé en attendant — à condition
  que la comparaison couvre ce qu'il couvrait. Attention : `DeepEqual` seul passe quand **les deux**
  côtés sont vides, donc la non-vacuité reste à exiger séparément.
- Vérifier au passage s'il reste des opérations **sans aucun** `security:` côté servi : la garde de
  step-330 dit que non aujourd'hui, la refaire dire par la comparaison est le but.

## Chaîne de preuves

1. **Le rouge existe déjà** : ajouter la comparaison avant de toucher au YAML doit nommer 50
   opérations. C'est le rouge de départ, et il se lit.
2. Corriger le YAML par familles, en vérifiant que le compte d'opérations en échec décroît — une
   famille corrigée, un sous-ensemble en moins.
3. **Mutation** : une fois vert, changer `admin:read` en `admin:write` sur **une** opération du
   contrat doit faire tomber la comparaison. Sans elle, on n'a prouvé que l'égalité de deux absences.
4. **Mutation** : vider la liste de scopes d'une opération **des deux côtés** doit tomber aussi —
   c'est le cas que `DeepEqual` laisse passer et que la non-vacuité doit rattraper.
5. `make check` vert, et `make contracts` qui reconnaît le bump mineur.

## Hors périmètre

Le `500` qu'aucune opération du document ne déclare alors que toute panne d'infrastructure le produit :
c'est une convention de tout le contrat, pas un défaut de ces 50.

Passer `auth.Middleware` en fail-closed (une opération sans `security:` refusée plutôt que servie).
C'est la ceinture qui rendrait la classe impossible plutôt que visible, mais elle change un
comportement global et mérite sa propre justification.

## Design arrêté

Rouge de départ lu le 2026-09-26 : lever la condition `cOp["security"] != nil` fait tomber **50**
opérations, toutes `contract: <nil>` (32 `admin:write`, 18 `admin:read`). Les codes `401`/`403` sont
déjà au contrat : la comparaison stricte des codes passait, seul `security:` manque.

- **YAML** : `security: [ { OperatorBearer: [ <scope servi> ] } ]` sur les 50, le scope recopié du
  rouge (donc de `scopeSecurity(...)`), pas relu d'intention. Bump **mineur** 6.6.0 → 6.7.0, confirmé
  par `make contracts`.
- **Garde** : dans `TestGeneratedSpecMatchesTheContractForEveryM1Operation`, la comparaison `security`
  devient inconditionnelle et monte **avant** la sortie anticipée des opérations d'upgrade ; la
  non-vacuité (chaque alternative nomme `OperatorBearer` avec au moins un scope) y est exigée sur le
  côté servi. `TestEveryGeneratedOperationRequiresAScope` est retiré ; `TestUpgradeOperationsDeclareTheirContract`
  se replie dans la branche d'upgrade du même test (coupe de revue).
- **Flux WebSocket** (constat de revue) : les trois `stream-*` exigent `admin:read` et ne déclaraient
  que `[101, 401]`. Le 403 est réel (prouvé par `TestStreamMetricsRequiresTheOperatorScope`), donc
  déclaré : l'attendu devient `[101, 401, 403]`, additif, même bump mineur.
- **Couverture équivalente** : l'ancienne garde parcourait toutes les opérations générées, la nouvelle
  parcourt `m1Operations` ; `TestGeneratedSpecRegistersNoOperationOutsideTheM1Surface` garantit que
  les premières sont incluses dans les secondes.
