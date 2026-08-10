# step-213 — Le contrat déclare 30 opérations que personne n'implémente, et rien ne le dit

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** step-208 (go-live), step-214 → step-220

## But

Rendre **impossible** qu'une opération soit déclarée dans un contrat publié sans que quelqu'un ait dit
ce qu'elle devient. Aujourd'hui l'écart entre le contrat Admin et le code est de 30 opérations, et
aucune des quatre gardes existantes ne le voit.

Cette fiche produit une **garde et un triage**. Elle n'implémente aucune des 30 : leur construction est
step-214 → step-220.

## Le constat, mesuré

| Source | Compte |
|---|---:|
| Opérations déclarées sous `paths:` dans `api/openapi-admin.yaml` | **133** |
| Opérations enregistrées par `internal/adminapi` | **103** |
| **Déclarées sans aucune implémentation** | **30** |
| Implémentées hors contrat | 0 |

Recoupé par deux sources indépendantes : l'extraction des `OperationID` du code, et la liste
`m1Operations` de `internal/adminapi/contract_test.go` (103 entrées, tenue honnête par
`TestGeneratedSpecRegistersNoOperationOutsideTheM1Surface`). Les deux concordent exactement.

**Ce ne sont pas des reliquats.** Les tables existent (`control_plane.customer_groups`, `webhooks`,
`sender_id_rewrite_rules`), la spec les décrit (§6.16, §6.17, §6.22, §6.23 ; les webhooks par le modèle
§6.18 qui les attribue au compte SMPP, et par la §MO/DLR), et
`docs/specification-technique-tableau-de-bord.md` les attend explicitement. Mais
`docs/plan-execution-passerelle.md` ne mentionne ni §6.16, ni §6.17, ni §6.18 : **aucun jalon ne les
porte**, et le plan s'arrête au go-live. Trois plans documentaires affirment que ces surfaces existent ;
le code dit le contraire ; le tableau de bord, dépôt séparé, consomme le contrat en package npm et
générerait des clients typés qui appellent des 404.

**Pourquoi rien ne l'a signalé.** Les trois gardes de `contract_test.go` raisonnent toutes sur
`m1Operations`, et vont toutes dans le sens **implémenté → déclaré** :

- `TestContractCoversEveryM1Operation` — chaque opération de la liste est dans le contrat ;
- `TestGeneratedSpecMatchesTheContractForEveryM1Operation` — et elle y correspond, champ par champ ;
- `TestGeneratedSpecRegistersNoOperationOutsideTheM1Surface` — le service n'expose rien hors liste.

Le sens inverse — *déclaré au contrat publié, implémenté par personne* — n'appartient à aucune d'elles.
`make contracts` ne le couvre pas non plus : il refuse un changement de contrat sans bump de
`api/package.json`, ce qui est une autre question.

**Le même trou existe côté public, latent.** `internal/restapi/conformance_test.go` a déjà le bon
vocabulaire (`implemented` + `deferred`, avec un commentaire qui explique que `cancel-message` a été
*retiré* du contrat par ADR-0009) — mais sa boucle fait `if !implemented[cop.OperationID] { continue }`.
Une opération ajoutée au YAML public sans être inscrite dans l'une des deux maps est ignorée en silence.
Il n'y a aujourd'hui aucun écart (5 servies sur 5) : la garde est donc gratuite à poser, et ne le sera
plus une fois qu'un écart existera.

## Périmètre

Des **tests** et un triage documenté. Aucun handler, aucun changement de contrat, aucune migration.

### D1 — La garde, côté Admin

Un test qui charge `api/openapi-admin.yaml` et exige que **chaque** opération déclarée sous `paths:`
soit classée : soit servie (présente dans `m1Operations`), soit inscrite dans une nouvelle liste
`deferred`, annotée de sa raison et de la step qui la portera. Une opération dans aucune des deux fait
échouer le test, en la nommant.

Transposer le patron de `internal/restapi/conformance_test.go` (`implemented` + `deferred`) — il est
éprouvé et déjà lu par tout le monde. **Ne pas inventer une seconde forme** : le dépôt aurait alors deux
descriptions concurrentes de la même règle.

### D2 — La même garde, côté public

Ajouter à `conformance_test.go` l'assertion de couverture qui lui manque : toute opération de `paths:`
est dans `implemented` ∪ `deferred`. Cinq lignes, zéro écart à absorber aujourd'hui.

### D3 — Le triage des 30

Chaque entrée de `deferred` porte sa raison et sa step. Le triage arrêté :

| Surface | Ops | Step | État réel |
|---|---:|---|---|
| Groupes de clients (§6.17) | 7 | step-214 | table `customer_groups` présente, **zéro code** |
| Webhooks admin | 4 | step-215 | repo `postgres/webhooks.go` livré en M4, aucune surface admin |
| Réécriture de sender ID (§6.16) | 5 | step-216 | table présente, **ni admin ni évaluation** dans le pool |
| Sessions REST | 3 | step-217 | `stream-sessions` existe, pas les trois opérations REST |
| Politiques de contenu (§6.23) | 4 | step-218 | client : colonne présente ; **plateforme : aucune table** |
| Métriques agrégées | 2 | step-219 | `stream-metrics` existe (push), rien en pull |
| Comptes et routes | 5 | step-220 | dont deux réglages **créables mais non modifiables** |

Total 30. Le compte par surface fait partie de ce qui est vérifié : il a été faux d'une unité à la
première rédaction (webhooks compté 5, faute d'un `get-webhook` que le contrat ne déclare pas).

## Points d'implémentation clés

- **Ne lire que `paths:`.** `api/openapi-public.yaml` déclare `on-mo` et `on-dlr` sous `webhooks:`
  (OpenAPI 3.1) : ce sont des **callbacks sortants**, pas des endpoints servis. Une garde qui balaie le
  document entier les compterait comme manquants et forcerait à les inscrire en `deferred`, ce qui
  serait un mensonge de plus. Le piège est vérifié : une première mesure les a comptés.
- **Une liste `deferred` non annotée devient un fourre-tout.** Ce qui donne sa valeur à la garde n'est
  pas le test, c'est l'obligation d'écrire une raison à côté de chaque ligne. Une entrée sans step ni
  raison doit être aussi visible qu'une opération non classée — c'est le seul moyen que la liste soit
  relue au lieu d'être allongée.
- **Le sort de `list-customer-accounts` (§ step-220) est une décision, pas une évidence.**
  `list-smpp-accounts` accepte déjà `customerId` **et** `groupId` en paramètres de requête : l'opération
  dédiée est fonctionnellement redondante. La retirer du contrat est une rupture que `oasdiff` classera
  `ERR` (bump majeur de `api/package.json`, dépôt du tableau de bord à prévenir). Trancher dans
  step-220, pas ici.
- **La garde ne doit pas figer le processus du dépôt.** CLAUDE.md impose que « le contrat se déclare
  **avant** l'implémentation » : un écart contrat → code est donc *normal* pendant une step. Ce que la
  garde interdit, c'est l'écart **non déclaré**. Une step qui ouvre un endpoint commence par l'inscrire
  en `deferred` et l'en retire quand elle le sert — deux lignes de diff qui rendent l'intention lisible
  en revue.

## Tests

- `D1`/`D2` : les deux gardes sont des tests ordinaires (pas de conteneur, pas de build tag) — elles
  lisent un YAML et un spec généré en mémoire.
- **Deux mutations doivent tomber, et elles sont différentes :**
  1. **ajouter** une opération au contrat sans toucher aux listes → la garde doit la nommer ;
  2. **retirer** une opération de `deferred` en la laissant au contrat → même verdict.
  La première seule ne prouve rien : une garde qui compare des tailles passe la première et rate la
  seconde. Précédent direct : step-201e a produit quatre tests creux, tous révélés par la mutation et
  aucun par la relecture.
- Vérifier que le triage est **exhaustif** en le recomptant depuis les sources, pas depuis la fiche :
  les 30 lignes de `deferred` doivent être exactement l'écart entre les `operationId` de `paths:` et les
  `OperationID` d'`internal/adminapi`.

## Definition of Done

- [ ] `make check` vert (lint · `test -race` · govulncheck · contrats)
- [ ] toute opération des deux contrats est classée : servie ou différée avec raison **et** step
- [ ] les deux mutations (ajout au contrat, retrait de `deferred`) ont été **vues** tomber
- [ ] aucune opération nouvellement servie, aucun contrat modifié

## Hors périmètre

**Un trou voisin, constaté et laissé ouvert :** la comparaison stricte
(`TestGeneratedSpecMatchesTheContractForEveryM1Operation`) porte sur l'`operationId`, les codes de
réponse et les schémas de requête/réponse — **pas sur les paramètres de requête**. Un `?groupId=`
déclaré au contrat et non lu par le handler passerait donc inaperçu. Le cas inverse existe déjà
(`list-smpp-accounts` implémente `groupId` alors qu'aucune surface ne permet d'assigner un groupe).
Le noter suffit ici ; l'élargissement de la comparaison est une step à part, dont le coût est le bruit
qu'elle produira sur 103 opérations.

L'implémentation des 30 → step-214 à step-220. Le retrait éventuel d'une opération du contrat, et le
bump majeur qui l'accompagne → la step qui la porte. La couverture de `api/collections/admin-api.yaml`
(déjà gardée par son propre test bloquant) et le versionnage du package npm (déjà couvert par
`make contracts`).
