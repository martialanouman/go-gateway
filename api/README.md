# Contrats API

Ce dossier est la **source de vérité** des interfaces de la passerelle. L'implémentation Go se
conforme à ces documents, jamais l'inverse.

| Fichier | Statut | Consommateur |
|---|---|---|
| `openapi-admin.yaml` | contrat | tableau de bord Admin (dépôt séparé), `admin-api-svc` |
| `openapi-public.yaml` | contrat | clients de la plateforme, `rest-api-svc` |
| `proto/` | contrat | services internes (gRPC) |
| `collections/admin-api.yaml` | **artefact dérivé** | outillage HTTP local |

`collections/admin-api.yaml` n'est pas un contrat : c'est une collection de requêtes qui **suit**
`openapi-admin.yaml`. Elle est gardée par `internal/adminapi/collection_test.go` et ne se modifie
jamais en premier.

## Le package publié

Le tableau de bord Admin vit dans un dépôt séparé. Pour qu'il ne puisse pas diverger, il ne copie
jamais ces fichiers : il consomme `@martialanouman/gateway-api-contracts`, publié sur GitHub
Packages par `.github/workflows/publish-api-contracts.yml` à chaque merge sur `main` touchant
`api/**`.

Le package contient les deux YAML bruts (pour alimenter un mock server côté consommateur) et les
types TypeScript générés par `openapi-typescript` :

```ts
import type { paths } from "@martialanouman/gateway-api-contracts/admin";
```

Les `.d.ts` sont générés à la publication, jamais commités.

Le package publié n'a **aucune dépendance d'exécution** : `openapi-typescript` et `typescript` sont
des `devDependencies` du générateur. `npm audit` y remonte des avis sur `js-yaml` (transitif via
`@redocly/openapi-core`, DoS quadratique sur des chaînes de merge-keys) que `npm audit fix` ne peut
pas résoudre sans casser la résolution amont. L'exposition est nulle : ce parseur ne lit que nos
propres contrats, au build, et n'atteint jamais un consommateur.

## Modifier un contrat

1. Éditer le YAML.
2. **Bumper `version` dans `api/package.json`** selon la nature du changement :

   | Bump | Quand |
   |---|---|
   | correctif | description, exemple, commentaire |
   | mineur | nouvelle opération, nouveau champ optionnel |
   | majeur | opération supprimée ou renommée, champ requis ajouté, type changé — tout ce que `oasdiff` classe `ERR` |

3. `make contracts` — refuse un contrat modifié sans bump, et une rupture qui n'est pas déclarée par
   un bump majeur.
4. `make contracts-types` — prouve que les YAML se génèrent en TypeScript valide. Cette cible
   demande Node ; elle n'est donc pas dans `make check`, mais elle tourne en CI sur chaque PR.
5. **Une opération neuve que la PR ne sert pas encore : l'inscrire en `deferred`.** Depuis step-320,
   une opération déclarée sous `paths:` que personne ne sert fait **échouer la suite** tant qu'elle
   n'est classée nulle part — c'est voulu : le tableau de bord génère ses clients depuis ce YAML, et
   une opération déclarée sans implémentation y devient un appel vers un 404. La liste vit dans
   `internal/adminapi/contract_test.go` (`deferred`, avec une raison **et** une step) ; côté public,
   dans `internal/restapi/conformance_test.go`. Déclarer le contrat avant l'implémentation reste la
   règle : ce qui est interdit, c'est l'écart **non déclaré**.
6. Implémenter côté Go pour se conformer, et mettre à jour la collection si l'Admin API a changé.
   L'opération sort alors de `deferred` et entre dans la surface servie — l'exclusion mutuelle des
   deux listes refuse qu'elle reste dans les deux.

Le consommateur n'a jamais à éditer un contrat : tout changement dont il a besoin passe par une PR
**ici**. Une fois le YAML mergé, il peut développer contre le mock sans attendre l'implémentation Go.
