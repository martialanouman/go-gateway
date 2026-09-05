# step-260h — Trois gardes passent de la prose au code

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** LIVRÉE (2026-09-05)
> **Dépend de :** — · **Bloque :** —

## Pourquoi cette fiche existe

L'audit du 2026-09-03 a trouvé trois gardes qui n'existent que dans un document :

1. `.claude/rules/errors.md` et le guide §11.2 disent que « l'énumération de référence » des codes
   d'erreur vit dans le champ `code` des deux OpenAPI. Les deux contrats déclarent `code: type: string`
   avec des `examples` — **aucun `enum`**, et rien ne compare le contrat au catalogue Go.
2. Neuf fichiers `internal/e2e/*_test.go` sont sous `//go:build loadref`. Ni `golangci-lint` ni `go vet`
   ne les compilent en CI : une erreur de compilation y resterait invisible jusqu'à la prochaine
   campagne de charge.
3. `ci.yml:1-2` et la DoD de `CLAUDE.md` promettent « `make check` vert local = CI verte ». Faux sur
   trois points : `make check` ne pose pas `CI`, donc un test d'intégration qui saute passe (c'est
   exactement le trou que `ciguard` ferme en CI) ; `gofmt -l` et `go mod tidy` sont vérifiés en CI et
   pas par `make check`.

La garde contrat ⊆ code reste dans step-320 (décision du 2026-09-03).

## Ce que l'exploration a établi

- Le catalogue (`internal/platform/errors/errors.go:141-179`) compte **26** codes ; 4 codes d'issue
  sortante (`delivery_failed`, `delivery_expired`, `fallback_exhausted`, `retries_exhausted`) sont des
  constantes **hors catalogue**, gardées ainsi par `errors_test.go:108-127`. Parmi les 26, **24** ont un
  statut HTTP (`errs.HTTPStatus` ok) ; `max_sessions_exceeded` (bind SMPP seulement, §11.3 « — ») et
  `submit_failed` (issue sortante, `cdr.error_code`) n'en ont pas. Le plan disait « 25 » : il comptait
  `max_sessions_exceeded`, que la spec §11.3 elle-même exclut de REST.
- Le champ est `components.schemas.Error.properties.code` dans les deux contrats
  (`openapi-public.yaml:348`, `openapi-admin.yaml:1687`). Le test Admin charge le YAML en arbre
  générique (`contract_test.go:152`) ; le test public en projection typée `contractDoc` sans
  `components` (`conformance_test.go:16-26`).
- Verdict `oasdiff` mesuré pendant la planification (enum ajouté au `code` Admin) : `0 error`, warnings
  `response-property-enum-value-added` seulement → bump **mineur** `4.0.3 → 4.1.0` (`api/README.md:47`).
- `golangci-lint run --build-tags loadref ./internal/e2e/...` et `go vet -tags=loadref` passent
  aujourd'hui : la garde est gratuite maintenant.
- `go mod tidy -diff` existe (Go 1.26) et passe. La CI vérifie `gofmt -l .` et `go mod tidy` + diff
  (`ci.yml:41-55`).
- Sur ce poste, `/var/run/docker.sock` est un lien vers le socket OrbStack : Docker est joignable
  même sans `DOCKER_HOST`, et un `DOCKER_HOST` faux n'empêche pas les tests de tourner. La preuve
  « `make check` échoue au lieu de sauter » passe donc par `-short`, le chemin `ciguard.Skip` que tous
  les helpers prennent (`redistest.go:59`, `smscsim.go:53`).

## Design arrêté

**A. L'enum du champ `code`.** Dans les deux `Error.code` : `enum:` = les **24 codes à surface HTTP**,
triés. La description dit que les codes sans surface REST (`max_sessions_exceeded`, les issues
sortantes de `cdr.error_code`) n'y figurent pas. `examples` conservés. `api/package.json` → `4.1.0`.
`errs.HTTPCodes()` est cette liste, calculée depuis le catalogue. **Le YAML seul ne suffit pas** : le
test strict `TestGeneratedSpecMatchesTheContractForEveryM1Operation` compare le schéma résolu de chaque
réponse d'erreur, `enum` compris, donc le modèle servi doit le déclarer aussi — `humaerr.Model`
implémente `huma.SchemaTransformer` et pose l'enum sur `code` depuis `HTTPCodes()` ; servi et déclaré ne
peuvent plus diverger. Les trois constructeurs de `humaerr` (`newError`, `Fail`, `FromError`) rendent un
code sans surface HTTP en `internal_error`/500 : un tel code ne peut plus atteindre un corps hors enum.
Tests `TestErrorCodeEnumMatchesTheCatalogue` dans `internal/adminapi/error_enum_test.go` (marche l'arbre
générique) et `internal/restapi/error_enum_test.go` (`contractDoc` gagne `Components`) : ensemble de
l'enum == `errs.HTTPCodes()`, les deux sens. `.claude/rules/errors.md` point 2 et
guide §11.2 : « l'énumération de référence » devient « l'enum des codes à surface HTTP, gardée par
`TestErrorCodeEnumMatchesTheCatalogue` ; le catalogue complet est le tableau §11.3 ».

**B. `loadref` en CI.** `.golangci.yml` `run.build-tags: [loadref]`. `make lint` reste
`golangci-lint run` : local = CI.

**C. `make check` dit ce qu'il prouve.** Deux cibles neuves, `fmt-check` (`gofmt -l`) et `tidy-check`
(`go mod tidy -diff`), miroirs des étapes CI ; `check: lint fmt-check tidy-check vuln contracts` puis
`CI=1 $(MAKE) test`. `make test` seul garde son comportement de portable (saut autorisé). Le commentaire
de `ci.yml:1-2` et la DoD de `CLAUDE.md` disent ce que `make check` exige (Docker + `make smsc-sim`) et
ce qu'il ne couvre pas (`contracts-types`, `load-smoke`, `migrate`).

## Chaîne de preuves

1. Rouge lu : les deux `TestErrorCodeEnumMatchesTheCatalogue` échouent sur « le contrat n'a pas
   d'enum ». Le YAML les rend verts et rend **rouge** le test strict Admin (drift `enum` sur chaque
   réponse d'erreur) : c'est ce rouge qui a imposé le `SchemaTransformer`. Mutations : un code ajouté au
   catalogue Go (HTTP 418) sans le YAML → les deux tests d'enum et le strict tombent ; une valeur retirée
   de l'enum → tombe ; `HTTPCodes` renvoyant tout → `TestHTTPCodesAreTheCatalogueEntriesWithAStatus`
   tombe ; `Fail(submit_failed)` rendait `submit_failed` dans le corps → rouge lu sur les trois
   constructeurs, puis vert. `make contracts` : « Aucune rupture », mineur confirmé.
2. Une erreur de compilation dans `internal/e2e/reference_test.go` → `make lint` rouge ; retirée.
3. `GOFLAGS=-short make check` → rouge au premier `ciguard.Skip` (CI=1) ; `GOFLAGS=-short make test` →
   vert avec sauts. Un fichier non formaté → `make fmt-check` rouge.
4. `make check` vert ; `make contracts-types` vert (le TS gagne une union).

## Commits

1. Cette fiche.
2. `api` : enum + description dans les deux contrats, `package.json` 4.1.0.
3. `adminapi`, `restapi` : les deux tests d'enum ; `errors.md`, guide §11.2.
4. `lint` : `build-tags: [loadref]`.
5. `make` : `fmt-check`, `tidy-check`, `check` sous `CI=1` ; `ci.yml`, `CLAUDE.md`.
6. Fiche → `tasks-done/`.

## Definition of Done

- [x] `make check` vert (85 paquets ; une première passe a refusé trois `prealloc` dans les tests neufs)
- [x] un code ajouté au catalogue Go sans le contrat fait rouge (mutation 1, vue tomber)
- [x] une erreur de compilation sous `loadref` fait rouge : avec `build-tags`, 2 findings ; sans, 0
- [x] `make check` échoue au lieu de sauter : `GOFLAGS=-short make check` tombe sur 39 paquets avec
      « this test may not be skipped in CI » ; `GOFLAGS=-short make test` reste vert (85 ok)
- [x] `make fmt-check` rouge sur un fichier non formaté, vert sur l'arbre ; `make contracts-types` vert,
      le TypeScript gagne une union sur `code`

## Revue

Un sous-agent en lecture seule, aucun bloquant. Deux Required corrigés dans la PR : la fiche décrivait
« vert par le YAML » alors que le YAML seul rougit le test strict (d'où le `SchemaTransformer`, qui
n'était pas dans le design) ; et `Fail`/`FromError`/`newError` laissaient un code sans surface HTTP
sortir en 500 avec ce code dans le corps — hors de l'enum que le contrat venait de publier. Aucun
appelant ne le faisait ; la porte est fermée et testée. Nits retenus : commentaire narratif retiré,
`v.(string)` remplacé par un `Fatalf`, test de `HTTPCodes` recentré sur une assertion non circulaire.

## Hors périmètre

La garde contrat ⊆ code (step-320). Un enum sur `Message.error_code` (issues sortantes) : suite
possible. Faire appeler `make fmt-check`/`make tidy-check` par `ci.yml` à la place de ses scripts
inline : suite possible, la CI ne change pas dans cette PR.
