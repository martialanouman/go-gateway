# step-260h — Trois gardes passent de la prose au code

> **Jalon :** Audit du 2026-09-03 (correctifs) · **Statut :** EN COURS (2026-09-05)
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

- Le catalogue (`internal/platform/errors/errors.go:141-181`) compte **26** codes ; 4 codes d'issue
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
Test `TestErrorCodeEnumMatchesTheCatalogue` dans `internal/adminapi/contract_test.go` (marche l'arbre
générique) et `internal/restapi/conformance_test.go` (`contractDoc` gagne `Components`) : ensemble de
l'enum == `{c ∈ errs.Codes() : HTTPStatus(c) ok}`, les deux sens. `.claude/rules/errors.md` point 2 et
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
   d'enum ». Vert par le YAML. Mutations : un code ajouté au catalogue Go (HTTP 418) sans le YAML →
   tombe ; une valeur retirée de l'enum → tombe ; `make contracts` sur la branche confirme le mineur.
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

- [ ] `make check` vert
- [ ] un code ajouté au catalogue Go sans le contrat fait rouge (mutation 1)
- [ ] une erreur de compilation sous `loadref` fait rouge (preuve 2)
- [ ] `make check` échoue explicitement au lieu de sauter quand une dépendance manque (preuve 3)

## Hors périmètre

La garde contrat ⊆ code (step-320). Un enum sur `Message.error_code` (issues sortantes) : suite
possible. Faire appeler `make fmt-check`/`make tidy-check` par `ci.yml` à la place de ses scripts
inline : suite possible, la CI ne change pas dans cette PR.
