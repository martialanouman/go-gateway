# step-089 — Contrats API publiés comme package versionné + garde de rupture

> **Jalon :** transverse (infrastructure, hors M0→M12) · **Statut :** À FAIRE
> **Dépend de :** — · **Bloque :** démarrage du dépôt tableau de bord

## But
Rendre impossible la divergence entre `api/openapi-*.yaml` et le tableau de bord Admin, qui vit dans
un dépôt séparé. `go-gateway` reste propriétaire unique des contrats et les publie comme package npm
versionné ; le consommateur les prend en dépendance, jamais en copie.

## Périmètre (ce que fait CETTE PR)
- `api/package.json` + `api/tsconfig.json` : package `@martialanouman/gateway-api-contracts` —
  les deux YAML bruts et les types générés par `openapi-typescript` ; `.d.ts` générés à la
  publication, jamais commités.
- `scripts/check-contracts.sh` : garde de cohérence — contrat modifié sans bump de version → échec ;
  rupture `oasdiff` sous un bump non-majeur → échec.
- `.github/workflows/publish-api-contracts.yml` : publication idempotente sur GitHub Packages, sur
  merge `main` touchant `api/**`.
- `ci.yml` : jobs `contracts` (la garde) et `contracts-types` (génération + typecheck).
- `Makefile` : `contracts` (dans `check`) et `contracts-types` ; `oasdiff` dans `tools`.
- `api/README.md` : statut des fichiers, règle de bump, procédure de modification.

## Points d'implémentation clés
- `collections/admin-api.yaml` est un **artefact dérivé**, pas un contrat : hors package, déjà gardé
  par `internal/adminapi/collection_test.go`.
- La version du package est **indépendante du tag Go** : `release.yml` tague à chaque feat/fix, ce
  qui n'a aucun rapport avec la cadence du contrat.
- La garde n'interdit pas les ruptures — elle exige qu'elles soient **déclarées** par un bump majeur,
  que le dépôt consommateur verra passer.
- `openapi-typescript@7.13.0` exige `typescript@^5.x` (versions figées, pas de `^` : un package
  publié ne se dépublie pas).
- `contracts-types` hors de `make check` : sinon Node devient prérequis de tout contributeur Go.

## Tests (écrits dans la même PR)
Garde éprouvée sur branches jetables, les quatre cas :
- champ optionnel ajouté sans bump → échec (bump manquant) ;
- même changement + `1.1.0` → passe ;
- opération supprimée + `1.1.0` → échec (rupture sous bump mineur) ;
- même suppression + `2.0.0` → passe.

## Definition of Done
- [ ] `make contracts` vert · `make contracts-types` vert (types générés, `tsc --noEmit` propre)
- [ ] `go test -race ./...` inchangé et vert
- [ ] les quatre cas de la garde vérifiés · `api/README.md` et `CLAUDE.md` à jour
- [ ] après merge : package visible dans GitHub Packages, re-run du workflow idempotent

## Hors périmètre
La création du dépôt tableau de bord et son plan d'exécution. La configuration Renovate côté
consommateur. La génération de `collections/admin-api.yaml` depuis l'OpenAPI (amélioration réelle,
sans rapport avec la synchro inter-dépôts).
