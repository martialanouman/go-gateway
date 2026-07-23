# step-027 — Rotation d'identifiant de bind avec fenêtre de grâce

> **Jalon :** M3 (§7 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-024 · **Bloque :** —

## But
Honorer au bind la fenêtre de grâce d'une rotation de secret : l'ancien mot de passe reste valide jusqu'à `grace_expires_at`, puis est coupé.

## Périmètre (ce que fait CETTE PR)
- Étendre l'auth bind (step-024) : après échec sur `password_hash`, tenter `previous_secret_hash` **si** `grace_expires_at > now` (colonnes déjà au schéma `control_plane.credentials`).
- Étendre la requête sqlc `GetBindCredentialBySystemID` pour ramener `previous_secret_hash`/`grace_expires_at`.
- L'endpoint Admin `rotate-credential` (déjà déclaré `api/openapi-admin.yaml`) : vérifier/compléter qu'il pose `previous_secret_hash`, `grace_expires_at`, `rotated_at` (paramètre `gracePeriodSec`). Si non implémenté, l'implémenter ici côté `adminapi`.

## Points d'implémentation clés
- **Temps constant** sur les deux comparaisons (§1.9) ; la tentative sur l'ancien hash suit la même discipline.
- Après `grace_expires_at`, l'ancien hash ne doit **jamais** authentifier (test explicite).
- Nettoyage optionnel des colonnes de grâce expirées (best-effort, non bloquant).
- Secret révélé une seule fois à la rotation, stocké en hash (règle d'or secrets).

## Tests (écrits dans la même PR)
- Rotation avec `gracePeriodSec` : ancien secret valide pendant la fenêtre, refusé après (test avec horloge injectée / `now` mockée).
- Nouveau secret valide immédiatement.
- Intégration PG : `rotate-credential` pose bien les colonnes.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] coupure post-grâce prouvée par test

## Hors périmètre
Rotation des clés API REST (même mécanisme, hors ce step) ; anti-brute-force (step-026).
