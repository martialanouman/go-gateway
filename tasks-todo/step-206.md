# step-206 — Auth opérateur réelle (OIDC/mTLS) remplaçant le stub M1

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-205 · **Bloque :** —

## But
Remplacer le vérificateur de jetons statiques de M1 (`internal/auth/static.go`) par une **authentification
opérateur réelle** (OIDC, adossée au mTLS), tout en conservant le modèle de scopes et le middleware
existants.

## Périmètre (ce que fait CETTE PR)
- `internal/auth/` : nouveau `Verifier` OIDC (validation de jeton, mapping claims → `Principal`/scopes).
- Retrait/retrait progressif de `StaticVerifier` (conçu pour être « remplacé en bloc à M12 » — voir sa
  godoc). Câblage dans `cmd/admin-api-svc`.
- Config OIDC (issuer, audience, JWKS) via `internal/config` ; `AdminTokens` (stub M1) retiré/déprécié.

## Points d'implémentation clés
- Le `StaticVerifier` documente explicitement son remplacement à M12 — respecter l'interface `Verifier`
  déjà consommée par `auth.Middleware` pour que le reste de l'Admin API ne change pas.
- **`ctx7`** avant d'ajouter une lib OIDC/JWKS (validation de signature, rotation de clés) — ne pas rouler
  sa propre validation JWT.
- Scopes opérateur inchangés (mêmes `Scope`) ; mapping depuis les claims du fournisseur d'identité.
- Comparaisons/erreurs sans fuite ; adossé au mTLS de step-205.

## Tests (écrits dans la même PR)
- Jeton OIDC valide → `Principal` + scopes ; jeton invalide/expiré → refusé.
- Mapping claims → scopes ; les endpoints Admin restent gardés comme avant (middleware inchangé).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] auth opérateur réelle active ; stub M1 retiré ; validation OIDC via lib figée par `ctx7`

## Hors périmètre
Manifests k8s → step-207. Checklist prod → step-208.
