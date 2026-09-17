# step-310 — Auth opérateur réelle (OIDC/mTLS) remplaçant le stub M1

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-300 · **Bloque :** —

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
- **Hérité de step-290b.** `auth.Fingerprint` vit dans `auth.go`, pas dans le stub : il ne part pas avec
  `StaticVerifier`, parce que la migration 0014 et son test d'intégration en dépendent. Après le passage
  à OIDC, `Subject` vaudra le `sub` du jeton. Les colonnes `operator` (trois tables, plus
  `audit_log` après 290c) porteront donc deux formats : `tok_…` pour l'historique, et `sub` pour la
  suite. Aucune table ne relie une empreinte à un `sub` : décider ici si cette correspondance est
  nécessaire, par exemple pour qu'un auditeur retrouve un même opérateur avant et après la bascule.
- Le `StaticVerifier` documente explicitement son remplacement à M12 — respecter l'interface `Verifier`
  déjà consommée par `auth.Middleware` pour que le reste de l'Admin API ne change pas.
- **`ctx7`** avant d'ajouter une lib OIDC/JWKS (validation de signature, rotation de clés) — ne pas rouler
  sa propre validation JWT.
- Scopes opérateur inchangés (mêmes `Scope`) ; mapping depuis les claims du fournisseur d'identité.
- Comparaisons/erreurs sans fuite ; adossé au mTLS de step-300.

## Tests (écrits dans la même PR)
- Jeton OIDC valide → `Principal` + scopes ; jeton invalide/expiré → refusé.
- Mapping claims → scopes ; les endpoints Admin restent gardés comme avant (middleware inchangé).

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] auth opérateur réelle active ; stub M1 retiré ; validation OIDC via lib figée par `ctx7`

## Hors périmètre
Manifests k8s → step-270. Checklist prod → step-410.
