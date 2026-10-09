# step-287h — L'environnement de test passe en OIDC : l'Admin API accepte les JWT du BFF

> **Jalon :** M12 · **Statut :** À FAIRE
> **Dépend de :** step-287 (fin de la campagne : `main` gelée pendant les runs), step-310 (ADR-0019)
> Demande humaine du 09/10/2026 : faire passer la tranche réelle par le BFF. Unité faute de multiple de dix libre.

## Pourquoi
Pour que la tranche réelle passe, contabo169 doit accepter les JWT du BFF. Une bascule faite à la main ne
tient pas :
- **`admin-api-svc` n'accepte qu'un mode à la fois.** Avec `OIDC_ISSUER` rempli, il refuse de démarrer si
  `HTTP_ADMIN_TOKENS` n'est pas vide (`cmd/admin-api-svc/wiring.go:497`).
- **Chaque déploiement échouerait.** `test-seed` et `smoke` appellent l'Admin API avec le premier jeton de
  `HTTP_ADMIN_TOKENS` (`cmd/test-env/main.go:54`). Sans lui, ils s'arrêtent ; avec lui, admin-api-svc ne
  démarre pas. Le smoke tombe, donc « Deploy test » aussi.
- **Le déploiement suivant défait la bascule.** L'overlay réapplique `deploy/test/gateway-oidc.yaml` (vide) et
  réinjecte `HTTP_ADMIN_TOKENS` (`patches/admin-api-svc.yaml`) ; `check.sh` exige ce dernier.
- **La campagne de charge ne peut plus semer de clients.** `run.sh seed` (Job `seed-load`) passe par le
  même jeton. Les clés déjà semées restent valides : l'API REST ne passe pas par l'Admin API.
- **L'accès exploitant change.** Le jeton de `~/.config/go-gateway-test/admin-token` ne sert plus ; il faut un
  JWT du BFF (README §8).

## À trancher avant le design
- L'URL de l'émetteur et des JWKS du BFF, l'audience, et l'autorité de son certificat (`OIDC_JWKS_CA_FILE`).
- Comment un Job obtient un jeton : identifiants client du BFF rangés dans `gateway-secrets`, ou autre.
- Le sort de `bootstrap-secrets.sh` (il tire aujourd'hui `HTTP_ADMIN_TOKENS` et écrit `admin-token`).

## Definition of Done
- [ ] `gateway-oidc.yaml` rempli, `HTTP_ADMIN_TOKENS` retiré de l'overlay, `check.sh` garde l'inverse
- [ ] `test-seed`, `smoke` et `seed-load` obtiennent un jeton du BFF ; « Deploy test » vert
- [ ] README §5 et §8 : secrets et accès exploitant par le BFF
