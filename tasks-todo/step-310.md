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

- **Hérité de step-296.** `audit_log.operator` a un **troisième** format : `declared:<nom>`, écrit par
  `mt-replay`, qui n'a pas de principal. Cette step ne l'authentifie pas ; la dette vit dans
  `debts/rejeu-impute-a-une-identite-declaree.md`.

## Tests (écrits dans la même PR)
- Jeton OIDC valide → `Principal` + scopes ; jeton invalide/expiré → refusé.
- Mapping claims → scopes ; les endpoints Admin restent gardés comme avant (middleware inchangé).

## Design arrêté

Arbitrage Fable, 2026-09-26 (F1-F9), sans conflit avec la spec ; F1 confirmé par l'utilisateur. ADR-0017
fixe l'appelant : l'Admin API authentifie un **jeton de service** (BFF, script, collection), jamais l'humain.

- **Vérificateur** : `auth.OIDCVerifier` derrière `TokenVerifier`, sur `github.com/coreos/go-oidc/v3`
  (`NewRemoteKeySet` + `NewVerifier`, lib figée par `ctx7`). Pas de discovery : l'URL JWKS est configurée,
  les clés se chargent paresseusement, et le service boote et passe readiness sans l'IdP. La rotation sur
  `kid` inconnu est celle de `RemoteKeySet`.
- **Algorithmes** : `RS256` et `ES256`, figés. `typ: at+jwt` n'est pas exigé (SHOULD côté émetteur, absent
  chez Keycloak) : le contrôle d'audience écarte déjà un ID token.
- **Scopes** : claim `scope`, chaîne séparée par espaces (RFC 9068 §2.2.3). `scp` n'est pas lu. Un scope
  inconnu est ignoré, parce que les IdP en greffent par défaut. Un jeton valide sans scope connu donne un
  403, pas un 401.
- **Subject** : `sub` brut. `tok_` et `declared:` se distinguent déjà seuls. `client_id`/`azp` est
  réassignable, `sub` est l'identité stable. Un `sub` vide donne un 401.
- **IdP injoignable → 503** : le client JWKS a un timeout de 5 s. `Verify` rend `ErrServiceUnavailable`
  quand les clés n'ont pas pu être chargées, et en journalise la cause. Le middleware gagne une branche 503.
  *(Revue : `IDTokenVerifier` aplatit l'erreur du KeySet en `%v`, et une page non-JSON ou un corps tronqué
  échappaient à `*url.Error`. La classification se fait dans un adaptateur de KeySet : go-oidc enveloppe
  tout échec de rechargement en `fetching keys %w`, et l'échec « aucune clé ne vérifie » est un `errors.New`
  sans cause. Le `RoundTripper` qui changeait un statut ≠ 200 en erreur disparaît ; une redirection est
  désormais suivie, sauf de https vers http. Limites acceptées : un 200 JSON sans clés (`{}`) reste un 401,
  et un appelant qui raccroche pendant un rechargement journalise un faux « IdP indisponible ».)*
  Un 401 ferait prendre au BFF un hoquet d'IdP pour une session morte. Le contrat n'est pas touché : le
  503 d'infrastructure n'est énuméré par aucune opération, comme le 500.
- **Configuration** : `OIDC_ISSUER`, `OIDC_AUDIENCE`, `OIDC_JWKS_URL`, dans une section déclarée par le seul
  admin-api-svc. La section refuse une configuration partielle partout, et une valeur entourée de blancs
  (l'issuer est comparé octet pour octet). En production, elle exige les trois et une URL JWKS en `https`.
  `validateAdminConfig` refuse un `HTTP_ADMIN_TOKENS` non vide à côté d'un IdP, où il n'aurait aucun effet.
- **StaticVerifier** : conservé **hors production** seulement. Le dev local (collection, README) n'a aucun
  IdP. La DoD « stub M1 retiré » se lit « retiré de la production ».
- **Manifests** : `deploy/k8s/admin-api-svc.yaml` troque `HTTP_ADMIN_TOKENS` contre les `OIDC_*` dans cette
  PR, sinon la production refuse de booter.
- **`tok_` ↔ `sub`** : pas de table. Seul le détenteur du jeton en clair peut relier l'empreinte, et la
  continuité « même opérateur » vit côté BFF (ADR-0017). L'historique reste `tok_`, ce que dit la godoc
  de `Fingerprint`.
- **Contrat** : inchangé. `OperatorBearer` dit déjà JWT + clientCredentials. Le vrai `tokenUrl` est un
  fait de déploiement, pour la checklist de step-410.
- **Dettes nées ici** : `debts/created-by-jamais-renseigne.md`, `debts/jwks-recharge-sans-limite-de-frequence.md`,
  `debts/jwks-joint-par-les-seules-racines-systeme.md`.
- **RFC 8705 (liaison au certificat)** : rien. Le mTLS de step-300 lie déjà l'appelant à un certificat.
  Déclencheur : plusieurs certificats clients admis avec des privilèges différents.

## Definition of Done
- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] critères couverts par tests · godoc sur l'exporté · aucun invariant (a/b/c/d) violé
- [ ] auth opérateur réelle active ; stub M1 retiré de la production ; validation OIDC via lib figée par `ctx7`

## Hors périmètre
Manifests k8s → step-270. Checklist prod → step-410.
