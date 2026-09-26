# step-405 — La passerelle accepte les jetons du BFF : ancre du JWKS, contrat honnête, `created_by` rempli

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** À FAIRE
> **Dépend de :** step-310, ADR-0019 (`Accepted`) · **Bloque :** step-410

## Pourquoi cette fiche existe

ADR-0019 fait du BFF l'émetteur des jetons de l'API Admin, au nom de l'opérateur connecté. step-310 vérifie
déjà des JWT contre un JWKS configuré. Trois choses manquent pourtant côté passerelle pour que le schéma
fonctionne en production, et aucune step ne les porte :

1. **Le JWKS du BFF sera servi sous une autorité interne.** Le client qui le charge
   (`internal/auth/oidc.go`, `keySetClient`) ne connaît que les racines système. Chaque jeton recevrait donc
   503. `debts/jwks-joint-par-les-seules-racines-systeme.md` est bloquante pour le go-live depuis ADR-0019.
2. **Le contrat ment sur le flux.** `OperatorBearer` déclare `clientCredentials` avec un `tokenUrl`
   placeholder (`https://admin.gateway.internal/oauth/token`) qu'aucun service ne sert. Le tableau de bord
   génère ses clients depuis ce contrat.
3. **`created_by` reste nul**, alors que `sub` porte désormais `dashboard.operators.id`.
   `debts/created-by-jamais-renseigne.md` est payable.

## Périmètre (ce que fait CETTE PR)

- **Ancre du JWKS** : `OIDC_JWKS_CA_FILE`, sur le modèle `*_TLS_CA_FILE` de step-305. Réutiliser
  `tlsconf.StoreClientConfig(caFile)` : une CA vide garde les racines système, une CA renseignée devient le
  seul pool, avec un plancher TLS 1.2. Une CA illisible fait échouer le boot, avec une erreur rendue. Le
  manifest d'admin-api-svc monte la CA, et la lecture passe par le ConfigMap `gateway-oidc` ou par un volume
  (à trancher).
- **Contrat** : la description d'`OperatorBearer` dit que le jeton est émis par le BFF du tableau de bord
  (ADR-0019), et en donne les claims attendus (`iss`, `aud`, `sub`, `scope` en chaîne, `exp`). Le contrat est
  déclaré **avant** l'implémentation, avec un bump MINEUR de `api/package.json`
  (`.claude/rules/contracts-api.md`).
- **`created_by`** : les créations qui portent la colonne (groupes de clients, scripts de routage et leurs
  versions, règles de réécriture, sender IDs) écrivent le `Subject` du principal quand c'est un uuid. Il ne
  s'écrit **jamais** pour `tok_…` (hors production) ni pour `declared:…` (`mt-replay`).

## Points d'implémentation clés

- **Tranché (utilisateur, 2026-09-26) : la FK `created_by → dashboard.operators(id)` disparaît.** Le BFF a sa
  propre base (spec tableau de bord §3.1 : « schéma PostgreSQL 18 séparé »), et la passerelle ne doit pas
  dépendre des tables d'un autre service. La table `dashboard.operators` n'est qu'un stub qui n'existe que
  pour satisfaire ces FK (`db/schema_passerelle_sms.sql:51-59`) : aucun opérateur réel n'y figurera jamais,
  donc chaque écriture d'un `sub` réel échouerait. La migration retire les quatre FK (`:77`, `:375`, `:497`,
  `:581`), puis le stub, puis le schéma `dashboard` s'il est vide. Le schéma **et** la migration changent
  ensemble (`.claude/rules/db-schema.md`). `created_by` reste un uuid sans référence : c'est l'`operator_id`
  du BFF, et c'est au BFF de le traduire en nom, comme ADR-0017 lui confie l'humain.
  `internal/storage/postgres/queries/customer_groups.sql:3` justifie la colonne par cette FK : corriger le
  commentaire, puis régénérer sqlc.

- **À arbitrer avant tout code (échelle : spec → Fable → humain)** :
  - La forme de l'ancre dans le manifest : fichier monté, ou PEM dans le ConfigMap.
  - Un `sub` qui n'est pas un uuid, venu d'un émetteur mal configuré : écrire NULL, ou refuser l'appel ?
- `internal/controlplane/customergroup.go:23` annonce encore « the operator identity arrives with step-310 » :
  corriger ce commentaire quand `created_by` sera câblé.
- Le résidu de step-310 est à fermer ici si c'est possible : l'usage de `keySetClient()` par
  `NewOIDCVerifier` n'est pas prouvé. Avec une ancre configurable, un test https de bout en bout (httptest
  TLS + CA de `tlstest`) peut enfin le prouver, y compris le refus d'une redirection de https vers http.

## Tests (écrits dans la même PR)

- Un JWKS servi sous une CA de `tlstest` est lu avec `OIDC_JWKS_CA_FILE`, et refusé (503) sans elle.
- Une CA illisible fait échouer le boot, avec une erreur rendue.
- Une création sous un jeton OIDC écrit `created_by` = `sub`. Sous `tok_…` ou `declared:…`, elle écrit NULL.
- Le contrat : la garde de contrat existante reste verte après le bump.

## Definition of Done

- [ ] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [ ] `debts/jwks-joint-par-les-seules-racines-systeme.md` passe à `PAYÉE`, avec la date et la PR
- [ ] `debts/created-by-jamais-renseigne.md` passe à `PAYÉE` ; FK et stub `dashboard.operators` retirés
  (schéma + migration)
- [ ] contrat `OperatorBearer` à jour, `api/package.json` bumpé en MINEUR
- [ ] le résidu `keySetClient()` de step-310 est prouvé, ou sa raison est écrite

## Hors périmètre

L'émission des jetons, le JWKS, la rotation et la traduction permissions → scopes : dépôt du BFF
(ADR-0019, action 4). La limite de fréquence des rechargements du JWKS :
`debts/jwks-recharge-sans-limite-de-frequence.md`.
