# step-405 — La passerelle accepte les jetons du BFF : ancre du JWKS, contrat honnête, `created_by` rempli

> **Jalon :** M12 (§16 `docs/plan-execution-passerelle.md`) · **Statut :** LIVRÉE (#254)
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

## Design arrêté

Arbitrage Fable du 2026-10-04 sur les deux points ouverts. La spec ne tranchait ni l'un ni l'autre ; seule
contrainte : une variable TLS porte un CHEMIN, jamais du PEM (`internal/config/config.go`, type `TLS`).

**Ancre : une clé de plus dans `gateway-oidc`, aucun volume neuf.** `OIDC_JWKS_CA_FILE` est lue par
`configMapKeyRef` comme les trois autres, vide dans `deploy/test/gateway-oidc.yaml` (le ConfigMap est lu
sans `optional`, la clé doit exister). ADR-0019 sert le JWKS « sous une autorité interne » : c'est la PKI de
step-300, déjà montée en `/etc/gateway/tls/ca.crt`. step-410 y écrit ce chemin. Une autorité tierce
exigerait un montage ajouté par l'exploitant, comme `KAFKA_TLS_CA_FILE` de step-305 ; une ligne au tableau
de `deploy/k8s/tls/README.md` le dit. Un volume dédié serait spéculatif ; du PEM dans le ConfigMap, du bruit.
`config.Load` refuse `OIDC_JWKS_CA_FILE` sans `OIDC_ISSUER` (« would have no effect »), comme Kafka et
ClickHouse.

**Client du JWKS.** `NewOIDCVerifier` prend le fichier de CA et rend une erreur : il appelle
`tlsconf.StoreClientConfig`, et le transport du `keySetClient` est un clone de `http.DefaultTransport` qui
porte cette config. Une CA illisible remonte de `newVerifier` et fait échouer `newAdminApp`, donc le boot.
Vide, les racines système restent, avec le plancher TLS 1.2 de `StoreClientConfig`. Le résidu de step-310
se ferme par un test https de bout en bout (`httptest` TLS sous une CA `tlstest`) : accepté avec l'ancre,
503 sans elle, 503 sur une redirection de https vers http.

**Un `sub` qui n'est pas un uuid est refusé par le vérifieur.** `OIDCVerifier.Verify` rend
`ErrUnauthenticated` et un WARN qui nomme `iss` et `sub` (des identifiants, pas des secrets). ADR-0019 fait
de `sub` un `operator_id` pour tout jeton du BFF : un autre format est un émetteur mal configuré, une panne
de déploiement qui doit se voir à la première requête. Écrire NULL rouvrirait en silence la dette que cette
step paie ; ne refuser que les créations donnerait « lectures OK, créations 401 » pour une seule faute.

**`created_by` = `uuid.Parse(principal.Subject)`, nil sinon.** Un seul helper dans `internal/adminapi`,
appelé par les quatre handlers de création (groupes, scripts — une version EST une ligne de script —,
règles de réécriture, sender IDs). Ce sont les seuls écrivains de ces tables. `tok_…` ne se parse pas, donc
NULL ; `declared:…` (`mt-replay`) ne passe jamais par l'API Admin et ne se parserait pas non plus. Les
requêtes sqlc des groupes et des règles de réécriture prennent la colonne (les scripts et les sender IDs la
prennent déjà), puis sqlc se régénère.

**Schéma.** Migration `0027` : retrait des quatre FK, de `dashboard.operators`, puis du schéma `dashboard`.
La down les recrée, FK en `NOT VALID` : des `created_by` réels ne référencent aucun opérateur du stub, et
une FK validée rendrait la down impossible.

**Contrat.** La description d'`OperatorBearer` dit que le BFF émet le jeton (ADR-0019), que le `tokenUrl`
n'est servi par personne et ne reste que parce qu'un flux OAuth2 l'exige, et donne les claims (`iss`, `aud`,
`sub` uuid, `scope` en chaîne séparée par des espaces, `exp`). Le type ne change pas (ADR-0019 : ce serait
une rupture). **En plus de la fiche :** le schéma déclare `cdr:export_bulk`, que des opérations exigent déjà
sans que le schéma le liste ; c'est additif. Bump MINEUR `6.11.0 → 6.12.0`. Le miroir Go
(`operatorSecurityScheme`) porte la même description.

## Tests (écrits dans la même PR)

- Un JWKS servi sous une CA de `tlstest` est lu avec `OIDC_JWKS_CA_FILE`, et refusé (503) sans elle.
- Une CA illisible fait échouer le boot, avec une erreur rendue.
- Une création sous un jeton OIDC écrit `created_by` = `sub`. Sous `tok_…` ou `declared:…`, elle écrit NULL.
- Le contrat : la garde de contrat existante reste verte après le bump.

## Definition of Done

- [x] gofmt/goimports · golangci-lint · `go test -race ./...` · govulncheck verts
- [x] `debts/jwks-joint-par-les-seules-racines-systeme.md` passe à `PAYÉE`, avec la date et la PR
- [x] `debts/created-by-jamais-renseigne.md` passe à `PAYÉE` ; FK et stub `dashboard.operators` retirés
  (schéma + migration)
- [x] contrat `OperatorBearer` à jour, `api/package.json` bumpé en MINEUR
- [x] le résidu `keySetClient()` de step-310 est prouvé, ou sa raison est écrite

## Hors périmètre

L'émission des jetons, le JWKS, la rotation et la traduction permissions → scopes : dépôt du BFF
(ADR-0019, action 4). La limite de fréquence des rechargements du JWKS :
`debts/jwks-recharge-sans-limite-de-frequence.md`.
