# ADR-0019 : Le BFF du tableau de bord émet les jetons de l'API Admin, au nom de l'opérateur connecté

**Status:** Accepted
**Date:** 2026-09-26
**Deciders:** Équipe plateforme (arbitrage utilisateur, après step-310)
**Réf spec:** passerelle §6.14 ; tableau de bord §6.9, §6.10, §517 ; ADR-0017 (amendé, non remplacé) ; step-310

## Context

step-310 vérifie des JWT signés contre un JWKS configuré (`auth.OIDCVerifier` : `iss`, `aud`, `exp`, signature
`RS256`/`ES256`, claim `scope`, `sub` non vide). Elle ne dit pas **qui les émet**, et aucune spec ne le dit :
- le plan (§236, §532) parle seulement d'« auth opérateur réelle (OIDC/mTLS) » ;
- le contrat Admin déclare un flux `clientCredentials` dont le `tokenUrl`
  (`https://admin.gateway.internal/oauth/token`) est un placeholder qu'aucun service ne sert ;
- la spec du tableau de bord refuse un IdP externe pour les humains (§6.9), avec pour raison « aucun service
  d'identité à exploiter ni verrouillage fournisseur » (§517).

Le BFF est l'appelant principal de l'API Admin. Il authentifie déjà les opérateurs, avec MFA, et signe déjà sa
propre session. ADR-0017 en a tiré une conséquence : la passerelle ne voit que le jeton de service du BFF,
jamais l'humain. Cela laisse `created_by` vide pour toujours (`debts/created-by-jamais-renseigne.md`) et fait
détenir au BFF tous les scopes, quel que soit l'opérateur connecté.

## Decision

Le BFF est l'**émetteur** des jetons de l'API Admin. Il n'y a pas d'IdP séparé.

- **Clés.** Le BFF détient une clé privée `ES256`, qui ne le quitte jamais (secret Kubernetes ou KMS). Il publie
  la clé publique en JWKS, avec un `kid` par clé, sur une URL interne. La passerelle la lit par
  `OIDC_JWKS_URL`. La passerelle ne détient aucun secret d'authentification.
- **Jeton.** Un JWT par opérateur connecté :

  | Claim | Valeur |
  |---|---|
  | `iss` | l'URL canonique du BFF, identique octet pour octet à `OIDC_ISSUER` |
  | `aud` | `gateway-admin`, la valeur de `OIDC_AUDIENCE` |
  | `sub` | `dashboard.operators.id` de l'humain connecté |
  | `scope` | les scopes de passerelle que ses permissions ouvrent, séparés par des espaces |
  | `iat`, `exp` | durée de vie de **5 minutes** |

  Le BFF réutilise le jeton d'un opérateur jusqu'à 60 s avant son `exp`. Un changement de rôles ou de statut
  de l'opérateur invalide ce jeton côté BFF.
- **Scopes.** Le BFF traduit ses permissions (§6.10) en scopes de passerelle. `content:read`, `content:erase`,
  `gdpr:erase`, `cdr:export_bulk` et `audit:read` se transposent un pour un. `admin:read` et `admin:write`
  couvrent le reste, grossièrement. `msisdn:reveal` n'a pas d'équivalent dans le catalogue du BFF : le BFF
  l'ajoute à son catalogue avant de pouvoir l'émettre.
- **Rotation.** Le BFF publie la nouvelle clé, signe avec elle, puis retire l'ancienne du JWKS une fois la
  durée de vie maximale d'un jeton écoulée. La passerelle recharge le JWKS dès qu'elle rencontre un `kid`
  inconnu : aucune coordination, aucun redémarrage.
- **Jobs planifiés.** Les extractions programmées sont planifiées **par le BFF**, qui signe un jeton de
  5 minutes à chaque exécution : aucun jeton n'est stocké. Un job tourne au nom de **l'opérateur qui l'a
  créé** (`sub`, avec ses scopes au moment de l'exécution). Si cet opérateur est désactivé, ses jobs
  s'arrêtent. Le traitement lui-même est asynchrone dans la passerelle (le modèle de
  `create-message-export`), qui n'appelle pas l'API Admin et n'a besoin d'aucun jeton.
- **Scripts et collection en production.** Pas de compte de service : aucune automatisation externe n'est
  prévue (arbitrage utilisateur, 2026-09-27). Le BFF délivre à un opérateur
  authentifié, MFA comprise, un **jeton personnel** à son `sub`, dont les scopes sont inclus dans les siens et
  dont la durée est d'une heure au plus. Tout appel reste imputé à une personne.
- **Flux temps réel du tableau de bord** *(amendement du 2026-10-10, go-gateway-bo step-070)*. Les trois
  flux `/admin/stream/*` sont ouverts par le BFF, une connexion par cluster, sans opérateur derrière :
  le jeton porte un `sub` constant (un UUID fixe du BFF, sans ligne en base) et `admin:read` seul. Ce
  n'est pas un compte de service : il ne lit que les flux, n'écrit rien et ne sort pas du BFF.
- **Transport.** Le mTLS de step-300 reste la preuve de *quelle machine* appelle. Le jeton est la preuve de
  *qui*, et avec quels droits.

## Options Considered

### Option A : le BFF émetteur (retenue)
| Dimension | Évaluation |
|---|---|
| Complexité | Faible côté passerelle (step-310 suffit) ; moyenne côté BFF (signature, JWKS, rotation) |
| Coût | Aucun service de plus |
| Identité | L'humain atteint la passerelle |
| Verrouillage | Aucun |

**Pour :** cohérente avec §517. `created_by` devient remplissable. Un opérateur sans `content:read` ne peut plus
le demander à la passerelle, même par un BFF défaillant.
**Contre :** la clé privée du BFF forge n'importe quel jeton. Le rayon est le même qu'un secret
`client_credentials` détenu par le BFF, mais il faut la garder et la faire tourner.

### Option B : un IdP d'entreprise en `client_credentials`
**Pour :** aucun code d'émission ; le code de step-310 marche tel quel.
**Contre :** exige un IdP que la spec refuse d'exploiter. La passerelle ne voit toujours qu'un jeton de service.

### Option C : un Keycloak dédié
**Contre :** un service lourd de plus (JVM, base, mises à jour) pour un seul client, et toujours sans l'humain,
sauf échange de jeton. Écartée.

### Option D : la passerelle émet ses propres jetons (`/oauth/token` sur admin-api-svc)
**Contre :** la passerelle deviendrait un serveur OAuth qu'elle vérifierait elle-même, avec des secrets
clients à stocker. Elle ne connaîtrait pas l'humain davantage. Écartée.

## Trade-off Analysis

A et B coûtent la même chose à la passerelle. Ce qui les sépare : A fait porter l'émission au composant qui
connaît déjà l'humain, B la confie à un service qui ne le connaît pas. Le prix de A est la garde d'une clé
de signature par le BFF, un service déjà sensible (il détient la session de tous les opérateurs). Le gain de
moindre privilège est réel pour les scopes sensibles. Il est nul entre `admin:read` et `admin:write`, qui
restent grossiers : un `script_author` porte `admin:write`, et le BFF reste seul juge de la granularité fine.

## Consequences

- **ADR-0017 est amendé, pas remplacé.** Les deux journaux gardent leur périmètre. Mais
  `control_plane.audit_log.operator` porte désormais l'`operator_id` de l'humain pour tout appel du BFF. La
  corrélation par `X-Request-Id` n'est plus nécessaire pour retrouver l'humain.
- **`created_by` devient remplissable, sans FK.** Le BFF a sa propre base : la FK vers le stub
  `dashboard.operators` ne pourrait jamais être satisfaite par un opérateur réel, et elle tombe (step-405).
  La colonne porte l'`operator_id` du BFF, et c'est au BFF de le traduire en nom.
- **Le JWKS du BFF sera servi sous une autorité interne.** `debts/jwks-joint-par-les-seules-racines-systeme.md`
  devient **bloquante pour le go-live** : la passerelle a besoin d'une ancre de confiance pour le JWKS, sur le
  modèle `*_TLS_CA_FILE` de step-305.
- **Le contrat ment sur le flux** : `clientCredentials` et le `tokenUrl` ne décrivent plus rien. La
  description d'`OperatorBearer` doit dire qui émet le jeton. Un changement de prose seul est un bump MINEUR ;
  changer le type du schéma ne l'est pas, et n'est pas proposé ici.
- **Plus dur :** le BFF doit garder une clé, la faire tourner et servir un JWKS disponible. Si son JWKS est
  indisponible pendant qu'une clé tourne, la passerelle répond 503 aux jetons signés par la nouvelle clé.
- **À revoir** si une automatisation externe doit un jour parler à l'API Admin sans humain derrière elle.
  Il faudrait alors des comptes de service **gérés par le BFF** (secret échangé auprès du BFF contre un jeton
  de 5 minutes, révocation dans le BFF), plutôt que des jetons longue durée révocables par la passerelle, qui
  lui imposeraient une liste de révocation sur le chemin d'authentification.

## Action Items

1. [x] Passerelle (step-405) : ancre de confiance du JWKS (`OIDC_JWKS_CA_FILE`), qui paie
   `debts/jwks-joint-par-les-seules-racines-systeme.md`, avant step-410.
2. [x] Passerelle (step-405) : description d'`OperatorBearer` dans `api/openapi-admin.yaml` (émetteur = BFF),
   bump MINEUR.
3. [x] Passerelle (step-405) : retirer la FK `created_by → dashboard.operators` et le stub, puis écrire `sub`
   dans `created_by`, ce qui paie `debts/created-by-jamais-renseigne.md`.
4. [ ] BFF (`go-gateway-bo`) : émission, JWKS, rotation, traduction permissions → scopes, jetons personnels.
5. [ ] step-410 : `gateway-oidc` renseigné avec `OIDC_ISSUER` (le BFF), `OIDC_AUDIENCE=gateway-admin` et
   `OIDC_JWKS_URL` (le JWKS du BFF).
