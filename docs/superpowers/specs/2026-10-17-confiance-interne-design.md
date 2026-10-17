# Confiance interne — une identité par service, partout

2026-10-17 · Décision 6 du monorepo (`docs/audit-architecture-2026-10-14.md` §2 F5, §6).
Statut : design validé.

## 1. Problème

La frontière de confiance interne est plate :

- `admin-api-svc` n'a pas de `TLS_ALLOWED_CLIENTS` : tout détenteur d'un certificat de la CA est admis,
  alors que billing, content-key, session-manager et smpp-server nomment leurs appelants ;
- personne ne dit qui émet le certificat client du BFF ;
- le JWKS est servi sur l'écouteur public du BFF, alors que l'ADR-0019 le veut interne ;
- la KEK de `content-key-svc` est dans `gateway-secrets`, le Secret qui porte aussi `POSTGRES_URL` et
  `REDIS_URL` de tous les pods ;
- un seul rôle Postgres et un seul schéma pour tous les services ; Kafka sans authentification
  (`kafka-sans-authentification`). L'audit citait aussi les secrets de webhook en clair : la fiche
  est payée depuis step-295b (`webhooks.secret_sealed`) ;
- l'identité système des flux porte `admin:read`, donc tout le plan de contrôle ;
- step-287h demande des identifiants client du BFF pour des Jobs, ce que l'ADR-0019 interdit ;
- aucun secours quand le BFF, seul émetteur d'identité, est tombé.

## 2. Décisions

| # | Décision |
|---|---|
| D1 | **Portabilité.** La plateforme se déploie sur Kubernetes autohébergé comme sur un cloud managé. Seules des briques identiques dans les deux sont retenues ; ce qui varie vit dans leur configuration, jamais dans notre code. |
| D2 | **Une CA interne unique**, émise par cert-manager, pour tous les certificats, BFF compris. |
| D3 | **Chaque serveur interne nomme ses appelants.** Une allowlist vide fait échouer le démarrage en production. |
| D4 | **Une identité par service sur chaque magasin** (rôle Postgres, principal Kafka, utilisateur Redis), droits **générés** depuis ce que le service déclare. |
| D5 | **Un Secret par service** ; la KEK sort du cluster applicatif, dans **OpenBao Transit**, seule implémentation de l'interface `KMS` (ADR-0011). |
| D6 | **JWKS interne** ; scope **`stream:read`** pour le hub ; **aucun compte de service** (ADR-0019). |
| D7 | **Secours par SQL**, sous identifiants dynamiques d'une heure, audité et propagé par le trigger des décisions 5 et 7. Pas de second émetteur de jetons. |

## 3. Briques

| Besoin | Brique | Autohébergé | Cloud managé |
|---|---|---|---|
| mTLS interne | cert-manager, émetteur CA interne | ✔ | ✔ |
| KMS (KEK du contenu, `ConfigSecrets`) | OpenBao Transit | Déverrouillage Shamir, manuel | Déverrouillage automatique par la KMS du fournisseur |
| Identité Postgres | Rôle par service, GRANT | ✔ | ✔ (RDS, Cloud SQL) |
| Identité Kafka | mTLS, principal = CN, ACL | ✔ | ✔ (MSK, Confluent) |
| Identité Redis | Utilisateur ACL, motif de clés | ✔ | ✔ (ElastiCache, Memorystore) |
| Réseau | NetworkPolicy | ✔ (k3s l'applique nativement) | ✔ |
| Secrets | Secret Kubernetes par service | ✔ | ✔ |

Un adaptateur vers la KMS native d'un fournisseur ne s'écrit que lorsqu'un déploiement l'exige. Coût
assumé : exploiter OpenBao même dans le cloud, contre un seul chemin de KMS, testé en CI.

## 4. Transport

- **cert-manager signe tout** depuis une seule CA interne : passerelle, BFF (`dashboard`), Jobs.
- **Allowlists** : chaque serveur gRPC ou HTTP interne déclare `TLS_ALLOWED_CLIENTS`. `admin-api` admet
  `dashboard` seul. Vide en production : le démarrage échoue. L'admission implicite de toute la CA
  disparaît.
- **NetworkPolicy** *deny* par défaut sur chaque namespace ; les flux permis sont générés depuis les
  allowlists et le catalogue de la décision 4.

## 5. Magasins

### 5.1 Postgres

- Un rôle par service, qui n'a que les GRANT des tables qu'il lit ou écrit.
- Un rôle **`migrator`** distinct, propriétaire des schémas ; aucun service ne l'est.
- `message_state` (décision 2) et la facturation ont chacune leur rôle écrivain.
- Les GRANT sont **générés** depuis un fichier de déclaration par service et le classement des tables
  (décision 5 §4.3). Une garde refuse un droit non déclaré.

### 5.2 Kafka

mTLS ; le principal est le CN du certificat. Les ACL sont **générées depuis le catalogue** de la
décision 4 (`producers`, groupes) et appliquées par le même Job. Aucune politique écrite à la main.

### 5.3 Redis

Un utilisateur ACL par service, limité à son espace de clés. Ce qui reste dans Redis est l'affaire de
la décision 7.

## 6. Secrets

- **Un Secret Kubernetes par service**, qui ne porte que ce dont il a besoin. `gateway-secrets`
  disparaît.
- **La KEK** vit dans OpenBao Transit. Seul `content-key-svc` peut appeler `encrypt` et `decrypt`
  (politique OpenBao).
- `mot-de-passe-de-bind-en-argv` vise deux outils de charge (`smpp-bindgen`, `smsc-ceiling`), pas un
  service : la fiche reste ouverte, hors de cette décision.

## 7. Jetons

- **JWKS** servi sur un écouteur interne du BFF, sous mTLS, allowlist `admin-api`. Absent de
  l'écouteur public.
- **`stream:read`** couvre les trois flux `/admin/stream/*`. L'identité système du hub ne porte que lui.
- **Aucun compte de service** (ADR-0019, arbitrage du 2026-09-27). Les Jobs de test (step-287h)
  s'authentifient par le `StaticVerifier`, que le démarrage refuse en production.

## 8. Secours

Quand le BFF est tombé ou sa clé révoquée :

1. L'exploitant obtient d'OpenBao des identifiants **dynamiques** du rôle `breakglass`, valables une
   heure ; OpenBao audite l'émission.
2. Il écrit sur `control_plane` en SQL, sous `SET LOCAL app.actor = 'breakglass:<nom>'` : le trigger
   de la décision 5 refuse toute écriture sans acteur.
3. La propagation est automatique : le trigger notifie au commit (décision 7).

Les validations de l'Admin API ne s'appliquent pas : c'est le prix d'un secours sans second émetteur
de jetons, c'est-à-dire sans seconde clé capable de tout fabriquer. La procédure vit dans le runbook.

## 9. Tests bloquants

- Une allowlist vide fait échouer le démarrage en production.
- Chaque serveur refuse un appelant absent de son allowlist.
- Les ACL Kafka générées correspondent exactement au catalogue.
- Aucun service n'a de GRANT au-delà de sa déclaration.
- Le JWKS est absent de l'écouteur public.
- Le hub ne peut appeler aucune opération hors `stream:read`.

## 10. Supprimé, neuf

| | |
|---|---|
| **Supprimé** | `gateway-secrets` ; la KEK dans un Secret Kubernetes ; le JWKS public ; `admin:read` du hub ; l'admission implicite de toute la CA ; le rôle Postgres unique |
| **Neuf** | OpenBao (Transit, identifiants dynamiques `breakglass`) ; génération des ACL Kafka, des GRANT et des NetworkPolicy |

## 11. Effets sur la spec et les contrats actuels

| Où | Effet |
|---|---|
| ADR-0011 | Amendé : la KEK vit dans OpenBao Transit ; `LocalKMS` reste pour les tests |
| ADR-0019 | Amendé : JWKS interne ; `stream:read` remplace `admin:read` pour le hub |
| Contrat Admin | `stream:read` ajouté (inclus dans le bump majeur de la décision 5) |
| Dettes soldées | `kafka-sans-authentification` |
| Manifests | Un Secret par service ; allowlist sur chaque serveur interne ; NetworkPolicy générées |
