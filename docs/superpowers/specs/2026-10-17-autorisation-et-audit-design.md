# Autorisation et audit — la passerelle décide, un journal par fait

2026-10-17 · Décision 5 du monorepo (`docs/audit-architecture-2026-10-14.md` §2 F4, §6).
Statut : design validé.

## 1. Problème

- **Deux points de décision.** 46 clés de permission du BFF se réduisent à `admin:read`/`admin:write`
  plus six scopes sensibles. Toute clé d'écriture ouvre toutes les écritures amont ; le jeton personnel
  contourne les gardes fines du BFF ; un rôle avec écriture sans lecture passe le BFF puis prend un 403
  amont.
- **Audit doublé.** Chaque mutation relayée est auditée par le BFF, hors transaction, après avoir relu
  l'état amont pour remplir le « before », et par la passerelle. On ne sait pas quel journal fait foi.
- **Relais écrit à la main** : ~33 opérations, ~75 lignes de Go chacune, enums recopiés sans garde.
  Divergences réelles (sender IDs : `maxLength` 11 contre 20, `traffic_category` absent, statut
  `pending_carrier_approval` manquant). Troisième copie du modèle dans `internal/fakegateway`.
- **Trois listes de scopes** tenues alignées par convention.

## 2. Décisions

| # | Décision |
|---|---|
| D1 | **La passerelle est le seul point de décision d'autorisation.** Le BFF relaie sans regarder. |
| D2 | **Le dashboard authentifie et accorde ; la passerelle applique.** Le BFF authentifie les humains (mot de passe, MFA, WebAuthn, session), attribue les rôles et signe le jeton (ADR-0019). La passerelle n'authentifie aucun humain : elle vérifie la signature par le JWKS, puis le scope de l'opération. |
| D3 | **Scopes `<tag>:read` / `<tag>:write`** dérivés des tags du contrat Admin, plus les six scopes sensibles. Une seule liste : le contrat. |
| D4 | **La passerelle est la source unique de l'audit du plan de contrôle**, valeurs avant/après comprises, capturées par trigger dans la transaction de la mutation. Le BFF ne journalise que ses propres faits. |
| D5 | **Le rejeu est une opération Admin**, authentifiée et auditée comme les autres. Aucun binaire de rejeu. |

**Options écartées.**
- *Le BFF seul décideur, aucun jeton ne sort de lui* : garde les relais et leurs gardes, supprime le
  jeton personnel, et une compromission du BFF ouvre tout sans qu'aucune garde amont ne reste.
- *Fusionner le BFF dans `admin-api`* : supprime le plus de code, mais expose au navigateur (cookies,
  MFA) un plan de contrôle aujourd'hui interne et sous mTLS. Écartée pour la sécurité.
- *Table générique de relais par `operationId`* (piste de l'audit) : inutile, un proxy aveugle n'a rien
  à décider par opération.

## 3. Autorisation

### 3.1 Scopes

- Chaque tag du contrat Admin donne `<tag>:read` et `<tag>:write` (`routes:read`,
  `connectors:write`…).
- Les six scopes sensibles restent, chacun avec sa raison : `content:read`, `content:erase`,
  `gdpr:erase`, `msisdn:reveal`, `cdr:export_bulk`, `audit:read`.
- `admin:read` et `admin:write` disparaissent.
- Pas de joker, de hiérarchie ni de scope par opération : à ajouter quand un rôle réel en a besoin.

### 3.2 Une seule liste

Chaque opération Admin déclare **exactement un** scope dans `security`. La liste est générée depuis le
contrat et consommée par l'enum `auth.Scope` de la passerelle, l'éditeur de rôles du BFF et les types de
la SPA.

### 3.3 Le BFF

- **Un rôle est un ensemble de scopes** de la passerelle, plus les permissions propres au BFF
  (`operators:*`, `roles:*`, `sessions:*`), déclarées dans son contrat.
- Le jeton de l'opérateur (ADR-0019) porte les scopes de ses rôles. Le jeton personnel reste inclus
  dans les droits de l'opérateur, une heure au plus ; il n'est plus un contournement, puisque la
  passerelle applique les mêmes scopes quel que soit le chemin.
- **Proxy `/api/gw/*`** : vérifie la session, remplace le cookie par le jeton de l'opérateur, et fait
  suivre **tel quel**. Il ne garde rien, ne journalise rien, ne transforme rien, et refuse tout chemin
  absent du contrat Admin.
- La SPA masque les actions interdites : confort d'affichage, jamais une décision.

### 3.4 Gardes (tests bloquants)

1. Chaque opération Admin a exactement un scope déclaré (prolonge `operation-without-security-is-public`).
2. Le scope appliqué est celui que déclare le contrat : chaque opération, appelée avec un jeton qui a
   tous les scopes sauf le sien, rend 403.
3. Le proxy n'expose que les chemins du contrat Admin.
4. Le BFF n'a aucun handler pour une opération de la passerelle.

## 4. Audit

### 4.1 `audit_log` — les événements

- Une ligne par mutation et par lecture sous scope sensible ; les lectures ordinaires ne sont pas
  journalisées.
- Colonnes : `operator` (le `sub` de l'opérateur, ou `service:<nom>`), `operation_id`, `target`,
  `request_id`, `status`, `outcome`, `at`, `finished_at`. Immuable (step-315).
- **`content_access_audit` est absorbée** : une lecture de corps est une ligne avec `outcome` à
  `granted`, `unreadable` ou `not_found`, écrite **avant** que le corps soit rendu (step-163).

### 4.2 `audit_changes` — les valeurs

- Trigger `AFTER INSERT/UPDATE/DELETE` sur chaque table de configuration, qui écrit
  `(request_id, actor, table, pk, op, before, after, at)` dans la transaction de la mutation.
- **Acteur obligatoire.** L'Admin API pose `SET LOCAL app.actor = <sub>` et `app.request_id` ; un
  écrivain interne pose `app.actor = 'service:<nom>'` (le STOP par MO du routeur, step-398). Sans
  acteur, le trigger lève une erreur : une écriture non attribuable échoue.
- **Colonnes exclues du diff** : `*_sealed`, `*_hash`, `*_kms_key_ref`, `wrapped_key`.

### 4.3 Classement des tables

Chaque table de `control_plane` est **auditée** ou **exemptée avec une raison** ; la garde refuse une
table non classée.

| Classement | Tables | Raison |
|---|---|---|
| Auditée (diff) | `customers`, `customer_groups`, `rate_plans`, `smpp_accounts`, `credentials`, `sender_ids`, `connectors`, `connector_accounts` (décision 3), `routes`, `route_targets`, `routing_scripts`, `rate_limits`, `antispam_rules`, `webhooks`, `sender_id_rewrite_rules`, `inbound_numbers`, `inbound_keywords`, `opt_out_keywords`, `platform_policy` (décision 8), `content_keys` | Configuration |
| Événement seul | `exact_routes` | Un import de masse fait des millions de lignes : une ligne d'`audit_log` avec le nombre de lignes et l'empreinte du fichier |
| Événement seul | `suppressions`, `unrouted_mo` | MSISDN : les copier dans un journal irait contre T8 |
| Exemptée | `balances`, `balance_deltas`, `billing_ledger*`, `billing_idempotency`, `billing_events_outbox`, `credit_leases` (décision 1) | Le grand livre est lui-même le journal de la facturation ; volume du chemin chaud |
| Exemptée | `gdpr_erase_jobs`, `message_export_jobs`, `cdr_archives`, `connector_account_leases`, `pool_members`, `connector_account_poll_cursors`, `lane_parking` | État d'exécution ; la création d'un job est déjà une ligne d'`audit_log` |
| Exemptée | `audit_log`, `audit_changes` | Le journal lui-même |

### 4.4 Rétention

Plancher existant `8760 hours` (step-297) ; les deux tables sont partitionnées par mois.

### 4.5 Le journal du BFF

Ses seuls faits : connexions et déconnexions, échecs de mot de passe ou de MFA, enrôlement MFA et
WebAuthn, création et désactivation d'opérateurs, changements de rôles, **émission** d'un jeton
personnel, refus de session.

### 4.6 L'écran d'audit

Deux onglets, une source chacun, jamais fusionnés :

| Onglet | Source | Contenu |
|---|---|---|
| Plan de contrôle | Passerelle (`audit_log` + `audit_changes`), par le proxy sous `audit:read` | Toute modification de la configuration (qui, quoi, résultat, avant/après), rejeux, lectures sensibles |
| Accès au dashboard | Journal du BFF | §4.5 |

Un jeton personnel relie les deux par son `sub` : son émission est dans l'un, chacun de ses usages dans
l'autre.

## 5. Rejeu

L'outil de rejeu de la décision 4 (`lane-replay`) est l'opération Admin **`replay-dead-letter`**, sous
`dead-letters:write`, authentifiée et auditée comme les autres. L'identité auto-déclarée
`declared:<nom>` disparaît, et avec elle le binaire `mt-replay`.

## 6. Pannes

| Panne | Effet |
|---|---|
| Écriture d'audit | La mutation échoue avec elle (même transaction) : aucune mutation sans trace |
| JWKS du BFF injoignable | Clés en cache utilisées ; un `kid` inconnu est refusé (401) |
| BFF arrêté | Le dashboard est indisponible ; la passerelle continue d'appliquer les jetons déjà émis jusqu'à leur `exp` (5 min) |

## 7. Tests bloquants

- Le trigger capture avant et après ; les colonnes scellées sont absentes ; une écriture sans acteur
  est refusée ; toute table de `control_plane` est classée.
- Une lecture de corps est auditée avant que la réponse parte.
- Les quatre gardes du §3.4.

## 8. Porté, réécrit, supprimé, neuf

| | |
|---|---|
| **Porté** | Vérificateur JWT de la passerelle ; émission de jetons du BFF (ADR-0019) ; session, MFA, WebAuthn ; hub temps réel |
| **Réécrit** | Permissions du BFF en ensembles de scopes ; middleware d'audit (`operator = sub`, `SET LOCAL`) |
| **Supprimé** | Les 33 relais et `relay.go` ; les gardes fines du BFF sur la passerelle ; l'audit du BFF sur les mutations ; `internal/fakegateway` (remplacé par Prism sur le contrat Admin) ; `content_access_audit` ; `cmd/mt-replay` ; `admin:read`, `admin:write` |
| **Neuf** | `audit_changes` et ses triggers ; proxy `/api/gw/*` ; génération des scopes ; `replay-dead-letter` |

## 9. Effets sur la spec et les contrats actuels

| Où | Effet |
|---|---|
| Contrat Admin | Scopes refondus : les jetons actuels deviennent invalides, **bump majeur** |
| Contrat BFF | Les 33 opérations relayées disparaissent : **bump majeur** |
| ADR-0019 | Amendé : la correspondance permissions → scopes devient l'identité |
| Schéma | `audit_changes` et triggers ; `content_access_audit` supprimée après migration de ses lignes dans `audit_log` ; `operator` porte le `sub`. Les lignes antérieures gardent leur `tok_…`, qu'aucune donnée ne relie à un `sub` |
| Décision 6 | La clé de signature du BFF fabrique n'importe quel jeton (ADR-0019) : sa protection est l'affaire du modèle de confiance interne |
