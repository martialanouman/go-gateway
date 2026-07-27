# Tableau de bord Admin — Spécification Technique
**Modèle :** RESHADED (Requirements → Estimation → Storage Schema → High-Level Design → API Design → Detailed Design → Evaluation → Distinctive Component)
**Composant :** Tableau de bord Admin / Exploitation (TanStack Start / React, pnpm)
**Document compagnon :** `specification-technique-passerelle-sms.md` (ce tableau de bord est un client de l'API Admin de la passerelle)
**Statut :** v2.0

*Note de convention : les blocs de code (schémas, endpoints API, diagrammes, JSON, y compris leurs commentaires) restent en anglais. Seul le texte narratif est en français.*

---

## 1. Exigences (Requirements)

### 1.1 Exigences fonctionnelles

- **Gestion des clients et de leurs comptes SMPP (admin)** — navigation à deux niveaux suivant le modèle de la passerelle (§6.18 compagnon) : un **client** détient un ou plusieurs **comptes SMPP**. Les clients n'ont aucun accès à la plateforme ; les admins créent/modifient/suspendent les deux niveaux et gèrent chaque sous-ressource.
  - **Au niveau client** : identité, statut (suspendre un client suspend tous ses comptes), **sender IDs**, **facturation** (soldes MT et MO, plan tarifaire, découvert, `balance_scope`), **politique de stockage de contenu**, appartenance à un **groupe**.
  - **Au niveau compte SMPP** : **identifiant de bind SMPP + une clé API** (création, rotation manuelle avec fenêtre de grâce, révocation), **canaux** (SMPP/REST), **politique d'autorisation de sender ID**, bascules **`query_sm`/`cancel_sm`**, **quotas/limites de débit**, `max_sessions`, **webhook MO/DLR**.
- **UI de groupes de clients (organisationnel)** — CRUD des groupes pour segmenter la base (par secteur, région, revendeur), affectation de clients, et filtrage par groupe partout où les clients/comptes apparaissent. Un groupe ne porte ni solde, ni quota, ni règle de configuration (§6.17 compagnon).
- **UI de gestion des connecteurs** — CRUD des connecteurs SMSC, formulaire à divulgation progressive (champs requis d'abord, section « Avancé » pour l'ensemble SMPP), statut de bind en direct, **`link_status` et `breaker_state` distincts** (§6.5), configuration du pool de binds, rebind/déconnexion forcée, débit/taux d'erreur.
- **UI de gestion des routes** — constructeur visuel des règles déclaratives (priorité, glisser-déposer, conditions), sélecteur de stratégie de distribution, plus le **routage par numéro exact** (§6.7).
- **UI de règles de réécriture de sender ID (admin)** — CRUD à portée plateforme/client/compte/connecteur, avec test.
- **Éditeur de scripts de routage personnalisés (admin)** — éditeur Monaco (JS/Lua), validation/lint, test contre un payload, versions, publication/retour arrière, affectation compte/plateforme, métriques d'exécution par script.
- **UI de désabonnement (opt-out)** — gestion des listes de suppression scopées au canal, mots-clés par pays, import en masse, et outil « pourquoi ce message a-t-il été bloqué ? » (§6.16).
- **Numéros entrants & mots-clés** — CRUD des shortcodes/long codes, affectation compte/mot-clé, file « MO non routés » (§6.17).
- **Moniteur de session en direct** — tous les binds SMPP actifs (utilisateur + SMSC), avec déconnexion forcée et gestion de `max_sessions` (§6.5).
- **Tableau de bord de trafic temps réel** — compteurs/graphiques MT/MO, taux de succès/échec, latences, ventilation par connecteur/client/compte/groupe.
- **Recherche/explorateur de CDR** — recherche paginée et filtrable (client, compte, groupe, date, statut, source/dest, connecteur, route), panneau de détail (chronologie + corps si stocké et autorisé), export CSV asynchrone gouverné.
- **Visualiseur de trace SMS** — trace complète par ID de message/trace ; le corps n'apparaît jamais dans la trace.
- **Gestion anti-spam** — CRUD des règles, file de revue, tendance de réputation par client.
- **UI de solde de crédit SMS** — soldes **MT et MO présentés distinctement** (le MO est un compteur, pas un solde), recharges, `balance_scope`, plan tarifaire, fournisseur externe, grand livre (§6.11).
- **UI de politique de contenu & effacement RGPD** — politique de stockage plateforme/client, lecture de corps gardée et auditée, effacement (crypto-shred + droit à l'oubli) (§6.19).
- **Alerting & notifications** — règles configurables ; les alertes sur métriques d'infrastructure sont évaluées par la pile Alertmanager de la passerelle, indépendamment de la disponibilité du tableau de bord (§6.8).
- **Autorisation basée sur les permissions & journal d'audit** — contrôle d'accès fin au niveau permission, piste d'audit complète, rôles par défaut.
- **Authentification** — email/mot de passe + **MFA** (application authenticator/TOTP et passkey/WebAuthn), sans dépendance à un fournisseur d'identité externe (§6.9).

### 1.2 Exigences non fonctionnelles

| Catégorie | Cible |
|---|---|
| Latence temps réel | Mises à jour métriques/session visibles dans 2–5 s |
| Opérateurs simultanés | 100–300 (outil interne) |
| Disponibilité | 99,9 %. Atteignable uniquement en topologie **multi-instance** (≥2 BFF + pub/sub de diffusion, §4.1) — un process unique serait un SPOF |
| Portée de l'indisponibilité tolérable | Une panne du tableau de bord ne dégrade que la *visualisation*, pas la *détection* : l'alerting infra reste opérationnel via Alertmanager (§6.8) |
| Fraîcheur des données pour la recherche | Recherche CDR en ~10–30 s après traitement |
| Sécurité | Permissions appliquées côté serveur, journal d'audit, MFA requis pour les rôles privilégiés, WebAuthn/passkey ; secrets d'identifiants et **corps des messages** jamais exposés hors permission dédiée (§6.7) |
| Réactivité | Desktop principalement ; dégradation propre sur tablette |
| Accessibilité | WCAG 2.1 AA pour les parcours principaux |

### 1.3 Contraintes

- Frontend : **TanStack Start** (React, TypeScript, routage fichiers, SSR + fonctions serveur), gestionnaire **pnpm**. Consomme exclusivement l'API Admin de la passerelle ; aucun accès DB direct — tout passe par les fonctions serveur qui jouent le rôle de BFF (§4.1).
- Ne doit jamais se trouver sur le chemin critique du plan de données — une panne du tableau de bord n'affecte jamais le débit de SMS.
- Déployé indépendamment de la passerelle, comme serveur Node.js auto-hébergé (pas edge/serverless : il maintient des connexions WebSocket longue durée). « Auto-hébergé » implique **≥2 instances** derrière un load balancer pour la HA (§4.1).
- L'UI de facturation est un proxy fin (aucune donnée propre) et se dégrade vers « fonctionnalité indisponible » quand le module de facturation est désactivé.
- L'évaluation des alertes sur métriques d'infrastructure ne dépend pas de la disponibilité du tableau de bord (§6.8).

---

## 2. Estimation

- Opérateurs simultanés : 100–300, pics lors d'incidents.
- Flux temps réel : compteurs agrégés (pas d'événements bruts) — ~1 mise à jour/s par widget actif ; avec 300 clients × ~10 widgets ≈ 3 000 msgs/s de diffusion depuis la couche WS du BFF (agrégation faite une fois en amont).
- Recherche CDR : faible volume (dizaines/s), chaque recherche pouvant scanner de larges plages — adossée au magasin CDR de la passerelle.
- Moniteur de session : jusqu'à des dizaines de milliers de sessions — pagination/virtualisation, mises à jour en deltas.
- Journal d'audit : ~10–50 actions/jour/opérateur — volume négligeable.
- Évaluation d'alerte infra : déportée sur Alertmanager, aucun coût supplémentaire côté tableau de bord au-delà du webhook entrant.

---

## 3. Schéma de stockage (Storage Schema)

Le tableau de bord est quasi sans état : le plan de contrôle de la passerelle (PostgreSQL) reste la source de vérité pour la configuration, le magasin CDR (ClickHouse) pour l'historique. Le BFF ne possède que les préoccupations propres à l'UI.

### 3.1 Données propres au tableau de bord (schéma PostgreSQL 18 séparé et de petite taille)

Tous les ID sont des UUIDv7, cohérent avec la convention plateforme.

```
operators
  id (uuidv7, pk)
  email, display_name
  password_hash                          -- email/password auth (§6.9)
  mfa_totp_secret        (nullable)       -- enrolled authenticator (TOTP)
  mfa_webauthn_credentials (jsonb/array)  -- registered passkeys/WebAuthn authenticators
  status                 (active|disabled)
  last_login_at

permissions                          -- fixed catalog, seeded/versioned with releases, not admin-editable
  key                    (pk, e.g. routes:read, routes:write, routes:import, scripts:read, scripts:write,
                           scripts:publish, sessions:read, sessions:disconnect, antispam:read, antispam:write,
                           customers:read, customers:write, accounts:read, accounts:write,
                           credentials:read, credentials:write, credentials:rotate,
                           senderrewrite:read, senderrewrite:write,
                           suppressions:read, suppressions:write, suppressions:delete,
                           inbound:read, inbound:write, groups:read, groups:write,
                           billing:read, billing:write, billing:topup, billing:provider:write, billing:scope_change,
                           content:read, content:erase, gdpr:erase,
                           cdr:export_bulk, alerts:read, alerts:write, audit:read, operators:manage, roles:manage)
  category               (routing|connectors|sessions|antispam|accounts|billing|content|compliance|alerts|audit|admin)
  description

roles
  id (uuidv7, pk)
  name                   (unique, e.g. super_admin, ops, support_readonly, billing_admin, billing_readonly,
                          script_author, account_manager, compliance, auditor)
  description
  is_default (bool), created_by (nullable), created_at

role_permissions                     -- many-to-many: this is what makes authorization permission-based
  role_id (fk), permission_key (fk)

operator_roles                       -- many-to-many: an operator can hold more than one role
  operator_id (fk), role_id (fk)

audit_log
  id (uuidv7, pk)
  operator_id (fk)
  action                 (e.g. "route.update", "session.disconnect", "script.publish", "billing.topup",
                          "credentials.rotate", "billing.scope_change", "suppression.delete", "gdpr.erase",
                          "content.read")
  target_type, target_id
  before_json, after_json
  ip_address, created_at

alert_rules
  id (uuidv7, pk)
  metric                 (e.g. connector.error_rate, connector.status, account.reputation,
                          billing.mt_balance_low, billing.mo_floor_reached)
  scope                  (global|connector|account)
  scope_id (nullable)
  evaluation_owner       (alertmanager|bff)   -- infra metrics -> alertmanager (independent of dashboard uptime);
                                               -- business-domain metrics -> bff (§6.8)
  condition_json, notify_channels_json (email|webhook|slack), status, created_by

notifications
  id (uuidv7, pk)
  alert_rule_id (fk, nullable)
  source                 (alertmanager|bff_evaluator|billing_alert_stream)
  severity (info|warning|critical), message, read_by_operators (jsonb), created_at

saved_views
  id (uuidv7, pk)
  operator_id (fk)
  view_type              (cdr_search|traffic_dashboard)
  filters_json, name
```

`audit_log` volume modeste ; partitionnement mensuel pour l'hygiène de rétention/archivage, cohérent avec la spec compagnon (§6.14).

### 3.2 Sources de données externes (lues via l'API Admin de la passerelle)

- Clients, comptes SMPP et sous-ressources, connecteurs, routes, scripts, règles anti-spam, sessions, suppressions, numéros entrants → plan de contrôle de la passerelle (PostgreSQL) via l'API Admin.
- Métriques temps réel → **exclusivement via l'API Admin** : instantané REST (`get-metrics-summary`, `get-traffic-metrics`) puis flux `stream-metrics`. Le tableau de bord n'interroge jamais Prometheus/Thanos directement ; seul Alertmanager le fait, pour les alertes infra (§6.8).
- Recherche/historique CDR + traces → magasin CDR (ClickHouse) et backend de tracing via `search-messages` / `get-message-trace` (jamais d'accès direct). Il n'existe **pas** de lecture unitaire de CDR côté passerelle : la fiche d'un message se compose côté BFF.
- Corps d'un message → endpoint de lecture de contenu gardé par `content:read` (§6.19).
- Facturation → plan de contrôle via les endpoints de facturation.
- Alertes d'infrastructure → Alertmanager pousse vers un webhook du BFF (§6.8), inséré dans `notifications`.

---

## 4. Conception de haut niveau (High-Level Design)

```
+----------------------------------------------------------------------------+
|         TanStack Start Application (Node.js, >= 2 instances for HA)          |
|  +-----------------------------+    +------------------------------------+  |
|  | Client (React, browser)      |    | Server (SSR + server functions =   |  |
|  |  Routes: Customers, SMPP      |    |  the BFF layer)                    |  |
|  |  Accounts, Connectors, Routes,|<-->|  - Session/auth: email+password +  |  |
|  |  Exact routes, Script Editor, | HTTP|   MFA (TOTP + WebAuthn); own       |  |
|  |  Sessions, Traffic, CDR,      |  + |    session cookie/JWT               |  |
|  |  Anti-spam, Suppressions,     |  WS |  - Proxy/aggregation to Gateway    |  |
|  |  Inbound numbers, Sender      |    |    Admin API                       |  |
|  |  rewrite, Billing, Content,   |    |  - WebSocket hub: merges the 3     |  |
|  |  Alerts, Audit, Operators     |    |    gateway WS streams into one     |  |
|  |  State: TanStack Query         |    |    multiplexed client socket        |  |
|  |  Real-time: WS client           |    |  - Permission enforcement (§6.10)  |  |
|  +-----------------------------+    |  - Owns: operators, roles/perms,   |  |
|                                       |    audit_log, alert_rules,          |  |
|         (shared Redis Pub/Sub          |    notifications, saved_views       |  |
|          across BFF instances, §4.1)   |  - BFF-side alert evaluator         |  |
|                                       |    (business-domain metrics, §6.8) |  |
|                                       |  - Alertmanager webhook receiver    |  |
|                                       +-----------------+-------------------+  |
+-------------------------------------------------------+--------------------+
                                                        | Gateway Admin API (REST + WS)
+--------------------------------------------------------v--------------------+
|                              SMS Gateway (Core)                              |
|   admin-api-svc, session-manager-svc, billing-svc, metrics pipeline,         |
|   CDR store, tracing backend, Prometheus Alertmanager (infra alerts,         |
|   evaluates and pages independently of this dashboard's uptime)              |
+-----------------------------------------------------------------------------+
```

### 4.1 Le BFF comme couche serveur TanStack Start, en topologie HA

- **Un seul déployable** : un process Node sert l'application rendue et la logique BFF (session/auth, permissions, proxying, diffusion WS).
- **Sécurité de type de bout en bout** : les fonctions serveur sont appelées comme du RPC typé.
- **Centralisation** : application des permissions, journalisation d'audit, diffusion WebSocket (le navigateur ne parle jamais directement à la passerelle).
- **Cible Node auto-hébergée** (pas edge/serverless) car le hub de diffusion WebSocket a besoin d'un processus longue durée.
- **HA multi-instance** : ≥2 instances BFF derrière un load balancer (affinité WS), coordonnées par une **couche pub/sub partagée** (Redis Pub/Sub) sur laquelle les trois flux de la passerelle sont consommés une seule fois, republiés, puis re-diffusés par chaque instance. Plus de SPOF, déploiements sans coupure — et la passerelle ne voit pas son nombre d'abonnés WS croître avec le nombre d'instances BFF.

### 4.2 Architecture Frontend

- **Framework** : TanStack Start (React + TypeScript, Vite, routage fichiers, SSR + fonctions serveur/BFF). **Gestionnaire** : pnpm.
- **État serveur** : TanStack Query (cache, refetch, pagination), associé au pattern loader de TanStack Router.
- **Temps réel** : une WebSocket par client, multiplexée par sujet (`metrics.traffic`, `metrics.connectors`, `sessions.events`, `notifications`, `billing.alerts`). La passerelle expose **trois** flux WebSocket distincts — `stream-metrics`, `stream-sessions`, `stream-billing-alerts` — que le BFF **agrège**, avec ses propres notifications, en une seule socket client. Le sens du travail est donc l'inverse d'un simple fan-out : plusieurs connexions montantes, une seule descendante par opérateur.
- **Graphiques** : visx/Recharts pour les séries temporelles ; tableaux virtualisés pour les grandes listes.
- **Éditeur de script** : Monaco (JS/Lua), diagnostics de lint en ligne, exécuteur de payload en split-pane.
- **Flux d'auth** : login email/mot de passe + MFA géré par la couche serveur (§6.9) ; l'UI se rend selon l'ensemble de permissions retourné par `/auth/me`, jamais un contrôle de rôle codé en dur.

---

## 5. Conception de l'API (API Design)

Le client parle uniquement à son propre serveur TanStack Start (le BFF), qui parle à l'API Admin de la passerelle (§5.3 compagnon) et à son petit schéma PostgreSQL.

### 5.1 API BFF — `dashboard.gateway.example.com/api`

> Surface du BFF, **pas** celle de la passerelle. Elle est alignée sur les 133 opérations d'
> `api/openapi-admin.yaml` (préfixe `/admin`, consommé par le BFF via
> `@martialanouman/gateway-api-contracts`) ; les commentaires signalent les endroits où la forme BFF
> et la forme passerelle diffèrent. Toute évolution du contrat doit être répercutée ici.

```
# Auth (email/password + MFA — TOTP + WebAuthn/passkey; §6.9)
POST   /auth/login                     # email + password -> MFA challenge
POST   /auth/mfa/verify                # TOTP code or WebAuthn assertion
POST   /auth/mfa/enroll                # enroll an authenticator or register a passkey
POST   /auth/logout
GET    /auth/me                        # current operator + resolved permission set (union of held roles)

# Customer groups (§6.17)
GET/POST/PATCH/DELETE  /customer-groups
GET                     /customer-groups/{id}/customers
PATCH                   /customers/{id}/group

# Customers (billing, sender IDs, group)
GET/POST/PATCH/DELETE  /customers                        # ?groupId= filter
POST                    /customers/{id}/suspend
GET/POST/PATCH/DELETE  /customers/{id}/sender-ids
GET                     /customers/{id}/smpp-accounts

# SMPP accounts (channels, quotas, sessions, webhooks, credentials)
GET/POST/PATCH/DELETE  /smpp-accounts                    # POST requires customerId; ?customerId=/?groupId=
POST                    /smpp-accounts/{id}/suspend       # cascade descendante de suspend-customer
PATCH                   /smpp-accounts/{id}/channels
PATCH                   /smpp-accounts/{id}/session-limits
GET                     /smpp-accounts/{id}/sessions      # binds vivants vs max_sessions — alimente l'écart de §6.5
PATCH                   /smpp-accounts/{id}/sender-id-policy
PATCH                   /smpp-accounts/{id}/smpp-ops       # query_sm / cancel_sm toggles
GET/POST/PATCH/DELETE  /smpp-accounts/{id}/webhooks
GET     /smpp-accounts/{id}/credentials                  # masked
POST    /smpp-accounts/{id}/credentials                  # secret shown exactly once
PATCH   /smpp-accounts/{id}/credentials/{credId}
DELETE  /smpp-accounts/{id}/credentials/{credId}
POST    /smpp-accounts/{id}/credentials/{credId}/rotate  # manual, optional grace window

# Connectors / routes / exact routes / scripts / sessions / anti-spam
GET/POST/PATCH/DELETE  /connectors
POST                    /connectors/{id}/rebind
GET                     /connectors/{id}/status           # link_status par bind + breaker_state, distincts (§6.5)
PATCH                   /connectors/{id}/reconnect-policy
PATCH                   /connectors/{id}/bind-pool
GET/POST/PATCH/DELETE  /routes
POST                    /routes/reorder
GET/POST                /exact-routes                     # routes:read / routes:write
PATCH/DELETE            /exact-routes/{msisdn}            # la clé EST le MSISDN — pas d'id de substitution
POST                    /exact-routes/import              # routes:import
GET                     /exact-routes/lookup?msisdn=      # lecture unitaire : pas de GET /exact-routes/{msisdn}
GET/POST/PATCH/DELETE  /routing-scripts
PATCH                   /routing-scripts/{id}/assign
POST                    /routing-scripts/{id}/validate
POST                    /routing-scripts/{id}/test
POST                    /routing-scripts/{id}/publish
GET                     /routing-scripts/{id}/versions
GET     /sessions                       # paginated
DELETE  /sessions/{id}
GET/POST/PATCH/DELETE  /antispam-rules

# Opt-out / suppression (§6.16)
GET     /suppressions?scope=&scopeId=&msisdn=       # suppressions:read
POST    /suppressions                                # suppressions:write
DELETE  /suppressions/{id}                           # suppressions:delete
POST    /suppressions/import
POST    /suppressions/check                          # -> blocked? by which scope?
GET/POST/PATCH/DELETE  /opt-out-keywords

# Inbound numbers & keywords (§6.17)
GET/POST/PATCH/DELETE  /inbound-numbers
PATCH                   /inbound-numbers/{id}/assign
GET/POST/PATCH/DELETE  /inbound-numbers/{id}/keywords
GET                     /mo/unrouted

# Sender ID rewrite rules
GET/POST/PATCH/DELETE  /sender-rewrite-rules?scope=&scopeId=
POST                    /sender-rewrite-rules/{id}/test

# Billing (proxied; "disabled" shape when the gateway module is off)
GET/PATCH               /customers/{id}/billing
GET                      /customers/{id}/balances
POST                      /customers/{id}/billing/topup     # { credits, direction: mt|mo, accountId? }
POST                      /customers/{id}/billing/transfer
POST                      /customers/{id}/billing/scope     # requires billing:scope_change (all balances zero)
GET                       /customers/{id}/billing/ledger    # ?direction= &?accountId=
GET/POST/PATCH/DELETE    /rate-plans
GET/POST/PATCH/DELETE    /billing-providers
POST                      /billing-providers/{id}/test-connection

# Content storage, gated read, RGPD erasure (§6.18/§6.19)
GET/PATCH               /platform/content-policy
GET/PATCH               /customers/{id}/content-policy
GET     /messages/{id}/content                       # decrypt + return body — content:read, audited
POST    /customers/{id}/content/erase                # content-only crypto-shred — content:erase
POST    /customers/{id}/content/rotate-key
POST    /gdpr/erase                                  # { subjectType: customer|msisdn, id } — gdpr:erase, async
GET     /gdpr/erase/{jobId}

# CDR / search / export
GET     /messages?account=&customer=&group=&status=&dateFrom=&dateTo=&connector=&cursor=&limit=
                                                     # -> search-messages ; pagination par CURSEUR, pas par numéro de page
GET     /messages/{id}                               # composé côté BFF (search-messages filtré + trace) —
                                                     # la passerelle n'expose pas de lecture unitaire de CDR
GET     /messages/{id}/trace
POST    /messages/export                             # async; cdr:export_bulk, row-cap, role-based MSISDN mask
GET     /messages/export/{jobId}

# Metrics (REST snapshot + WS stream)
GET     /metrics/summary?window=
GET     /metrics/traffic?groupBy=&window=            # connector | customer | account | group
WS      /stream                                      # multiplexed: metrics.*, sessions.events, notifications, billing.alerts
                                                     # côté passerelle, TROIS flux distincts à agréger :
                                                     # stream-metrics, stream-sessions, stream-billing-alerts

# Dashboard-owned resources
GET/POST/PATCH/DELETE  /alert-rules
GET     /notifications
POST    /notifications/{id}/read
GET/POST/DELETE        /saved-views
GET     /audit-log?operator=&targetType=&dateFrom=&dateTo=
POST    /internal/alertmanager-webhook               # server-to-server, mTLS/shared secret (§6.8)

# Operators, roles & permissions (operators:manage / roles:manage)
GET/POST/PATCH/DELETE  /operators
POST                    /operators/{id}/roles
GET                     /permissions                  # read-only catalog
GET/POST/PATCH/DELETE  /roles
```

### 5.2 Enveloppe de message WebSocket

```json
{ "topic": "metrics.traffic", "ts": "...", "data": { "mtPerSec": 8123, "moPerSec": 1987, "errorRate": 0.004 } }
{ "topic": "sessions.events", "ts": "...", "data": { "event": "connected", "sessionId": "...", "accountId": "...", "bindType": "trx" } }
{ "topic": "notifications", "ts": "...", "data": { "severity": "critical", "message": "Connector 'orange-ci' error rate above 5%", "source": "alertmanager" } }
{ "topic": "billing.alerts", "ts": "...", "data": { "customerId": "...", "direction": "mo", "event": "mo_balance_floor_reached", "creditsAccrued": 5000 } }
```

Le client envoie `{"action":"subscribe","topics":[...]}` au montage et `unsubscribe` au démontage.

---

## 6. Conception détaillée (Detailed Design)

### 6.1 UI du constructeur de routes

- Table ordonnée par priorité avec glisser-déposer ; chaque ligne résume conditions, stratégie et connecteur(s).
- Formulaire d'édition : champs structurés (compte/client/expéditeur/destination/contenu, testeur regex), menu de stratégie, éditeur de cibles adapté (poids pour `weighted`, ordre pour `failover_priority`), sélecteur de route de repli.
- Action « Simuler » : soumettre un message d'exemple et voir la route/le connecteur résolus par le matching déclaratif ; une bannière signale si le compte a un script actif (qui prévaudrait) et si un numéro exact s'applique (prioritaire).

### 6.2 Éditeur de scripts de routage personnalisés (admin)

- Monaco (JS/Lua), contrat `resolveRoute(message) -> routeId | null` documenté, aides `lookup()`/`findRouteByName()`.
- **Affectation, pas attachement** : sélecteur de portée « ce compte » / « ce client » / « plateforme entière » ; règle un-seul-script-actif-par-portée appliquée à la publication.
- **Valider / Tester** : diagnostics en ligne ; exécuteur de payload d'exemple affichant la route résolue ou « aucune correspondance ».
- **Versions & retour arrière**, **santé en direct** (invocations, latence p50/p99, taux de timeout/erreur), **messages de garde-fou** (limites du bac à sable, repli déclaratif).
- **Accès** : `scripts:write`/`scripts:publish` ; outil exclusivement du fournisseur (aucun script client nulle part).

### 6.3 Tableau de bord de trafic temps réel

- Widgets : MT/s, MO/s, taux de succès, latence p50/p99, sessions actives — adossés au sujet WS `metrics.traffic` avec instantané REST au chargement.
- Ventilations par connecteur, **client**, **compte SMPP** et **groupe** (les trois niveaux du modèle ; la ventilation par groupe somme les séries par compte, le groupe n'étant pas un label Prometheus), triables, avec drill-down vers le CDR Explorer.
- Bascule de plage (5 min / 1 h / 24 h) : plages courtes via WS, longues via instantané REST pré-agrégé.

### 6.4 CDR Explorer

- Barre de filtre (**client**, **compte SMPP**, **groupe**, date, statut, source/dest, connecteur, route) avec vues sauvegardées ; le filtre par groupe est résolu vers les clients membres courants.
- Table de résultats virtualisée, pagination côté serveur.
- Panneau de détail : chronologie complète (soumis → routé → SMSC → DLR → remis), route/script/connecteur/décision de facturation, rendu en cascade de spans.
- **Corps du message (dégradation propre)** : affiché uniquement si (a) la politique du client le stocke et (b) l'opérateur a `content:read` ; sinon un état explicite (« non stocké », « expiré », « effacé », « non autorisé »). Afficher le corps déclenche un appel `content:read` **audité** — mention « lecture journalisée » à côté du bouton.
- Export CSV de masse : job asynchrone gouverné (permission `cdr:export_bulk` hors lecture seule, plafond de lignes, masquage MSISDN par rôle, audit, TTL d'artefact).

### 6.5 Moniteur de session

- Table en direct des binds actifs (utilisateur + SMSC, par onglets), mise à jour en deltas.
- Déconnexion forcée avec confirmation, journalisée.
- **`max_sessions` par compte** : réglable depuis la page du compte, affichage « sessions vivantes / limite ». Une baisse de quota **ne coupe pas** les binds vivants (spec compagnon §6.3) ; badge d'écart explicite (« 8 vivantes / limite 4 »), forcer la convergence exige une déconnexion explicite ; avertissement avant sauvegarde si la valeur est inférieure aux sessions ouvertes.
- **Santé des connecteurs** (`get-connector-status`) : `link_status` (up|reconnecting|down) et `breaker_state` (closed|open|half_open) affichés **séparément** ; badge d'avertissement pour un connecteur s'appuyant sur le disjoncteur mais sans auto-reconnexion. La réponse détaille l'état **par bind du pool** — la vue doit donc supporter un connecteur dont certains binds sont up et d'autres non, pas un état unique par connecteur.

### 6.6 UI anti-spam & réputation

- CRUD reflétant le schéma anti-spam, aperçu « tester contre un exemple ».
- File de revue d'activité signalée (approuver/bloquer/liste blanche).
- Graphique de tendance de réputation par client, seuils d'alerte alimentant `alert_rules`.

### 6.7 UI numéros entrants, mots-clés & routage exact

- **Numéros entrants** : adresse, type, pays, connecteur, affectation dédié/partagé ; éditeur de mots-clés (numéros partagés) avec test « à quel compte irait ce MO ? » ; file **« MO non routés »** avec création de règle à la volée.
- **Routage par numéro exact** : recherche « où partirait ce numéro, et pourquoi ? » (niveau qui a décidé), **import MNP en masse** (job, reconstruction du filtre de Bloom), volume d'entrées et date du dernier import. **Bandeau de priorité** : ces règles priment sur scripts et matching déclaratif, et ne court-circuitent que la résolution de route — opt-out, autorisation d'expéditeur, anti-spam et facturation continuent de s'appliquer.

### 6.8 Alerting

**Répartition de l'évaluation entre Alertmanager et le BFF :**

- **Métriques d'infrastructure** (`connector.error_rate`, `connector.status`, débit) — `evaluation_owner = alertmanager`. Le tableau de bord est l'UI de configuration (créer/éditer une règle écrit aussi la config Alertmanager via l'API Admin) ; l'évaluation et le déclenchement ont lieu dans Alertmanager, indépendamment de la disponibilité du tableau de bord. Alertmanager notifie le BFF par webhook pour peupler le centre de notification (affichage, pas détection ; il peut aussi paginer directement).
- **Métriques de domaine métier** (`account.reputation`, `billing.mo_floor_reached`) — `evaluation_owner = bff`. N'existent pas dans Prometheus. Évaluées sur une **source durable** (topic Kafka `billing.events` ou pull réconciliateur depuis `billing-svc`) avec un **curseur/offset persisté**, de sorte qu'un redémarrage/basculement rejoue les transitions manquées au lieu de les perdre ; le flux WS sert l'affichage, jamais l'unique détection.
- **Réconciliation Alertmanager** : un job périodique vérifie que les règles `evaluation_owner=alertmanager` déclarées existent réellement dans Alertmanager et remonte toute dérive.
- Sur déclenchement, une ligne `notifications` est créée et distribuée (email/webhook/Slack), avec dédoublonnage (transition + rappel périodique pour la sévérité critique).

### 6.9 Authentification

Email/mot de passe + **MFA** : application authenticator (TOTP) et **passkey/WebAuthn** sur les appareils compatibles. Pas de dépendance à un fournisseur d'identité externe.

- La couche serveur (BFF) gère le hachage de mot de passe (protection anti-brute-force, verrouillage temporaire), l'enrôlement TOTP et les cérémonies WebAuthn, et émet sa propre session (cookie/JWT signé).
- **MFA requis pour les rôles privilégiés** ; WebAuthn/passkey privilégié quand l'appareil le supporte.
- Le client ne gère jamais les identifiants au-delà du formulaire de login ; toute la logique sensible vit côté serveur.

### 6.10 Modèle de permission & rôles par défaut

Autorisation **basée sur les permissions** : chaque action protégée correspond à une clé du catalogue (§3.1), les rôles sont des paquets nommés et éditables, un opérateur peut détenir plusieurs rôles.

**Rôles par défaut** (pré-remplis, non supprimables ; rôles personnalisés possibles) :

| Rôle | Cas d'usage | Portée |
|---|---|---|
| `super_admin` | Propriétaire | Toutes les permissions, y compris `operators:manage`/`roles:manage` |
| `ops` | Exploitation réseau | Lecture/écriture routage (dont numéros exacts, `routes:import`), connecteurs, sessions, anti-spam, scripts, réécriture, numéros entrants ; `suppressions:read/write` **sans `:delete`** ; lecture seule facturation/audit |
| `script_author` | Ingénieurs scripts | `scripts:read/write` (pas `publish` — revue par `ops`/`super_admin`) |
| `support_readonly` | Support L1 | Lecture seule (comptes, routage, sessions, CDR/trace, facturation) — **hors** secrets d'identifiants, code source de script, et **corps des messages** (`content:read` jamais implicite) |
| `billing_admin` | Finance | Facturation complète (`billing:read/write/topup/provider:write/scope_change`), lecture seule ailleurs |
| `billing_readonly` | Reporting finance | `billing:read` uniquement |
| `account_manager` | Onboarding client | `customers:read/write`, `accounts:read/write`, `credentials:read/write/rotate`, `groups:read/write` ; pas de routage/connecteur/fournisseur de facturation, pas de `billing:topup` |
| `compliance` | Conformité / juridique | `suppressions:read/write/delete`, `inbound:read`, `gdpr:erase`, lecture seule comptes/CDR. Seul rôle par défaut habilité à **lever** un désabonnement et à **exécuter un effacement RGPD**. Pas de `content:read` par défaut |
| `auditor` | Revue conformité/sécurité | `audit:read` uniquement |

### 6.11 UI de solde de crédit SMS

Section « Facturation », visible aux détenteurs d'une permission `billing:*`, uniquement quand le module est activé. Chaque nombre est un compteur entier de crédits.

- **Deux cartes distinctes** (le point pédagogique) :
  - **Solde MT** — un vrai solde (« 12 450 SMS restants »), rechargeable ; bloque à zéro en prépayé sans découvert.
  - **Compteur MO** — un compteur d'usage postpayé (« MO consommé : 3 120 crédits », qui monte), avec le plancher `mo_billing_floor` ; texte explicite : « le MO est toujours remis, un dépassement MO ne bloque jamais vos envois MT ».
- **Portée du solde (`balance_scope`)** affichée en permanence : en pool partagé, ventilation de la consommation par compte (le grand livre porte `owner_*` et `customer_id`/`account_id`) ; en par-compte, une carte MT + MO par compte.
- **Changement de `balance_scope`** : bouton visible mais inerte tant qu'un solde n'est pas à zéro, avec l'explication en ligne ; permission `billing:scope_change`.
- Recharge (entier non négatif, par direction), gestion des plans tarifaires, configuration/test du fournisseur externe, grand livre paginé filtrable par direction/compte.

### 6.12 Visualiseur de trace SMS

- Cascade de spans par étape (ingestion, autorisation sender ID, opt-out, anti-spam, routage, débit, facturation, envoi, DLR, remise), avec durée/statut/attributs (route/script/connecteur, résultat de facturation, codes d'erreur).
- Étapes en échec ou lentes signalées.
- **Le corps n'apparaît jamais** dans la trace (§6.11 compagnon).
- Consultable par lien direct pour partage dans un ticket/fil.

### 6.13 UI de règles de réécriture de sender ID (admin)

Écran CRUD (`senderrewrite:read`/`senderrewrite:write`), accessible depuis Connecteur/Client/Compte et une liste plateforme. Création consciente de la portée, éditeur (conditions, type, priorité, raison), visibilité de précédence, action de test, visibilité CDR (adresse originale vs utilisée).

### 6.14 UI de gestion des identifiants d'un compte SMPP (admin)

Écran rattaché à la page du compte SMPP (`credentials:*`).

- **Exactement deux identifiants** : deux cartes fixes « Identifiant SMPP » et « Clé API » (contrainte de schéma côté passerelle), pas une liste extensible.
- **Toujours masqué** (type, 4 derniers caractères, statut, dernière utilisation, état de rotation) ; aucune action « révéler ».
- **Création** : secret affiché une seule fois dans une modale non réaffichable.
- **Rotation manuelle** uniquement (`credentials:rotate`), avec fenêtre de grâce mise en avant et avertissement (une rotation sans grâce coupe les binds vivants du client).
- **Révocation** avec indication du nombre de sessions vivantes déconnectées ; **diagnostic d'échec de bind** (échecs d'auth récents).

### 6.15 UI de groupes de clients (admin, organisationnel)

Écran CRUD (`groups:read`/`groups:write`). Un groupe regroupe des **clients**. Liste (nom, nombre de clients membres, statut), éditeur, affectation depuis l'écran ou la page client (sélecteur unique). Suppression non destructive (détache les clients, ne supprime rien). Filtre groupe dans liste des clients, comptes, CDR Explorer, ventilation de trafic.

### 6.16 UI de désabonnement (opt-out)

Gouverné par `suppressions:read`/`suppressions:write`/`suppressions:delete`. Adossé à §6.20 compagnon.

- **Le canal est l'unité** : une suppression s'affiche « +225… s'est désabonné du 36000 (canal *Alertes Banque X*) », avec origine (`mo_stop`/`admin`/`import`/`regulator`). Portées plus larges affichées comme telles.
- **Outil « pourquoi bloqué ? »** : destinataire + expéditeur + compte → bloqué ou non, et **par quelle portée**.
- **Levée de suppression = action à part** : bouton et permission distincts (`suppressions:delete`), confirmation, audit — réautoriser l'envoi vers un désabonné est l'acte à risque juridique.
- Import en masse avec compte-rendu ; mots-clés par pays (STOP/START/HELP + gabarits).
- **Avertissement structurel** : signale les comptes n'envoyant que depuis des expéditeurs alphanumériques sans numéro entrant — ils n'ont aucun moyen de recevoir un désabonnement.

### 6.17 UI numéros entrants & mots-clés

Voir §6.7.

### 6.18 Politique de contenu & effacement RGPD

Deux niveaux (`content:read` pour lire un corps ; `content:erase` et `gdpr:erase` pour effacer), cohérents avec §6.14/§6.23 compagnon.

- **Politique plateforme** (admin) et **par client** (`off`/`stored_plaintext`/`stored_encrypted`/`inherit`, `content_retention_days`), chaque option accompagnée de sa conséquence en clair.
- **Honnêteté** : l'écran indique que le chiffrement protège le repos et que `content:read` reste la frontière d'accès.
- **Effacement du contenu seul** (`content:erase`) : « détruit la clé — contenu illisible, métadonnées conservées, irréversible ».
- **Effacement RGPD complet** (`gdpr:erase`) avec choix de cible : **client** (crypto-shred + purge, avertit si le grand livre doit être conservé pour obligation fiscale) ou **personne / MSISDN** (suppression ciblée across clients, job asynchrone + attestation, opt-out conservé).
- **Journal des accès au contenu** : vue dédiée des lectures de corps (qui, quel message, quand).

---

## 7. Évaluation (Evaluation)

| Décision | Compromis |
|---|---|
| Une couche BFF vs le client appelant directement l'API Admin | Centralise permissions, audit et diffusion WS, garde la surface admin hors d'Internet public. |
| BFF = fonctions serveur TanStack (un déployable) vs service séparé | Moins de pièces, sécurité de type, dev local simple ; le BFF ne scale pas indépendamment du rendu et impose une cible Node auto-hébergée. |
| Topologie HA multi-instance + pub/sub de diffusion | Rend la cible 99,9 % atteignable (plus de SPOF, déploiements sans coupure) au prix d'une dépendance Redis Pub/Sub. |
| Répartition Alertmanager (infra) / BFF (métier) pour l'alerting | La détection d'incident infra ne dépend pas de la disponibilité du tableau de bord ; les alertes métier sont évaluées sur source durable à offset persisté (pas de perte au redémarrage), avec réconciliation du write-through Alertmanager. |
| Auth email/mot de passe + MFA (TOTP + passkey), sans IdP externe | Aucun service d'identité à exploiter ni verrouillage fournisseur ; le BFF porte le code sensible (hachage, WebAuthn), mitigé par MFA obligatoire pour les rôles privilégiés. |
| Navigation à deux niveaux client → comptes SMPP, permissions `customers:*`/`accounts:*` séparées | Reflète le modèle de domaine ; onboarder un client et provisionner un compte technique sont deux actes distincts. |
| Solde MT et compteur MO présentés comme deux objets différents | Empêche le malentendu « le MO bloque comme le MT » ; l'UI le rend explicite (le MT bloque à zéro, le MO monte et ne bloque rien). |
| Lecture de contenu derrière `content:read` (hors lecture seule, auditée) + dégradation propre | Le corps (OTP/PII) est la lecture la plus sensible ; jamais implicite, chaque accès tracé, états explicites plutôt qu'un blanc. |
| Effacement RGPD avec choix de cible (client vs personne) | Un droit à l'oubli exécutable et prouvable ; l'UI expose les deux cibles et leurs conséquences plutôt qu'un bouton unique trompeur. |
| `suppressions:delete` séparée + rôle `compliance` | Réautoriser un désabonné est l'acte à risque juridique ; réservé à un rôle dédié, pas à l'exploitation courante. |
| Bandeau de priorité sur le routage exact | Empêche de croire qu'une route « directe » contourne la conformité ; le court-circuit ne saute que la résolution. |
| Éditeur Monaco intégré avec test/validation | Itération rapide sans cycle de déploiement ; mitigé par RBAC et garanties de bac à sable côté passerelle. |
| Autorisation fine (permissions) vs rôles fixes | Supporte des organisations hors des préréglages sans changement de code ; coûte plus de modélisation en amont. |
| UI de facturation en proxy fin | Garde le tableau de bord sans état, pas de copie de données financières ; coûte un aller-retour réseau par chargement. |
| Formulaire de connecteur à divulgation progressive | Correspond au « peu de champs requis, le reste par défaut » ; un formulaire plat serait accablant pour le cas courant. |

**Ce qu'on revisiterait à mesure que le système grandit :** au-delà de ~300 opérateurs, revisiter la diffusion WS (déjà adossée à Redis Pub/Sub) ; si l'usage de script grandit fortement, un environnement de test dédié et un workflow d'approbation plus strict ; si le volume d'alertes métier pèse sur le BFF, les migrer vers un pipeline plus proche de la source.

---

## 8. Composant distinctif (Distinctive Component)

**Cockpit d'exploitation unifié avec IDE de script de routage intégré, conformité de premier plan et alerting à disponibilité stratifiée.**

La fonctionnalité distinctive est de rassembler des workflows normalement séparés — configuration de route déclarative, développement de script de routage, observabilité de trafic/session en direct, conformité (opt-out, autorisation d'expéditeur, effacement RGPD) — en une seule surface : un opérateur peut observer une anomalie, creuser dans le CDR Explorer, ouvrir la route responsable, et sauter dans l'éditeur Monaco pour écrire/tester/publier un script, sans quitter le tableau de bord ni attendre un déploiement, tandis que la santé par script boucle la boucle.

Cette boucle s'étend à la facturation et au traçage : chaque message porte un ID de trace et, s'il est facturable, une décision de facturation ; un opérateur peut aller d'une anomalie à la cascade de spans complète, à voir si une réservation a été rejetée — dans un seul outil. Le modèle de permission rend cette visibilité large sans risque : un `support_readonly` obtient la même profondeur d'investigation qu'un `super_admin`, sans capacité de changement, et sans jamais voir un secret d'identifiant ni le **corps d'un message** auquel il n'a pas explicitement droit.

La **stratification de la fiabilité d'alerting** fait que les signaux d'infrastructure sont évalués et notifiés indépendamment de la disponibilité du tableau de bord, tandis que les alertes métier reposent sur une source durable à offset persisté — le tableau de bord reste où l'on configure et consulte, sans être un point de défaillance unique pour la détection. La topologie HA multi-instance ferme le dernier SPOF : la fiabilité de l'outil d'exploitation n'est pas inférieure à celle du système qu'il supervise.