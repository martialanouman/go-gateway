# Passerelle SMS — Spécification Technique
**Modèle :** RESHADED (Requirements → Estimation → Storage Schema → High-Level Design → API Design → Detailed Design → Evaluation → Distinctive Component)
**Composant :** Passerelle SMS principale (Go)
**Statut :** v2.0

*Note de convention : les blocs de code (schémas DDL, endpoints API, diagrammes, JSON, y compris leurs commentaires) restent en anglais, conformément aux conventions de code de l'équipe. Seul le texte narratif est en français.*

---

## 1. Exigences (Requirements)

### 1.1 Exigences fonctionnelles

- **Modèle client / compte SMPP** — un **client** (`customers`) détient **un ou plusieurs comptes SMPP** (`smpp_accounts`). Chaque compte possède exactement un identifiant de bind SMPP et exactement une clé API. La facturation (solde de crédits, plan tarifaire, découvert), les sender IDs et l'appartenance à un groupe sont portés par le **client** ; les quotas/limites de débit, sessions et webhooks par le **compte SMPP** (§6.18).
- **Gestion des clients et comptes (admin uniquement)** — CRUD pour les clients et leurs comptes SMPP (comptes ESME), leurs identifiants (bind SMPP + clé API, rotation manuelle), canaux, quotas, webhooks, sender IDs et facturation. Les clients n'ont aucun accès à la plateforme : chaque sous-ressource est provisionnée et gérée exclusivement par le fournisseur via l'API/Tableau de bord Admin (onboarding B2B). Il n'existe aucun portail ni surface API en libre-service.
- **Groupes de clients (admin uniquement, organisationnel)** — regroupement de clients pour la segmentation (filtrage, reporting, analytique). Structure plate, un client appartient à zéro ou un groupe. Un groupe ne porte ni solde, ni quota, ni règle de configuration : c'est une dimension d'organisation, jamais un niveau d'héritage (§6.17).
- **Connectivité SMPP vers le SMSC (sortant)** — la passerelle agit comme *client* SMPP (ESME) vis-à-vis d'un ou plusieurs SMSC opérateurs : gestion des binds (TX/RX/TRX), plusieurs connecteurs avec bascule, répartition de charge, pool de binds par connecteur (§6.8), et un disjoncteur par connecteur (§6.15).
- **Serveur SMPP (entrant)** — la passerelle agit comme *serveur* SMPP, acceptant les binds des clients ESME pour qu'ils soumettent/reçoivent des SMS. Supporte `submit_sm`, `deliver_sm`, `enquire_link`, `unbind`, et `query_sm` / `cancel_sm` (désactivables par compte, §6.22).
- **API REST** — endpoints HTTP pour soumettre des SMS MT, interroger le statut de ses propres messages, et recevoir MO/DLR via webhook. Volontairement minimale : aucune configuration en libre-service.
- **SMS MT (Mobile Terminated)** — messages des clients (SMPP ou REST), routés vers un SMSC.
- **SMS MO (Mobile Originated)** — messages arrivant d'un SMSC, routés vers le bon compte SMPP via le numéro entrant/mot-clé (§6.21). Facturé sur un compteur MO distinct (§6.9).
- **Moteur de routage** — sélection du connecteur SMSC sortant en trois niveaux de priorité décroissante (§6.1) : (0) correspondance de numéro exact (portabilité/MNP, prioritaire et court-circuitant), (1) script de routage admin, (2) matching déclaratif (compte, client, sender ID, préfixe MSISDN/MCC-MNC, contenu, heure, priorité), avec route de repli.
- **Autorisation de sender ID (admin uniquement)** — le `source_addr` soumis doit correspondre à un sender ID approuvé du client, selon la politique du compte (§6.19).
- **Règles de réécriture de sender ID (admin uniquement)** — réécriture à la volée de l'adresse source, à portée plateforme/client/compte/connecteur, évaluée juste avant l'envoi (§6.16).
- **Scripts de routage personnalisés (admin uniquement)** — le fournisseur peut écrire un script isolé (JavaScript ou Lua) qui reçoit les données d'un SMS et retourne l'ID d'une route, en alternative au matching déclaratif. Scopé compte/plateforme, jamais attaché à une route, exécuté sous limites strictes (§6.2).
- **Désabonnement (opt-out / STOP)** — liste de suppression scopée au canal (numéro entrant) ; une étape MT bloquante empêche l'envoi vers un destinataire désabonné (§6.20).
- **Numéros entrants & mots-clés** — shortcodes/long codes détenus par le fournisseur, assignés à un compte SMPP ou résolus par mot-clé, servant de source de vérité au routage MO et à l'opt-out (§6.21).
- **Gestion des sessions SMPP** — cycle de vie des binds dans les deux sens : authentification, keep-alive `enquire_link`, application fenêtre/débit par session, `max_sessions` par compte, unbind gracieux, déconnexion forcée (§6.3).
- **Gestion du débit** — limitation par compte SMPP, connecteur et route ; fenêtre glissante SMPP ; backpressure et mise en file quand l'aval est saturé (§6.4).
- **Anti-spam / filtrage de contenu** — moteur de règles configurable : vélocité (MT et MO entrant), listes noires de contenu, détection de doublons, score de réputation par client (§6.5).
- **Accusés de réception (DLR)** — capture des rapports de remise du SMSC, transmission au compte d'origine via SMPP `deliver_sm` ou webhook, corrélés par ID de message.
- **SMS long / concaténé & encodage** — segmentation/réassemblage UDH, encodage GSM-7, UCS-2, 8-bit (§6.6).
- **Métriques et supervision temps réel** — compteurs de trafic en direct exposés pour tableaux de bord et alerting.
- **Traitement asynchrone** — la réception est découplée de l'envoi : l'ingestion place durablement le message en file (Kafka) et accuse réception immédiatement ; routage, facturation et envoi hors bande. Redis/Dragonfly fournit la file à faible latence et le cache (§6.12).
- **Reconnexion automatique SMPP (opt-in)** — reconnexion configurable par connecteur avec backoff quand un bind côté SMSC tombe ; désactivée par défaut (§6.13).
- **Traçabilité complète des SMS** — chaque message reçoit un ID de trace propagé à travers chaque étape, interrogeable de bout en bout. Le corps du message n'apparaît jamais dans une trace ni un log (§6.11).
- **Scalabilité horizontale** — le socle de traitement est sans état et s'étend horizontalement sous charge (§6.8).
- **Module de solde SMS (optionnel, opt-in)** — facturation prépayée/postpayée en **crédits SMS entiers**, avec **soldes MT et MO séparés** et un propriétaire de solde configurable par client (pool partagé ou par compte SMPP, §6.9). Entièrement optionnel : désactivé, le pipeline tourne sans latence ni dépendance ajoutées.
- **Stockage & chiffrement du contenu (configurable)** — le corps d'un message est stocké ou non selon une politique (défaut plateforme, surcharge par client) ; s'il est stocké, il l'est chiffré avec une clé par client, lisible uniquement depuis le tableau de bord sous permission dédiée (§6.23).
- **Effacement RGPD à la demande** — droit à l'oubli exécutable manuellement, ciblant un client entier ou une personne (MSISDN) (§6.14).
- **API Admin** — API interne consommée par le Tableau de bord Admin (distincte de l'API REST publique).

### 1.2 Exigences non fonctionnelles

| Catégorie | Cible |
|---|---|
| Disponibilité | 99,95 % par région (≈4,4 h d'indisponibilité/an) |
| Latence d'ingestion | p50 < 50 ms, p99 < 250 ms (soumission → mise en file) |
| Latence bout-en-bout (soumission → tentative de remise SMSC) | p50 < 400 ms, p99 < 2 s en charge nominale (connecteur non saturé, disjoncteur fermé) ; ne s'applique pas au backpressure/dead-letter délibérés (§6.7) |
| Débit | Soutenu 5 000–10 000 SMS/s, pic à 15 000 SMS/s |
| Durabilité | Aucune perte de message après accusé de réception (remise au SMSC au moins une fois) |
| Scalabilité | Extension horizontale linéaire des niveaux sans état ; les connecteurs s'étendent indépendamment |
| Sécurité | TLS pour REST/SMPP-TLS, hachage des identifiants, mTLS optionnel pour les liens SMSC, piste d'audit |
| Multi-tenant | Isolation stricte des données, quotas et limites de débit par compte |
| Conformité | Traitement RGPD, gestion des mots-clés d'opt-out/STOP, effacement à la demande, rétention configurable (90 j par défaut) |
| Observabilité | Métriques temps réel (<5 s), logs structurés, tracing distribué. **Le corps des messages n'apparaît jamais dans un log ni une trace**, quelle que soit la politique de stockage (§6.11/§6.23) |
| Traçage des messages | 100 % de couverture de trace sur toutes les étapes, interrogeable en quelques secondes |
| Surcoût de vérification de crédit (si activé) | La réservation atomique prépayée ajoute < 5 ms p99 au traitement interne ; surcoût nul si désactivé |

### 1.2bis Non-objectifs (hors périmètre)

- **Campagnes** (listes de diffusion, gestion de destinataires, modèles, suivi côté client) : hors périmètre. La plateforme est une passerelle — elle reçoit, traite et route. La composition de campagne appartient à l'application cliente ou à un produit au-dessus. Le pic de débit provient de la charge agrégée des clients.
- **Envoi programmé / différé côté client** : hors périmètre. La passerelle envoie quand elle reçoit (le seul délai est opérationnel : file, retry, backpressure, `validity_period` SMPP). Aucune entité `campaign`, `recipient_list` ou `scheduled_message` n'existe.
- **Reprise après sinistre / continuité d'activité** (RPO/RTO, bascule inter-région, réplication) : relève de l'exploitation infrastructure, non spécifié ici. Le multi-région (§6.8) est traité pour la latence uniquement.
- **Quiet hours** (plages horaires réglementaires) : reporté à une version ultérieure — une application correcte suppose une classification du trafic (marketing/transactionnel/OTP) qui n'est pas fiable sans modèle ML.

### 1.3 Contraintes

- Langage d'implémentation : Go (le modèle une-goroutine-par-connexion convient aux sessions persistantes SMPP).
- Doit exposer simultanément un serveur SMPP v3.4/5.0 et des interfaces REST/HTTP sur le même pipeline central (aucune duplication de logique).
- Doit interopérer avec des SMSC opérateurs hétérogènes (variantes SMPP) via une abstraction de connecteur.
- Déployé sous forme de services conteneurisés (Kubernetes) : tout état de session est externalisé ou l'affinité gérée explicitement.
- Le module de facturation est entièrement optionnel : désactivé par défaut, activable globalement et par client ; son absence ou sa panne ne bloque jamais l'envoi. Le mode fail-open est le défaut ; le fail-closed est une option par plateforme/client.
- La reconnexion automatique SMPP est opt-in par connecteur. Le disjoncteur (§6.15) ne réalise aucune reconnexion : il bloque/autorise l'envoi sur une connexion supposée active.
- Les clients ne s'authentifient jamais contre l'API/Tableau de bord Admin. La surface publique se limite à envoyer, recevoir et consulter le statut de ses propres messages.
- **Identifiants** : chaque ID d'entité est un **UUIDv7** (RFC 9562), trié chronologiquement, ce qui garde les index Postgres et l'élagage de partition ClickHouse efficaces. `message_id` et `trace_id` sont exposés au client mais ne sont jamais des secrets, sont scopés par compte côté API publique, et ne permettent pas l'énumération inter-comptes grâce à leur composante aléatoire. Les tables du plan de contrôle utilisent le générateur UUIDv7 natif de Postgres 18 ; `message_id` et `trace_id` sont générés côté application à l'ingestion (ils servent de clés/en-têtes Kafka avant toute écriture en base).

---

## 2. Estimation

Cible de conception : **échelle grande production / agrégateur national.**

### 2.1 Trafic
- Débit soutenu : **8 000 SMS/s** en moyenne, **15 000 SMS/s** en pic (charge agrégée des clients aux heures de pointe).
- Volume quotidien : enveloppe haute théorique ~690 M/jour au débit soutenu ; courbe diurne réaliste ~150–250 M msgs/jour.
- Ratio MT:MO : ~80:20 typique.
- Taille moyenne : 1,3 segment (SMS concaténés) → prévoir le surcoût de segmentation UDH.

### 2.2 Connexions
- Binds SMPP côté utilisateur simultanés : 5 000–20 000 sessions.
- Binds SMPP côté SMSC simultanés : dizaines à basses centaines (pool de binds par connecteur, §6.8).
- Connexions API REST simultanées : 10 000+ (HTTP/2 ou keep-alive avec pool).

### 2.3 Stockage & rétention
- Métadonnées de message (CDR) : ~0,5–1 Ko chacun. Le corps, quand stocké (§6.23), s'ajoute (~40–1 000 o) mais avec une rétention propre plus courte.
- À 250 M msgs/jour × 0,8 Ko ≈ **200 Go/jour** de CDR → ~18 To pour 90 jours (avant compression/tiering). Stockage optimisé en écriture avec TTL/tiering (§6.14).
- Séries temporelles de métriques : cardinalité (compte × connecteur × route) — dizaines de milliers de séries actives, scrape 10–15 s.

### 2.4 Bande passante
- PDU SMPP moyenne ~150–300 o ; à 8 000/s × 2 sens × ~250 o ≈ 4 Mo/s minimum par sens, doublant avec le DLR → ~20–40 Mbps soutenu, marge de pic 100+ Mbps.

### 2.5 Dimensionnement du calcul (règle empirique)
- Workers sans état : ~1 vCPU soutient 1 000–2 000 msg/s de routage+anti-spam+encodage → **8–16 vCPU** en charge soutenue, auto-scalé 2–3× pour le pic. Un compte avec script de routage a une enveloppe propre, inférieure (§6.2).
- Broker Kafka dimensionné pour 8 000–15 000 msgs/s en écriture, réplication 3.

### 2.6 Facturation & traçage (si activés)
- Grand livre : jusqu'à 1 entrée par message, MT et MO facturables → volume comparable au CDR (~250 M/jour à pleine adoption) → magasin partitionné avec tiering, pas une simple table relationnelle (§6.14).
- Spans de trace : ~6–8 spans enfants par message → jusqu'à ~120 000 spans/s au pic ; backend dimensionné avec échantillonnage head/tail-based, 100 % pour les messages en échec.

---

## 3. Schéma de stockage (Storage Schema)

Persistance polyglotte : magasin relationnel pour la configuration (forte cohérence), cache clé-valeur rapide pour l'état de session/débit/réservation (Redis ou Dragonfly, compatible protocole Redis), broker de messages pour le plan de données, magasin analytique/columnar pour métriques et CDR.

### 3.1 Plan de contrôle (PostgreSQL 18)

Toutes les clés primaires `id` utilisent `uuidv7()` sauf mention contraire.

```
-- =====================================================================================================
-- DOMAIN MODEL — a CUSTOMER owns one or more SMPP ACCOUNTS.
--   customer_groups 1 ─ N customers 1 ─ N smpp_accounts 1 ─ 2 credentials (1 smpp_bind + 1 api_key)
-- Level ownership (§6.18):
--   CUSTOMER : billing config, balances (via balance_scope), sender IDs, group, reputation
--   ACCOUNT  : SMPP bind identity + API key, channels, rate limits/quotas, max_sessions, webhooks
-- =====================================================================================================

customer_groups                       -- organizational segmentation of customers (§6.17). Flat, 0..1 per customer.
  id (uuidv7, pk)
  name (unique), description
  status               (active|archived)
  created_by (fk -> operators, dashboard schema), created_at, updated_at

customers
  id (uuidv7, pk)
  name
  status               (active|suspended|closed)   -- suspending a customer suspends all its smpp_accounts (§6.18)
  group_id (fk -> customer_groups, nullable)        -- ON DELETE SET NULL
  -- Billing CONFIG (balances live in `balances`)
  rate_plan_id (fk, nullable)
  billing_enabled      (bool, default false)        -- opt-in master switch for this customer
  billing_mode         (nullable: prepaid|postpaid) -- MT balance only; MO is always a postpaid meter (§6.9)
  overdraft_enabled    (bool, default false)        -- prepaid MT only
  overdraft_limit      (integer, nullable)           -- max negative MT balance in SMS credits
  balance_scope        (customer|smpp_account, default customer)  -- who owns the balances (§6.9). Set at creation;
                                        -- changeable only by an audited op requiring ALL balances to be zero.
  mo_billing_floor     (integer, nullable)          -- how negative the MO meter may run before accrual stops + alert
  -- Content storage policy (§6.23)
  content_storage      (inherit|off|stored_plaintext|stored_encrypted, default inherit)  -- governs CDR storage only;
                                        -- logs/traces NEVER carry the body under any value (§6.11)
  content_retention_days (integer, nullable)        -- body retention, decoupled and typically shorter than CDR
  content_key_id       (fk -> content_keys, nullable)  -- active data key for stored_encrypted; destroy = crypto-shred
  created_at, updated_at

smpp_accounts                         -- one technical account of a customer (e.g. per app/env/brand)
  id (uuidv7, pk)
  customer_id (fk -> customers)        -- REQUIRED
  name                                  -- unique within the customer
  status               (active|suspended|closed)   -- effective status = min(customer.status, this)
  smpp_enabled         (bool, default true)        -- may open SMPP binds?
  rest_enabled         (bool, default true)        -- may call the REST API?
  CHECK (smpp_enabled OR rest_enabled)
  sender_id_policy     (strict|allow_unregistered_numeric|disabled, default strict)  -- sender-ID authorization (§6.19)
  query_sm_enabled     (bool, default true)        -- optional SMPP op (§6.22)
  cancel_sm_enabled    (bool, default true)
  allowed_bind_types   (tx|rx|trx)
  max_sessions                          -- simultaneous SMPP binds, enforced at bind time via session-manager-svc (§6.3)
  created_at, updated_at

credentials                           -- exactly TWO rows per smpp_account: one smpp_bind, one api_key
  id (uuidv7, pk)
  account_id (fk -> smpp_accounts)
  type                 (smpp_bind | api_key)
  UNIQUE (account_id, type)             -- the cardinality rule, enforced by the schema
  system_id            (nullable, set for smpp_bind)
  password_hash        (nullable, set for smpp_bind)
  api_key_hash         (nullable, set for api_key)
  status               (active|disabled|revoked)
  last_used_at         (nullable)
  previous_secret_hash (nullable)        -- set only during a manual rotation grace window (§6.3)
  grace_expires_at     (nullable)
  created_at, rotated_at
  -- Secrets are never returned in clear after creation/rotation; only masked. Rotation is manual (§6.3/§6.18).

sender_ids                          -- CUSTOMER-level; carrier/regulatory registration negotiated once per customer
  id (uuidv7, pk)
  customer_id (fk -> customers)
  address              (alphanumeric or MSISDN)
  status               (pending_carrier_approval|active|disabled)
  created_by (fk -> operators), approved_at
  -- Enforced at ingestion by sender-ID authorization (§6.19).

suppressions                        -- opt-out list, PER CHANNEL (§6.20)
  id (uuidv7, pk)
  scope                (inbound_number|smpp_account|customer|platform)  -- default for an MO STOP is inbound_number
  scope_id             (fk -> inbound_numbers.id | smpp_accounts.id | customers.id; null for platform)
  msisdn               (E.164, normalized at write)
  source               (mo_stop|admin|import|carrier|regulator)
  reason               (nullable), created_at
  UNIQUE (scope, scope_id, msisdn)
  -- MT enforcement blocks if ANY applicable scope matches (platform OR customer OR account OR inbound_number).

opt_out_keywords                    -- per country/locale; seeded defaults, admin-editable
  id (uuidv7, pk)
  country_code (nullable), keyword    -- STOP, ARRET, START, UNSTOP, HELP...
  action               (suppress|unsuppress|help)
  match_type           (exact|prefix, default exact)
  auto_reply_template  (nullable)     -- auto-reply is an MT, never billed (§6.20)
  status

inbound_numbers                     -- shortcodes / long codes owned by the provider (§6.21)
  id (uuidv7, pk)
  address, number_type (shortcode|longcode|alphanumeric)
  country_code, mccmnc (nullable)
  connector_id (fk -> smsc_connectors, nullable)   -- which carrier link delivers MO for this number
  account_id (fk -> smpp_accounts, nullable)       -- dedicated: all MO -> this account. NULL = shared (keyword-resolved)
  status               (active|disabled)
  created_at, updated_at
  UNIQUE (address, country_code)

inbound_keywords                    -- for SHARED inbound numbers only
  id (uuidv7, pk)
  inbound_number_id (fk -> inbound_numbers)
  keyword, match_type  (exact|prefix|regex, default prefix)
  account_id (fk -> smpp_accounts)
  priority (int), status

exact_routes                        -- MSISDN -> target; typically bulk-loaded from an MNP/portability database (§6.1)
  msisdn               (E.164, PRIMARY KEY)
  target_type          (connector|route)
  target_id            (fk -> smsc_connectors.id | routes.id)
  source               (mnp_import|manual|carrier_feed)
  imported_at, updated_at

smsc_connectors                     -- name/host/port/bind_type/system_id/password_hash required; all else defaulted
  id (uuidv7, pk)
  name, host, port
  bind_type            (tx|rx|trx)
  system_id, password_hash
  vendor_profile       (nullable)     -- optional preset pre-filling the fields below; explicit values override
  system_type          (default '')
  interface_version    (default 0x34 = SMPP v3.4; 0x50 = v5.0)
  addr_ton (default 0), addr_npi (default 1), address_range (default '')
  source_addr_ton (default 5), source_addr_npi (default 0)
  dest_addr_ton (default 1), dest_addr_npi (default 1)
  data_coding_default  (nullable)     -- per-connector coding override (§6.6); null = auto-detected
  registered_delivery_default (default 1 = request DLR)
  replace_if_present_flag_default (default 0), esm_class_default (default 0)
  priority_flag_default (default 0), validity_period_default (nullable), sm_default_msg_id (default 0)
  enquire_link_interval_sec (default 30), enquire_link_max_missed (default 3)
  bind_timeout_ms (default 5000), response_timeout_ms (default 5000)
  window_size          (default 10)   -- max outstanding unacked submit_sm per bind
  bind_pool_size       (default 1, max 32)  -- independent parallel SMPP binds for this connector (§6.8)
  throughput_limit_per_sec (nullable) -- absolute technical ceiling (e.g. carrier contract)
  tls_enabled (bool, default false), tls_config_json (nullable)
  priority_tier        (default 0)
  status               (active|degraded|disabled)   -- coarse config status. Live health is reported via two separate
                                        -- runtime fields: link_status (up|reconnecting|down) and
                                        -- breaker_state (closed|open|half_open) — never conflated (§6.13/§6.15)
  auto_reconnect_enabled (bool, default false)      -- opt-in (§6.13); UI/API warn if a breaker-reliant connector
                                        -- leaves this false
  reconnect_initial_delay_ms (default 1000), reconnect_multiplier (default 2.0)
  reconnect_max_delay_ms (default 60000), reconnect_jitter_pct (default 20), reconnect_max_attempts (default 0=infinite)

routes
  id (uuidv7, pk)
  name, priority (int, lower = evaluated first)
  match_account_id     (nullable, fk -> smpp_accounts)
  match_customer_id    (nullable, fk -> customers)   -- match every account of a customer
  match_sender_pattern (nullable, regex/glob)
  match_dest_pattern   (nullable, MSISDN prefix / MCC-MNC)
  match_content_pattern(nullable, regex/keyword)
  distribution_strategy (static|round_robin|weighted|failover_priority|least_loaded|hash_based, default static) -- §6.1
  target_connector_id  (fk, nullable)  -- used only when distribution_strategy = static
  fallback_route_id    (nullable, self-fk)
  status

route_targets                       -- used when distribution_strategy != static; >= 2 rows
  route_id (fk), connector_id (fk)
  weight               -- used by 'weighted'
  priority             -- used by 'failover_priority'

routing_scripts                     -- admin-authored; NOT bound to a route (§6.2)
  id (uuidv7, pk)
  scope                (platform|customer|smpp_account)
  scope_id             (nullable fk, matching scope; null for platform)
                       -- partial unique index: at most one active script per (scope, scope_id)
                       -- resolution: smpp_account -> customer -> platform, first match wins
  name, language (js|lua), source_code (text, versioned), checksum
  status               (draft|active|disabled)
  timeout_ms (default 2, hard cap ~20), max_instructions, max_memory_kb
  created_by (fk -> operators), created_at, published_at

rate_limits                         -- operational governor, always below the connector's technical ceiling (§6.4)
  entity_type          (smpp_account|connector|route)   -- no customer/group level (§6.18)
  entity_id
  max_per_sec          -- for connector: MUST be <= smsc_connectors.throughput_limit_per_sec when set
  max_per_day, burst_capacity

antispam_rules
  id (uuidv7, pk)
  rule_type            (velocity|content_blacklist|duplicate|reputation)
  scope                (global|customer|smpp_account)   -- resolution: smpp_account -> customer -> global
  config_json
  action               (block|flag|throttle)
  status

webhooks                            -- per SMPP ACCOUNT (§6.18)
  id (uuidv7, pk)
  account_id (fk -> smpp_accounts)
  event_type           (mo|dlr)
  url, secret, retry_policy_json

sender_id_rewrite_rules             -- admin-managed (§6.16)
  id (uuidv7, pk)
  scope                (platform|customer|smpp_account|connector)
  scope_id             (nullable fk, matching scope; null for platform)
  direction            (mt|mo, default mt)
  match_sender_pattern (nullable), match_dest_pattern (nullable)
  rewrite_type         (static|fallback_pool|truncate|sanitize)
  rewrite_to           (used when static)
  fallback_pool_json   (round-robin list, used when fallback_pool)
  max_length (nullable), sanitize_charset_json (nullable)
  priority (int), reason (free text), status (active|disabled)
  created_by (fk -> operators), created_at, updated_at

rate_plans                          -- SMS balance is a plain integer count, never monetary
  id (uuidv7, pk), name
  credits_per_segment_mt_json   -- integer credits per MT segment, by destination MCC-MNC/country and sender type
  credits_per_segment_mo_json   -- integer credits per MO segment
  billing_mode         (prepaid|postpaid|either)
  charge_on            (submission|delivery, default submission)  -- delivery -> a failed/expired DLR triggers a refund
  status

balances                            -- THE balance table. One row per (owner, direction).
  owner_type  (customer|smpp_account)   -- decided by customers.balance_scope
  owner_id    (uuidv7)                  -- fk -> customers.id OR smpp_accounts.id
  direction   (mt|mo)                   -- MT and MO balances are separate (§6.9)
  PRIMARY KEY (owner_type, owner_id, direction)
  credits     (integer, not null, default 0)   -- always an integer count, never monetary
  updated_at

billing_customers                   -- billing config per customer (balances live in `balances`)
  customer_id (pk, fk -> customers)
  billing_mode         (prepaid|postpaid)   -- MT only
  overdraft_enabled (bool), overdraft_limit (integer, nullable)
  credit_limit (integer, nullable)          -- postpaid MT soft-limit for alerting
  credit_limit_is_hard (bool, default false)
  external_billing_provider_id (fk, nullable)
  updated_at

billing_ledger                      -- append-only, partitioned by day (§6.14)
  id (uuidv7, pk)
  owner_type, owner_id, direction     -- which balance moved (mirrors `balances`)
  customer_id (fk), account_id (fk -> smpp_accounts, nullable)  -- attribution even for a shared pool
  message_id (fk, nullable)           -- null for manual top-ups/adjustments
  entry_type           (reserve|capture|release|refund|topup|adjustment)
  credits (integer, not null, signed), balance_after (integer, not null)
  reference (nullable), created_at
  UNIQUE (message_id, entry_type) WHERE message_id IS NOT NULL   -- idempotency under at-least-once delivery (§6.9)

content_keys                        -- per-customer data keys for content encryption at rest (§6.23)
  id (uuidv7, pk)
  customer_id (fk -> customers)
  wrapped_key (bytea)                 -- KMS-wrapped data key; plaintext exists only transiently in memory
  kms_key_ref (text)
  status               (active|retired|destroyed)   -- destroyed = crypto-shred (RGPD erasure)
  created_at, retired_at, destroyed_at

external_billing_providers
  id (uuidv7, pk), name, base_url, auth_config_json
  mode                 (balance_check|consume_delegate_async|consume_delegate_sync|both)
  cache_ttl_ms, sync_call_timeout_ms (nullable)
  failure_policy       (fail_open|fail_closed)
  status
```

**Le solde SMS n'est pas une somme monétaire.** C'est toujours un compteur entier de crédits SMS. Un SMS concaténé consomme plus d'un crédit : `rate_plans` tarifie **par segment**, donc le coût d'un message est `segment_count × credits_per_segment(destination, type d'expéditeur)`. La réservation de crédit a donc lieu après la segmentation et après la vérification de limite de débit (§6.9).

**Répartition des niveaux (§6.18).**

| Niveau **client** | Niveau **compte SMPP** |
|---|---|
| Config de facturation (plan tarifaire, prépayé/postpayé MT, découvert, `mo_billing_floor`, `balance_scope`) | Identifiant de bind SMPP + une clé API |
| Sender IDs (enregistrement opérateur négocié une fois par client) | Activation des canaux (`smpp_enabled`/`rest_enabled`) |
| Politique et clé de stockage de contenu | Politique d'autorisation de sender ID, bascules `query_sm`/`cancel_sm` |
| Appartenance à un groupe | Limites de débit/quotas, `max_sessions`, types de bind |
| Réputation anti-spam | Webhooks MO/DLR |

Les **soldes** vivent dans `balances`, clé `(owner_type, owner_id, direction)` ; le propriétaire suit `balance_scope`. MT et MO sont séparés (§6.9).

**Règle de validation** (rate_plans/customers) : l'affectation d'un `rate_plan` à un client est refusée si `rate_plans.billing_mode` (`prepaid`/`postpaid`) ne correspond pas au `customers.billing_mode`, sauf `either`.

### 3.2 État de session / débit / réservation (Redis ou Dragonfly, mode cluster)

```
session:{bind_id}                    -- session metadata (account_id, bind_type, connected_at, window_size,
                                         last_enquire_link) with TTL heartbeat
ratelimit:{entity_type}:{entity_id}:{window}   -- atomic token-bucket counters (Lua)
dedupe:{account_id}:{content_hash}   -- short-TTL set for duplicate-message anti-spam
reputation:{account_id}              -- rolling spam-score, decayed
exactroute:{msisdn}                  -- exact-number routing CACHE over exact_routes; read only on a Bloom-filter
                                        possible-hit, populated by the reader on a miss (read-through, TTL 6h ±10%),
                                        DELeted by the Admin API after its commit. Never a source of truth (§6.1)
suppress:{scope}:{scope_id}:{msisdn} -- opt-out entry; read only on a Bloom-filter possible-hit (§6.20)
billing:balance:{direction}:{owner_type}:{owner_id}   -- cached balance, mirroring `balances`. Atomic Lua for MT
                                         reserve/capture/release; accrual meter for MO. Reconciled with Postgres.
billing:reservation:{message_id}     -- short-TTL MT hold (amount, direction, owner, customer_id, account_id),
                                         cleared on capture/release; expiry sweep reconciles orphans. No MO reservation.
retry:delayed:{connector_id}         -- sorted-set delay queue (score = due ts) for connector retry/backoff
breaker:binds:{connector_id}         -- HASH of per (pod_id, bind_index) sub-bind states, each written only by the
                                         owning pod; the connector-level aggregate is derived by majority (§6.8/§6.15)
breaker:state:{connector_id}         -- derived connector aggregate (closed|open|half_open), read by router-svc only
                                         when (re)building its routing snapshot, never per-message
connectorload:{connector_id}         -- periodically-published in-flight gauge per connector, for least_loaded (§6.1)
config:changed                       -- pub/sub channel: Admin API announces a control-plane mutation; config-sync coalesces these (§11, M7)
breaker:events                       -- pub/sub channel for near-immediate routing-snapshot invalidation (config-sync M7, breaker transition M8)
```

### 3.3 Plan de données (broker de messages — Kafka)

```
mt.inbound        -- raw submissions (SMPP/REST), pre-routing. Partitioned by customer/account hash.
mt.routed         -- post-routing, ready for dispatch. Partitioned by (connector_id, shard_index) where
                     shard_index = hash(message_key) % bind_pool_size of the target connector (§6.8). message_key is
                     the LOGICAL message id (all UDH segments share it -> same bind, in order — §6.1/C-segment note).
                     Each message carries a fallback_chain header for unilateral reroute by connector-pool-svc (§6.15).
mo.inbound        -- raw deliver_sm from SMSC connectors, pre-routing to accounts
dlr.events        -- delivery receipt events, correlated to original message ID
mt.dead-letter / mo.dead-letter   -- failed/expired after retry exhaustion (incl. exhausted fallback_chain, §6.15)
mt.reroute-park   -- durable parking for the overflow of a large fallback reroute burst; drained rate-limited (§6.15)
```

### 3.4 Magasin CDR / analytique (columnar — ClickHouse ou équivalent)

```
cdr
  message_id (uuidv7, pk)               -- application-generated at ingress, before Kafka or any DB
  trace_id (uuidv7)                     -- correlates to the full OpenTelemetry trace (§6.11)
  account_id                            -- the SMPP account that sent/received
  customer_id                           -- denormalized owner (an account never changes customer — safe to denormalize)
  direction (mt|mo)
  source_addr, dest_addr                -- address actually used on the wire (post-rewrite, if any)
  original_source_addr (nullable)        -- populated when a sender_id_rewrite_rule changed it (§6.16)
  connector_id, route_id, routing_script_id (nullable)
  submitted_at, delivered_at
  status                (enroute|delivered|failed|expired|rejected|rerouted)
  error_code, segment_count, encoding
  content_ciphertext    (nullable)       -- the body, present only when content_storage is stored_encrypted (with the
                                            customer's key) or stored_plaintext; NULL when off. Never in logs/traces.
  content_key_id        (nullable)       -- which content_keys row decrypts it; destroyed key = body unreadable
  latency_ms
  billed (bool), credits_charged (integer, nullable)
```

Partitionné par jour (`PARTITION BY toDate(submitted_at)`), avec tiering TTL (§6.14). Interrogé par le tableau de bord pour recherche, traçage et réconciliation. `message_id`/`trace_id` étant déjà des UUIDv7 générés à l'ingestion, le sink CDR porte la valeur sans en générer.

---

## 4. Conception de haut niveau (High-Level Design)

```
                         +---------------------------------------------+
                         |              Admin Dashboard                 |
                         |   (React / TanStack Start — companion spec)  |
                         +-------------------+---------------------------+
                                             | Admin REST/WS API
+------------------------------------------------v-------------------------------------------+
|                                       CONTROL PLANE                                          |
|   Config service (accounts/customers/routes/connectors/anti-spam/...)  --> PostgreSQL        |
|   Admin API (gRPC/REST) - Config change bus (pub/sub to data plane)                          |
|   Circuit breaker state broadcast (Redis pub/sub, §6.15)                                     |
+------------------------------------------------+---------------------------------------------+
                                                  | config sync (pub/sub / polling)
+-------------------------------------------------v-------------------------------------------+
|                                        DATA PLANE                                            |
|  Ingress                    Processing Pipeline (router-svc)              Egress             |
|  +-------------+          +-----------------------------------+     +--------------------+   |
|  | SMPP Server |--+       | 1. Auth -> account -> customer     |     | SMSC Connector      |  |
|  | (user binds)|  |       | 2. E.164 normalize                 |     | Pool (per SMSC,     |  |
|  +-------------+  +-->Kafka| 3. Sender-ID authorization (§6.19) |-->Kafka bind pool §6.8,    |--> SMSC(s)
|  +-------------+  | mt.   | 4. Opt-out / suppression (§6.20)    | mt. | breaker §6.15,      |  |
|  | REST API    |--+ inbound| 5. Anti-spam                       |routed| sender-ID rewrite,  |  |
|  +-------------+       | 6. Route resolution: exact | script   |     | retry/failover,     |  |
|                       |    | declarative (§6.1)             |     | auto-reconnect)     |  |
|                       | 7. Encoding/UDH split (segment_count) |     +--------------------+   |
|                       | 8. Rate-limit                         |          | deliver_sm/DLR    |
|                       | 9. MT credit reserve (§6.9)           |          v                    |
|                       +---------------------------------------+     +--------------------+    |
|  +-------------+       +---------------------------------+          | MO/DLR Router      |    |
|  | SMPP Server |<------| Delivery: deliver_sm or webhook  |<--Kafka--| resolve inbound#/  |    |
|  | (MO/DLR out)|       +---------------------------------+  mo./    | keyword -> account, |    |
|  +-------------+                                            dlr.    | STOP detect, MO     |    |
|  | Webhook Sender|                                                  | meter (§6.9)        |    |
|  +-------------+                                                    +--------------------+    |
|  Session Manager (Redis) - all SMPP binds, windows, enquire_link, max_sessions, reconnect     |
|  Rate Limiter (Redis token-bucket) - per account/connector/route                              |
|  Anti-spam Engine - velocity (MT + inbound MO), content, duplicate, reputation                |
|  Circuit Breaker (per connector) - local decision + aggregated state to Redis (§6.15)         |
|  Sender ID Rewrite Engine (§6.16) - pre-dispatch                                               |
|  Credit Engine (opt-in) - MT reserve/capture/release, MO meter; skipped entirely when disabled |
+-------------------------------------------------+-------------------------------------------+
                                                  |
+-------------------------------------------------v-------------------------------------------+
|                                      OBSERVABILITY                                           |
|  Metrics: Prometheus (per-pod) -> remote-write -> long-term TSDB (Thanos/Mimir)              |
|  Infra alerting: Prometheus Alertmanager (independent of dashboard uptime)                   |
|  Real-time stream: WebSocket/SSE gateway (fed by Kafka metrics topic) for dashboard          |
|  Logs: structured JSON -> Loki/ELK   (message BODY is NEVER logged — §6.11)                  |
|  Tracing: OpenTelemetry (trace_id per message, span per stage; body NEVER in a span)          |
|  CDR sink: Kafka -> ClickHouse (analytics/search/trace lookup, partitioned by day)            |
+---------------------------------------------------------------------------------------------+
```

### 4.1 Services principaux (unités déployables)

1. **smpp-server-svc** — TCP longue durée, gère les binds SMPP côté utilisateur ; scalé horizontalement derrière un LB L4 avec affinité de session ; publie sur `mt.inbound`, consomme `mo.inbound`/`dlr.events` pour remettre à la session propriétaire (via le registre de session, §6.8). Gère `query_sm`/`cancel_sm` (§6.22).
2. **rest-api-svc** — service HTTP sans état pour la soumission MT, la requête de statut, la config webhook ; publie sur `mt.inbound`.
3. **router-svc** — consommateur sans état de `mt.inbound` ; applique auth, normalisation E.164, autorisation de sender ID (§6.19), opt-out (§6.20), anti-spam, résolution de route (§6.1), encodage/segmentation, limite de débit, puis réservation de crédit MT. Publie sur `mt.routed`. Émet un span par étape.
4. **connector-pool-svc** — un pool logique par SMSC ; gère les binds SMPP sortants avec `bind_pool_size` binds parallèles, consomme `mt.routed`, applique le lissage de débit et le disjoncteur (§6.15), évalue la réécriture de sender ID (§6.16) juste avant l'envoi, gère retries/bascule et l'auto-reconnexion (§6.13), capture/libère les réservations MT, publie `mo.inbound`/`dlr.events`. Publie l'état de disjoncteur agrégé dans Redis et reroute via `fallback_chain`.
5. **mo-dlr-router-svc** — consomme `mo.inbound`/`dlr.events` ; normalise E.164, détecte les mots-clés STOP (§6.20), résout le compte via le numéro entrant/mot-clé (§6.21), applique le compteur MO (§6.9), remet via SMPP ou webhook.
6. **session-manager-svc** — registre de session faisant autorité (Redis) ; expose une API gRPC pour bind/unbind/lookup, applique `max_sessions`, pilote la supervision `enquire_link`.
7. **billing-svc** (déployé si facturation activée) — possède `balances`/`billing_customers`/`billing_ledger` ; expose l'API réserve/capture/libère, réconcilie le cache avec Postgres, héberge l'adaptateur de facturation externe (§6.10). Absent du chemin de requête quand la facturation est désactivée.
7bis. **content-key-svc** — possède `content_keys` et est le **seul détenteur de la KMS** (la clé maître qui scelle les clés de données par client). Surface volontairement minimale (Postgres + gRPC) pour que le dépositaire de la clé reste auditable — voir ADR-0011.
8. **admin-api-svc** — API CRUD du plan de contrôle + métriques temps réel, traçage, export CDR asynchrone, lecture de contenu gardée et effacement RGPD (§5.3).
9. **config-sync** — pousse les changements du plan de contrôle vers le plan de données via pub/sub pour rechargement à chaud.

### 4.2 Flux de données — MT (soumission)

Un client soumet (SMPP `submit_sm` ou REST `POST`) → le service d'ingestion **authentifie** l'identifiant (bind ou clé API), le résout vers son **compte SMPP** puis son **client** (les deux ID sont attachés à l'enveloppe et propagés comme en-têtes Kafka), vérifie que le canal est activé et le quota, ouvre le span racine, **accuse réception dès validation dans `mt.inbound`** → `router-svc` : normalisation E.164 → **autorisation de sender ID** (§6.19) → **contrôle d'opt-out** (§6.20) → anti-spam → **résolution de route** (§6.1 : numéro exact → script → déclaratif) → encodage/segmentation (`segment_count`) → contrôle de limite de débit → **réservation de crédit MT** (ignorée si facturation désactivée) → publication sur `mt.routed` → `connector-pool-svc` vérifie le disjoncteur, applique la réécriture de sender ID (§6.16), envoie `submit_sm`, suit `submit_sm_resp`, **capture** en cas de succès / **libère** en cas d'échec, écrit le CDR (statut=enroute) — ou republie vers le connecteur suivant du `fallback_chain` si le disjoncteur est ouvert (§6.15) → plus tard, DLR reçu → CDR mis à jour, span clos, DLR transmis au compte d'origine.

### 4.3 Flux de données — MO (réception)

Le `deliver_sm` d'un SMSC arrive → `connector-pool-svc` → publication sur `mo.inbound` → `mo-dlr-router-svc` : normalisation E.164 → **détection de mot-clé d'opt-out** (§6.20 — un STOP écrit une suppression scopée sur le numéro entrant, le MO est quand même remis et jamais facturé) → **résolution du compte via le numéro entrant** (§6.21 — dédié → son compte ; partagé → mot-clé ; aucune correspondance → file « MO non routés ») → **remise immédiate** via SMPP `deliver_sm` au bind actif du compte, ou webhook HTTP (jamais conditionnée à un solde) → **comptage MO** = `segment_count × credits_per_segment_mo` sur le solde MO (§6.9 ; aucun effet sur le MT) → CDR écrit.

---

## 5. Conception de l'API (API Design)

### 5.1 Interfaces SMPP

- **Serveur (côté utilisateur)** : SMPP v3.4 (v5.0 optionnel) — `bind_transmitter`/`bind_receiver`/`bind_transceiver`, `submit_sm`, `submit_sm_multi` (optionnel), `deliver_sm` (MO + DLR), `enquire_link`, `unbind`, et `query_sm`/`cancel_sm` désactivables par compte (§6.22 ; une opération désactivée répond `ESME_RINVCMDID`, `query_sm` est soumis à une limite de débit dédiée). `replace_sm` et `data_sm` non supportés. Support TLV pour UDH, payload > 254 o.
- **Client (côté SMSC)** : même protocole, la passerelle agit comme ESME, avec adaptation par vendeur (profil vendeur). Chaque connecteur maintient plusieurs binds parallèles (`bind_pool_size`, §6.8).

### 5.2 API REST (publique, orientée client) — `api.gateway.example.com/v1`

```
POST   /messages                     # Submit MT SMS (single or batch)
GET    /messages/{id}                # Get message status
GET    /messages                     # Search/list own messages (paginated)
GET    /account                      # Read-only: own account info, active sender IDs, quota/rate limits
GET    /health
```

MO et DLR sont poussés via le webhook configuré par le fournisseur, ou via `deliver_sm` si le compte détient un bind. Auth : clé API (Bearer) ou HMAC. Webhooks signés HMAC-SHA256, retentés avec backoff, mis en dead-letter après N tentatives.

### 5.3 API Admin (interne, consommée par le tableau de bord) — `admin.gateway.internal/v1`

```
# Customer groups (organizational, §6.17)
GET/POST/PATCH/DELETE  /admin/customer-groups
GET                     /admin/customer-groups/{id}/customers

# Customers (billing, sender IDs, group; §6.18)
GET/POST/PATCH/DELETE  /admin/customers                 # ?groupId= filter
PATCH                   /admin/customers/{id}/group
POST                    /admin/customers/{id}/suspend    # suspends all its SMPP accounts
GET/POST/PATCH/DELETE  /admin/customers/{id}/sender-ids
GET                     /admin/customers/{id}/smpp-accounts

# SMPP accounts (channels, quotas, sessions, webhooks; §6.18)
GET/POST/PATCH/DELETE  /admin/smpp-accounts             # POST requires customerId; ?customerId=/?groupId= filters
POST                    /admin/smpp-accounts/{id}/suspend
PATCH                   /admin/smpp-accounts/{id}/channels        # { smppEnabled, restEnabled } — at least one true
PATCH                   /admin/smpp-accounts/{id}/session-limits  # { maxSessions, allowedBindTypes }
GET                     /admin/smpp-accounts/{id}/sessions        # live binds vs maxSessions
GET/POST/PATCH/DELETE  /admin/smpp-accounts/{id}/webhooks
PATCH                   /admin/smpp-accounts/{id}/sender-id-policy # strict | allow_unregistered_numeric | disabled
PATCH                   /admin/smpp-accounts/{id}/smpp-ops        # { querySmEnabled, cancelSmEnabled }

# Credentials — exactly 1 smpp_bind + 1 api_key per account (§6.3/§6.18)
GET     /admin/smpp-accounts/{id}/credentials               # masked, never a secret
POST    /admin/smpp-accounts/{id}/credentials               # secret returned exactly once
PATCH   /admin/smpp-accounts/{id}/credentials/{credId}      # status only
DELETE  /admin/smpp-accounts/{id}/credentials/{credId}      # revoke; force-unbinds live sessions
POST    /admin/smpp-accounts/{id}/credentials/{credId}/rotate  # manual, optional { gracePeriodSec }

# Connectors
GET/POST/PATCH/DELETE  /admin/connectors
POST                    /admin/connectors/{id}/rebind            # manual reconnect
GET                     /admin/connectors/{id}/status            # link_status + breaker_state, per bind in the pool
PATCH                   /admin/connectors/{id}/reconnect-policy
PATCH                   /admin/connectors/{id}/bind-pool

# Routes & exact routes
GET/POST/PATCH/DELETE  /admin/routes
POST                    /admin/routes/reorder
GET/POST/PATCH/DELETE  /admin/exact-routes                      # MSISDN -> connector|route (§6.1)
POST                    /admin/exact-routes/import               # bulk MNP import (async)
GET                     /admin/exact-routes/lookup?msisdn=

# Routing scripts (admin-authored, scoped account/customer/platform; §6.2)
GET/POST/PATCH/DELETE  /admin/routing-scripts
PATCH                   /admin/routing-scripts/{id}/assign
POST                    /admin/routing-scripts/{id}/validate
POST                    /admin/routing-scripts/{id}/test
POST                    /admin/routing-scripts/{id}/publish
GET                     /admin/routing-scripts/{id}/versions

# Sessions
GET     /admin/sessions
DELETE  /admin/sessions/{id}                            # force-disconnect

# Anti-spam
GET/POST/PATCH/DELETE  /admin/antispam-rules

# Opt-out / suppression (§6.20)
GET     /admin/suppressions?scope=&scopeId=&msisdn=
POST    /admin/suppressions
DELETE  /admin/suppressions/{id}                        # un-suppress — audit-logged
POST    /admin/suppressions/import
POST    /admin/suppressions/check                       # { msisdn, senderAddr, accountId } -> blocked? by which scope?
GET/POST/PATCH/DELETE  /admin/opt-out-keywords

# Inbound numbers & keywords (§6.21)
GET/POST/PATCH/DELETE  /admin/inbound-numbers
PATCH                   /admin/inbound-numbers/{id}/assign
GET/POST/PATCH/DELETE  /admin/inbound-numbers/{id}/keywords
GET                     /admin/mo/unrouted

# Sender ID rewrite rules (§6.16)
GET/POST/PATCH/DELETE  /admin/sender-rewrite-rules?scope=&scopeId=
POST                    /admin/sender-rewrite-rules/{id}/test

# Billing (customer-level; MT and MO balances separate; §6.9)
GET/PATCH               /admin/customers/{id}/billing
GET                      /admin/customers/{id}/balances
POST                     /admin/customers/{id}/billing/topup     # { credits, direction: mt|mo, accountId? }
POST                     /admin/customers/{id}/billing/transfer  # net-zero move between own balances
POST                     /admin/customers/{id}/billing/scope     # change balance_scope — 409 unless all balances zero
GET                      /admin/customers/{id}/billing/ledger    # ?direction= &?accountId=
GET/POST/PATCH/DELETE   /admin/rate-plans
GET/POST/PATCH/DELETE   /admin/billing-providers
POST                     /admin/billing-providers/{id}/test-connection

# Content storage & RGPD erasure (§6.14/§6.23)
GET/PATCH               /admin/platform/content-policy
GET/PATCH               /admin/customers/{id}/content-policy
GET     /admin/messages/{id}/content                    # decrypt + return body — gated by content:read, audited
POST    /admin/customers/{id}/content/erase             # content-only crypto-shred — content:erase
POST    /admin/customers/{id}/content/rotate-key
POST    /admin/gdpr/erase                               # { subjectType: customer|msisdn, id } — gdpr:erase, async
GET     /admin/gdpr/erase/{jobId}                       # status + erasure attestation

# Message tracing & export
GET     /admin/messages/{id}/trace                      # span timeline (never carries the body)
GET     /admin/messages/search?traceId=|accountId=|customerId=|groupId=|status=...
POST    /admin/messages/export                          # async export job (audit + row-cap + role-based MSISDN mask)
GET     /admin/messages/export/{jobId}

# Metrics & real-time
GET     /admin/metrics/summary?window=5m
GET     /admin/metrics/traffic?groupBy=connector|customer|group&window=1h
WS      /admin/stream/metrics
WS      /admin/stream/sessions
WS      /admin/stream/billing-alerts                    # MT low-balance / MT overdraft-attempt / mo_balance_floor_reached
```

---

## 6. Conception détaillée (Detailed Design)

### 6.1 Moteur de routage

#### Diagramme de flux — pipeline MT et résolution de route

```
                 SMPP submit_sm                REST POST /messages
                        +--------------+---------------+
                                       v
                        [ AUTH ] credential -> smpp_account -> customer      (§6.18)
                        [ CHANNEL ] smpp_enabled / rest_enabled ?
                                       v
                         ACK CLIENT once durable in Kafka mt.inbound         (§6.12)
============================ router-svc =======================================
                        [ 1. E.164 NORMALIZE dest/source ]                   (§6.19)
                        [ 2. SENDER-ID AUTHORIZATION ]  -- no match -> REJECT (§6.19)
                        [ 3. OPT-OUT / SUPPRESSION ]    -- hit -> REJECT     (§6.20)
                             Bloom filter (in-memory, no network for ~99%)
                             blocked if ANY scope matches:
                             platform | customer | smpp_account | inbound_number(source_addr)
                        [ 4. ANTI-SPAM ]               -- block -> REJECT    (§6.5)
                        +=====================================================+
                        |   5. ROUTE RESOLUTION  — 3 levels, first wins       |
                        |   L0. EXACT MSISDN MATCH (exact_routes) HIGHEST      |
                        |       Bloom -> Redis; short-circuits L1/L2          |
                        |       (target unavailable -> fall through to L1/L2) |
                        |   L1. ROUTING SCRIPT (account/platform) -> routeId  |
                        |   L2. DECLARATIVE prefix-trie / MCC-MNC             |
                        |       -> default route, else REJECT                |
                        +=====================================================+
                        distribution_strategy: static|round_robin|weighted
                                             |failover_priority|least_loaded|hash_based
                        (open-breaker / disabled connectors excluded, §6.15)
============ back to the common pipeline — NOTHING is skipped ================
                        [ 6. ENCODING / UDH SEGMENTATION ] -> segment_count   (§6.6)
                        [ 7. RATE LIMIT ]                                     (§6.4)
                        [ 8. MT CREDIT RESERVE ] on billing:balance:mt:{owner} (§6.9)
                        publish -> Kafka mt.routed (key = logical message id) (§3.3)
=========================== connector-pool-svc ================================
                        breaker closed? --no--> reroute via fallback_chain    (§6.15)
                        [ 9. SENDER-ID REWRITE ] provider-side, pre-dispatch  (§6.16)
                        submit_sm -> SMSC -> submit_sm_resp
                        capture credit / release on failure; write CDR        (§6.9/§3.4)
                        later: deliver_sm (DLR) -> update CDR -> push to client
```

> **Le court-circuit du niveau L0 ne saute que la résolution de route.** Les étapes 1–4 (E.164, autorisation de sender ID, opt-out, anti-spam) et 6–9 (segmentation, débit, crédit, réécriture, envoi) s'appliquent à **tout** message, y compris routé par numéro exact. Un raccourci de routage n'est jamais un contournement de conformité.

#### Résolution de route — les trois niveaux

L'**exécution** de la route (cibles multi-connecteurs, chaîne de repli) est identique quelle que soit la voie de résolution, et toujours déclarative.

0. **Correspondance de numéro exact (priorité maximale, court-circuit)** : si le MSISDN de destination figure dans `exact_routes`, sa cible est utilisée immédiatement, sans exécuter le script ni le matching déclaratif. C'est la réponse à la **portabilité des numéros** — le matching par préfixe (niveau 2) suppose à tort que le préfixe identifie l'opérateur, faux pour un numéro porté. La table est typiquement alimentée en masse depuis une base MNP. `router-svc` maintient en mémoire un **filtre de Bloom** de toutes les clés (rafraîchi via `config-sync`) : jamais de faux négatif, donc « absent » signifie certainement pas d'override et le message continue sans appel réseau (~99 % du trafic) ; seul un « peut-être » lit `exactroute:{msisdn}`. Cette clé est un **cache read-through** sur `exact_routes`, jamais une projection : sur un miss, le routeur lit la table par clé primaire et peuple la clé avec un TTL (6 h, jitter ±10 %) ; l'Admin API se contente de la **supprimer** après son commit, sans jamais y écrire de cible. Les deux jambes de lecture sont fail-closed en rejeu (§16). ~1,8 Mo de filtre par million d'entrées (le « ~1,2 Mo » porté ici jusqu'à step-250e correspondait à un taux de faux positifs de 0,01, jamais à celui du code, 0,001). Si la cible d'un numéro exact est indisponible (connecteur désactivé/disjoncteur ouvert), on retombe sur la chaîne normale plutôt que dead-letter.

1. **Script de routage** : si le compte (ou la plateforme) a un script actif, il est invoqué (§6.2). S'il retourne un ID de route valide, cette route est utilisée directement.

2. **Matching déclaratif** : règles ordonnées par `priority`, première correspondance complète gagnante, repli sur une route par défaut/catch-all (message rejeté, configurable, si aucune). Prédicats composables (ET). Le matching de contenu utilise des regex précompilées ; le matching de destination un préfixe-trie (O(longueur du préfixe)).

**Stratégies de distribution** (`routes.distribution_strategy`) :

| Stratégie | Comportement | Cas d'usage |
|---|---|---|
| `static` | Exactement un connecteur. | Route à un seul opérateur viable. |
| `round_robin` | Alterne parmi `route_targets` (≥2), un message à la fois. | Répartir uniformément une capacité équivalente. |
| `weighted` | Choisit aléatoirement proportionnellement à `weight`. | Capacité inégale entre connecteurs. |
| `failover_priority` | Essaie d'abord le `priority` le plus bas, passe au suivant si indisponible. | Primaire/secours. |
| `least_loaded` | Choisit la cible au plus faible nombre de messages en vol (charge publiée via `connectorload`). | Plafonds de débit très différents/fluctuants. |
| `hash_based` | Choisit déterministement en hachant une clé (MSISDN dest par défaut). | Cohérence d'affectation par abonné. |

Toutes n'opèrent que parmi les cibles passant le contrôle disjoncteur/désactivé.

Rechargement à chaud : `config-sync` pousse les diffs de routes/scripts vers `router-svc`, qui garde un instantané immuable échangé atomiquement. L'état volatil (disjoncteur agrégé, charge connecteur) vit dans une **surcouche mutable séparée** (mise à jour par pointeur atomique sur `breaker:events`), pour ne pas rebâtir l'instantané immuable à chaque transition.

### 6.2 Scripts de routage personnalisés (rédigés par l'admin, portée compte/plateforme)

Pour la logique que les règles déclaratives ne peuvent exprimer, le fournisseur peut rédiger un **script isolé** qui choisit une route.

- **Portée, pas attachement** : un script est scopé `platform`/`customer`/`smpp_account` (§3.1), jamais attaché à une route. Résolution `smpp_account → customer → platform`, au plus un actif par portée.
- **Environnement** : JS embarqué (`goja`, pur Go) principal, Lua (`gopher-lua`) alternatif. En processus dans `router-svc`.
- **Contrat** : `resolveRoute(message) -> routeId | null`, `message` exposant `sourceAddr`, `destAddr`, `content`, `accountId`, `customerId`, `timestamp`, plus `lookup(table, key)` (données de référence préchargées en mémoire, jamais un datastore synchrone) et `findRouteByName(name)`. `null` = repli déclaratif ; sinon un ID de route réel et actif. Aucun accès réseau/fichier.
- **Isolement & limites** : garde-fou **primaire = plafond d'instructions/bytecode** (déterministe, insensible aux pauses GC) ; timeout mur en filet (défaut 2 ms, jusqu'à ~20 ms) ; plafond mémoire. Toute violation → repli déclaratif, journalisée et remontée.
- **Isolement entre comptes** : programme compilé une fois, état réinitialisé par invocation (pool de runtimes réutilisés, pas d'allocation neuve par message).
- **Cycle de vie** : `draft → validate → test → publish` ; publication rejetée si un autre script est déjà actif dans la même portée.
- **Observabilité** : taux d'exécution, latence p50/p99, taux de timeout/erreur/ID invalide par script.
- **Enveloppe de performance** : un compte scripté a une capacité propre, inférieure au budget de référence (§2.5) ; `router-svc` isole ces comptes sur des pools/quotas séparés et le HPA tient compte de leur coût distinct.

### 6.3 Gestion des sessions SMPP & cycle de vie des identifiants

- Chaque bind est enregistré dans `session-manager-svc` (ID de session, compte/connecteur, type, fenêtre négociée).
- Supervision `enquire_link` : intervalle configurable (défaut 30 s), N réponses manquées (défaut 3) → unbind forcé.
- Débit par fenêtre glissante SMPP (submit_sm en vol borné par la fenêtre), découplé de la limite token-bucket métier.
- Au redéploiement d'un pod, unbind gracieux (drain) avec période de grâce ; les clients se reconnectent.

**Authentification & identifiants clients SMPP/API :**

- **Auth du bind** : à réception d'un `bind_*`, résolution du `system_id` vers une ligne `credentials` `smpp_bind` `active`, vérification du mot de passe contre `password_hash`, contrôle du type de bind contre `allowed_bind_types`. Idem clé API sur REST.
- **`max_sessions`** — nombre de binds simultanés autorisés par compte, appliqué **au bind contre le registre inter-pods** (pas best-effort par pod) : un bind au-delà du quota est refusé (`ESME_RBINDFAIL`). **Abaisser la limite sous le nombre de sessions vivantes ne coupe aucun bind existant** : la limite s'applique aux prochains binds ; forcer la convergence exige une déconnexion explicite (`DELETE /admin/sessions/{id}`). Un unbind/coupure/expiration `enquire_link` libère immédiatement le jeton.
- **Secret révélé une seule fois** : le mot de passe SMPP / la clé API n'est retourné qu'à la création et à la rotation ; seul le hash est stocké. Aucune action « reveal ».
- **Rotation manuelle** : `POST .../rotate` génère un nouveau secret avec un `gracePeriodSec` optionnel pendant lequel l'ancien reste valide en parallèle (un ESME détient des binds TCP longue durée ; une rotation dure couperait son trafic). Passé la fenêtre, l'ancien secret est invalidé et les binds encore authentifiés avec lui sont fermés. Il n'existe aucune rotation automatique/planifiée.
- **Révocation/suspension** : supprimer un identifiant, le passer `status != active`, suspendre le compte ou le client (`/admin/customers/{id}/suspend`, qui suspend tous ses comptes) force la déconnexion des sessions vivantes concernées.
- **Anti-brute-force** : échecs d'auth comptés par `system_id` et IP source (Redis TTL), backoff/blocage temporaire, événement de sécurité auditable.
- **Surcharge de codage par connecteur** (`data_coding_default`) : consultée par l'étape Encodage (§6.6), vit sur le connecteur.

### 6.4 Gestion du débit

- Deux niveaux : (1) fenêtre SMPP au protocole par session, (2) token-bucket métier par compte/connecteur/route dans Redis (Lua atomique).
- **Précédence** : `smsc_connectors.throughput_limit_per_sec` est le plafond technique absolu ; une ligne `rate_limits` pour ce connecteur est un gouverneur opérationnel qui ne peut jamais le dépasser (validation à l'écriture). Le débit effectif est le minimum des deux.
- **Backpressure** : à l'approche du plafond, `connector-pool-svc` ralentit la consommation Kafka ; les messages restent durablement en file.
- **Throttling adaptatif** : les signaux d'erreur `submit_sm_resp` (ex. `ESME_RTHROTTLED`) alimentent un ajusteur AIMD qui réduit puis remonte progressivement le débit effectif.
- **Politique de panne Redis (rate-limit)** : **fail-closed conservateur** — si les compteurs sont injoignables, chaque pod applique localement le plafond technique statique du connecteur, jamais un débit non borné.

### 6.5 Moteur anti-spam

- Étape de pipeline dans `router-svc` avant le routage. Le matching de contenu opère sur le corps **en clair, en mémoire**, avant toute décision de stockage — « ne pas stocker le contenu » n'empêche jamais l'anti-spam (§6.23).
- **Vélocité** : max messages/s/minute d'un compte vers une destination ou across destinations. S'applique aussi au **MO entrant par compte destinataire**, pour signaler/étrangler un afflux MO anormal (§6.9/H-abus).
- **Contenu** : liste noire mot-clé/regex, par compte/client/globale ; motifs raccourcisseur/phishing.
- **Doublons** : hash(source+dest+contenu) avec fenêtre TTL courte dans Redis.
- **Réputation** : score glissant par client (taux de blocage/plainte/échec DLR) ; franchir un seuil déclenche throttling ou revue.
- **Actions** : `block` / `flag` / `throttle`.
- **Politique de panne Redis (anti-spam)** : dégradation graduée — les vérifications à état partagé (dédup, vélocité, réputation) basculent en **fail-open avec flag** (message journalisé/marqué) ; les règles de contenu statiques continuent de s'appliquer. Configurable par règle.

### 6.6 Encodage & segmentation

- Détecte l'encodage (GSM-7/UCS-2/8-bit), en respectant `data_coding_default` du connecteur si défini ; calcule le nombre de segments ; découpe avec en-tête UDH.
- Réassemblage des MO concaténés avant remise ; le réassemblage détermine le nombre de segments pour le compteur MO (§6.9).
- S'exécute avant la limite de débit et la réservation de crédit (le coût dépend du nombre de segments).

### 6.7 Fiabilité & gestion des pannes

- Toute écriture d'ingestion vers Kafka constitue la limite de durabilité ; l'accusé n'a lieu qu'après validation dans `mt.inbound`.
- Pannes de connecteur : disjoncteur par connecteur (§6.15).
- Files dead-letter pour les messages ayant épuisé leurs retries (y compris via `fallback_chain`), remontées pour retraitement.
- L'exactement-une-fois n'est pas garanti de bout en bout (SMPP est au moins une fois) ; remise au moins une fois au SMSC, clés d'idempotence disponibles côté client. La facturation est idempotente par `message_id` (§6.9).
- **Un abonné peut donc recevoir deux fois le même SMS**, et c'est assumé plutôt que subi (ADR-0012, ADR-0014). Les causes résiduelles sont **deux**, une par étage qui publie avant de commiter : au **pool de connecteurs**, un crash entre le `submit_sm` déjà parti sur le fil et l'accusé du produce Kafka qui enregistre son issue ; au **routeur**, une interruption entre la publication d'un `mt.routed` et le commit de l'offset de `mt.inbound` (ADR-0014). Aucune conception ne les supprime — un `submit_sm` n'est transactionnel avec aucun magasin — mais elles sont **bornées**, et par la même variable aux deux étages : au plus **un poll par partition et par incident**, soit **~250 `submit_sm` par partition** côté pool (`KAFKA_FETCH_MAX_PARTITION_BYTES`, 56 KiB **compressés** — le nombre dépend du taux de compression, mesuré 2,81× sur des records `mt.routed` typiques), moins d'une demi-seconde de trafic par partition à la cible de débit. Côté routeur l'unité est le **message**, donc 1..N segments, et le compte en messages reste à mesurer : un record `mt.inbound` porte le corps entier là où un `mt.routed` porte un segment (ADR-0014). Les deux mitigations ci-dessus ne couvrent pas ce cas : les clés d'idempotence client valent à la frontière REST d'ingestion, l'idempotence de facturation protège le solde et non le combiné.

### 6.8 Stratégie de scalabilité

- Tous les services du plan de données sont sans état et s'étendent via HPA (CPU/lag Kafka).
- `smpp-server-svc` fait exception (état TCP) — scalé aussi, la remise MO/DLR utilisant le registre `session-manager-svc`. **Remise au bon pod** : le registre maintient `account → {pod_id, bind_id}[]` ; `mo-dlr-router-svc` remet **directement au pod détenteur via gRPC** (endpoint de remise interne), round-robin sur les binds vivants ; à défaut de bind, repli webhook.
- **Pool de binds par connecteur** : `bind_pool_size > 1` (§3.1) partitionne `mt.routed` par `(connector_id, shard_index)`, `shard_index = hash(message_key) % bind_pool_size`. Chaque partition est consommée par une instance dédiée tenant un bind indépendant. `message_key` est l'**ID de message logique** (tous les segments UDH d'un message concaténé le partagent → même shard/bind, dans l'ordre — requis par les SMSC réassemblant sur un seul bind).
- **Agrégation du disjoncteur multi-pod** : les binds étant répartis sur plusieurs pods, chacun écrit uniquement ses champs dans le hash `breaker:binds:{connector_id}` ; l'état connecteur agrégé (`breaker:state`) est **dérivé** par règle de majorité (recalcul-et-CAS sur transition, ou agrégateur élu). La charge (`connectorload`) suit le même schéma.
- Kafka : partitions dimensionnées pour le parallélisme (partitions par connecteur × `bind_pool_size` pour `mt.routed` ; hash de compte pour `mt.inbound`/`mo.inbound`).
- Multi-région : plan de données par région pour la latence, synchro de config cross-région ; primaire Postgres dans une région avec réplicas en lecture. *(La reprise après sinistre n'est pas traitée ici, §1.2bis.)*

### 6.9 Solde de crédit SMS (opt-in)

**Le solde est un compteur entier de crédits SMS**, jamais monétaire. Deux axes orthogonaux : la **direction** et le **propriétaire**.

**Axe 1 — MT et MO ont des soldes séparés.** Un MO est non sollicité (le SMSC l'a déjà remis avant toute décision de crédit). Un solde commun laisserait du trafic entrant hors du contrôle du client vider les crédits de ses envois, et permettrait à un tiers d'inonder un long-code pour couper les envois MT (déni de service économique). La séparation supprime ce couplage et ce vecteur.

- **Le solde MT est un vrai solde** : réserve → capture/libère (schéma bloquant). En prépayé sans découvert, atteindre zéro bloque l'envoi.
- **Le solde MO est un compteur postpayé qui ne bloque rien** : le MO est toujours remis et toujours compté ; le compteur peut descendre jusqu'à `mo_billing_floor`, après quoi l'accumulation cesse (MO toujours remis, plus débité) et une alerte `mo_balance_floor_reached` est émise. Un dépassement MO n'a **aucun** effet sur le MT. Le recours contre une facture MO impayée est commercial (suspendre le client/compte), pas un blocage par message.

**Axe 2 — le propriétaire du solde (`customers.balance_scope`) :**

- **`customer` (défaut)** : un pool partagé par direction, sur lequel puisent tous les comptes du client. Un compte actif peut consommer les crédits de ses frères ; l'attribution reste traçable (le grand livre porte `owner_*` et `customer_id`/`account_id`).
- **`smpp_account`** : chaque compte a ses propres soldes MT et MO ; isole les budgets et supprime le point de sérialisation Redis du pool partagé.
- **Verrou** : `balance_scope` est fixé à la création et ne peut être changé que si **tous les soldes du client sont à zéro** (sans rien à répartir, aucune allocation arbitraire possible). Le mode hybride est un non-objectif.

**Formule** : `credits = segment_count × credits_per_segment(destination, sender_type)`, consultée dans `rate_plans`, après segmentation et après la limite de débit.

**Prépayé MT — réserve → capture/libère :**
1. `router-svc` appelle la réservation atomique (Lua) sur `billing:balance:mt:{owner}` ; plancher `0` (ou `-overdraft_limit`).
2. Échec de réservation → rejet immédiat (REST `402`, SMPP `submit_sm_resp` code d'extension), aucune entrée de grand livre.
3. Sur `submit_sm_resp` réussi, `connector-pool-svc` **capture** ; sur échec/expiration, **libère**.
4. **Idempotence** : réserve/capture/libère sont idempotentes par `message_id` (clé de réservation unique + contrainte `UNIQUE(message_id, entry_type)`), car réserve (router) et capture (connector) encadrent un hop Kafka au moins une fois. Le sweep d'orphelins ne libère qu'après vérification d'absence de capture et de corrélation DLR.
- **Autorité du solde = grand livre Postgres durable** (chaque `reserve` est journalisée, solde reconstructible). Le cache Redis est une projection ; à sa perte/failover, le Credit Engine réhydrate depuis Postgres avant d'accepter une réservation et bloque (fail-closed) pendant la fenêtre pour les comptes en garantie stricte.

**Postpayé MT** : aucune vérification bloquante à la soumission ; usage enregistré après envoi. `credit_limit` souple pour alertes, bloquant seulement si `credit_limit_is_hard`.

**Moment de facturation & remboursement** : `rate_plans.charge_on`. En `submission` (défaut), capture à l'acceptation SMSC, non annulée si le DLR est `failed`/`expired`. En `delivery`, un DLR `failed`/`expired` déclenche une entrée `refund` annulant la capture.

**MO** : `mo-dlr-router-svc` remet toujours puis compte `segment_count × credits_per_segment_mo` (aucune réservation — un MO ne peut être pré-autorisé). Les MO opt-out/STOP et les messages réglementaires ne sont jamais comptés.

**Opt-in** : feature flag global + `customers.billing_enabled`. Désactivé, `router-svc`/`connector-pool-svc` sautent l'étape avec un contrôle booléen en cache — aucun appel réseau, aucune dépendance.

### 6.10 Interopérabilité de facturation externe

`billing-svc` est construit autour d'une interface fournisseur enfichable :

- **`balance_check`** : le système externe fait autorité ; la passerelle écrit quand même son grand livre pour corrélation/réconciliation.
- **`consume_delegate_async`** (défaut si délégation) : réserve/capture contre le cache local ; confirmation externe par réconciliation périodique. Latence hot-path inchangée, mais cohérence éventuelle (pas d'autorisation synchrone).
- **`consume_delegate_sync`** (opt-in) : appel HTTP synchrone sur le chemin critique pour une autorisation temps réel, avec budget de latence et politique fail-open/fail-closed dédiés.
- Job de réconciliation périodique comparant les totaux locaux à l'usage externe, écarts remontés.

### 6.11 Traçage des SMS

Chaque message reçoit un ID de trace (OpenTelemetry/W3C) à l'ingestion, propagé comme en-tête Kafka et rattaché au CDR.

- **Spans par étape** : ingestion/auth, autorisation sender ID, opt-out, anti-spam, routage (règle/script), limite de débit, réservation/capture, encodage, envoi SMSC + `submit_sm_resp`, réception DLR, remise finale.
- **Échantillonnage** : 100 % pour tout message en erreur/rejet/timeout ; configurable pour le trafic réussi.
- **Invariant absolu** : un span **ne contient jamais le corps du message**, sous aucune politique de stockage ni aucun environnement ; au plus une longueur et un `content_hash` tronqué. Idem pour tous les logs. Le corps n'est visible que depuis le tableau de bord (§6.23).

### 6.12 Traitement asynchrone & couche file/cache

La réception d'un SMS n'est jamais couplée de manière synchrone à son envoi ; un soumetteur est acquitté dès validation durable dans `mt.inbound`.

- **Kafka** : socle de durabilité du pipeline.
- **Redis/Dragonfly** : couche opérationnelle à faible latence (sessions, débit, dédup, cache/réservations de solde, retry/délai courts, état agrégé de disjoncteur, filtres). Jamais le magasin durable des messages.

Une panne Redis dégrade des fonctionnalités selon leur politique de panne (§6.4/§6.5/§6.9) sans perdre un message en vol (durable dans Kafka), et dégrade la fraîcheur de l'info de disjoncteur pour `router-svc` (correction au niveau `connector-pool-svc` via `fallback_chain`).

### 6.13 Reconnexion automatique SMPP (opt-in)

S'applique aux binds côté SMSC, par `smsc_connectors.auto_reconnect_enabled` — désactivée par défaut.

- **Désactivée** : un bind rompu est marqué `link_status=down` ; la reprise repose sur une action manuelle (rebind, §5.3) ou une alerte de supervision externe.
- **Activée** : un superviseur retente avec backoff exponentiel + jitter, plafonné à `reconnect_max_attempts`.
- **Le disjoncteur ne réalise aucune reconnexion** : ses sondes half-open nécessitent une connexion déjà établie. Conséquence pratique : sur perte de bind (cas `dead-carrier`), le half-open ne peut pas sonder et le disjoncteur ne se referme pas seul ; la reprise automatique n'existe donc que si l'auto-reconnexion est activée. **Recommandation normative** : tout connecteur s'appuyant sur le disjoncteur devrait activer l'auto-reconnexion ; l'UI/API émet un avertissement sinon.
- **États distincts** : le connecteur expose `link_status` (up|reconnecting|down) et `breaker_state` (closed|open|half_open) séparément — « disjoncteur ouvert, lien up » et « bind mort, lien down » appellent des actions opposées.
- **Garde-fou** : les rejets d'auth durs (`ESME_RINVPASWD`) arrêtent la boucle d'auto-retry.

### 6.14 Partitionnement, archivage, rétention & effacement

**6.14.1 Partitionnement.** CDR (ClickHouse) et grand livre (Postgres) partitionnés **par jour** ; journal d'audit **mensuel** ; plan de contrôle **non partitionné** ; les partitions Kafka servent le parallélisme, distinctes du partitionnement de table.

**6.14.2 Archivage & tiering.** Le partitionnement quotidien rend l'archivage réalisable (déplacer/détacher des partitions).

| Palier | Fenêtre (défaut) | Support |
|---|---|---|
| Chaud | 0–7 j | SSD local, Postgres primaire |
| Tiède | 8–90 j | Stockage objet + Parquet, partitions détachées |
| Froid/archive | > 90 j (jusqu'à limite légale, ex. 13 mois) | Stockage objet archive, immuable |

Format d'archive columnar (Parquet) auto-descriptif, relisible sans la plateforme.

**6.14.3 Rétention & purge (différenciées par classe).**

| Classe | Rétention par défaut | Note |
|---|---|---|
| Corps du message | `content_retention_days` (ex. 7 j) | Découplé, plus court que les métadonnées ; purge ou crypto-shred (§6.23) |
| Métadonnées CDR | 90 j (configurable) | MSISDN = donnée personnelle |
| Grand livre | 13 mois+ | Obligation comptable ; froid au-delà de la fenêtre active |
| Journal d'audit | 1–7 ans selon conformité | Immuable |
| Traces / logs | Court (jours) | Jamais de corps (§6.11) |
| Suppressions (opt-out) | Sans expiration | Expirer serait une violation (§6.20) |

Purge par échéance = **drop de partition**, pas `DELETE WHERE`.

**6.14.4 Effacement RGPD à la demande** (permission `gdpr:erase`, auditée, irréversible) :

- **Effacer un client** : crypto-shred de sa clé de contenu (§6.23) + purge de ses lignes CDR. Le grand livre peut devoir être conservé (obligation fiscale prime).
- **Effacer une personne (MSISDN — le cas DSAR)** : on ne peut pas crypto-shredder (clé partagée entre destinataires) → suppression ciblée ligne à ligne du contenu **et** des métadonnées, `WHERE source_addr = :m OR dest_addr = :m`, across clients. Job asynchrone (mutation ClickHouse) + attestation d'effacement.
- **Exception** : les suppressions/opt-out d'un MSISDN sont conservées (les effacer le ré-exposerait).

### 6.15 Disjoncteur (Passerelle → SMSC)

Traité comme requis. Au débit ciblé, un connecteur qui se dégrade sans disjoncteur est un vrai risque.

- **Machine à états par connecteur** (`closed → open → half-open → closed`) : Closed compte les issues `submit_sm_resp` sur fenêtre glissante ; Open déclenché au-delà d'un seuil (défaut 50 %, avec minimum de requêtes), arrête l'envoi et démarre un cool-down (défaut 30 s) ; Half-open laisse passer des sondes ; reprise si les sondes réussissent, sinon réouverture avec backoff.
- **Propagation d'état sans dépendance synchrone par message** :
  1. Décisions de routage futures : chaque `connector-pool-svc` publie l'état agrégé dans `breaker:state:{connector_id}` (dérivé du hash `breaker:binds`, §6.8) à chaque transition, avec notification `breaker:events`. `router-svc` consulte cet état uniquement en construisant son instantané, jamais par message.
  2. Messages déjà routés : chaque message porte un `fallback_chain` en en-tête (résolu au routage). Si `connector-pool-svc` reçoit un message pour un connecteur ouvert, il republie sur `mt.routed` vers le connecteur suivant, borné à quelques sauts avant dead-letter.
- **Reroutage de masse borné** : à l'ouverture d'un connecteur chargé, le backlog est redirigé par un **draineur à débit limité** (aligné sur `connectorload` du repli) ; l'excédent est parqué dans `mt.reroute-park` (rejoué progressivement) pour éviter une tempête de republication Kafka.
- **Stockage d'état** : décision rapide en mémoire locale par pod/bind ; seul l'agrégat est publié dans Redis (par transition, pas par message).
- **Relation** : throttling adaptatif (§6.4) = première ligne ; disjoncteur = arrêt dur ; auto-reconnexion (§6.13) = couche connexion, sans lien de causalité.

### 6.16 Règles de réécriture de sender ID (admin uniquement)

Réécrit l'adresse source à la volée (cas principal : un SMSC rejette certains formats) sans que le client le sache.

- **Où** : dans `connector-pool-svc`, immédiatement avant l'envoi, **après** la résolution du connecteur et **après** que l'anti-spam/routage ont évalué le sender ID *original*. L'original est préservé sur le CDR (`original_source_addr`).
- **À distinguer de l'autorisation (§6.19)** : l'autorisation contrôle l'adresse *revendiquée par le client* à l'ingestion ; la réécriture est décidée par *le fournisseur* avant l'envoi — elle n'a pas à être ré-autorisée.
- **Portée & précédence** : `connector → account → customer → platform`, première correspondance gagnante.
- **Types** : `static`, `fallback_pool` (round-robin), `truncate`, `sanitize`.
- **Cas d'usage** : conformité réglementaire (sender ID pré-enregistré, DLT/10DLC), repli pour expéditeur non approuvé, marque blanche revendeur, normalisation reply-to MO.

### 6.17 Groupes de clients (admin uniquement — segmentation)

Un groupe regroupe des **clients** pour segmenter la base (filtrer, ventiler, cadrer une recherche CDR). Structure plate, un client appartient à zéro ou un groupe.

- **Non-objectifs** : un groupe ne porte ni solde, ni quota, ni portée de configuration (routes/scripts/anti-spam/réécriture), et n'apparaît jamais sur le chemin critique.
- **CDR** : porte `customer_id` (lien stable, dénormalisable) mais pas `group_id` (l'appartenance change) ; un filtre groupe est résolu en `customer_id IN (...)`, reflétant l'appartenance courante.
- **Pas un label Prometheus** : dérivable du compte ; la ventilation par groupe somme les séries par compte.
- **Suppression non destructive** : détache les clients (`group_id → NULL`), ne supprime jamais un client ni ses comptes.

### 6.18 Modèle client / compte SMPP

Un **client** détient 1..N **comptes SMPP**.

- **Cardinalité des identifiants = contrainte** : chaque compte a exactement 1 identifiant de bind SMPP + 1 clé API (`UNIQUE(account_id, type)`), chacun isolé.
- **Canaux** : `smpp_enabled`/`rest_enabled` (`CHECK` au moins un) ; un compte « REST seulement » garde son identité SMPP réactivable.
- **Répartition des niveaux** (tableau §3.1) : relation commerciale (solde, tarif, sender IDs, réputation, contenu) → client ; intégration technique (identifiants, canaux, débit, sessions, webhooks) → compte SMPP.
- **Statut hiérarchique** : effectif = `min(customers.status, smpp_accounts.status)`.
- **Chemin critique inchangé** : l'auth résout identifiant → compte → client à l'ingestion et propage les deux ID dans l'enveloppe Kafka ; aucune jointure par message.

### 6.19 Normalisation E.164 & autorisation de sender ID

**Normalisation E.164** : à l'ingestion, destination (et source pour le MO) normalisées avant toute autre étape — sinon la déduplication, l'opt-out et la correspondance de numéro exact seraient contournables par un simple écart de format.

**Autorisation de sender ID** : dans `router-svc`, avant le routage et la facturation. Le `source_addr` doit correspondre à un `sender_ids` `active` du **client** (§6.18). Politique par compte (`sender_id_policy`) :

- `strict` (défaut) — correspondance obligatoire, sinon rejet (REST `403`, SMPP `ESME_RINVSRCADR`).
- `allow_unregistered_numeric` — alphanumériques enregistrés obligatoires, expéditeur numérique libre toléré.
- `disabled` — aucun contrôle. Déconseillé, audité, signalé par un avertissement UI (rouvre l'usurpation).

### 6.20 Désabonnement (opt-out / STOP) — par canal

**Le désabonnement vise le CANAL** (le numéro entrant auquel le destinataire a répondu STOP) — pas la plateforme ni le client en bloc. Portée par défaut d'un STOP : `inbound_number` ; portées plus larges disponibles (`smpp_account`, `customer`, `platform`).

**Chemin entrant** : un MO arrive sur un numéro entrant (§6.21) ; son corps est comparé aux `opt_out_keywords` du pays. `suppress` (STOP…) écrit une ligne `suppressions` scopée sur le numéro entrant reçu ; `unsuppress` (START…) la retire ; `help` déclenche une auto-réponse. Le MO d'opt-out est toujours remis au client et jamais facturé ; les auto-réponses sont des MT jamais facturés.

**Chemin sortant** : étape **bloquante** dans `router-svc`, avant anti-spam/routage/facturation. Le **canal** du MT est déduit de son `source_addr` (si ce sender ID est aussi un `inbound_numbers.address`). Blocage si le destinataire figure dans **l'une quelconque** des portées applicables (`platform` OU `customer` OU `smpp_account` OU `inbound_number(source_addr)`). Rejet explicite (REST `403 recipient_opted_out`, SMPP `ESME_RSUBMITFAIL`), CDR `status=rejected`, `error_code=opted_out`.

**Recherche sans coût** : filtre de Bloom par portée en mémoire de `router-svc` (jamais de faux négatif — la propriété qui compte ici : un faux négatif enverrait à un désabonné) ; seul un « peut-être » lit `suppress:{scope}:{scope_id}:{msisdn}`.

**Cas limite** : un expéditeur alphanumérique n'a pas de chemin de retour (on ne peut pas lui répondre STOP) — seules les portées compte/client/plateforme s'y appliquent ; le tableau de bord avertit les comptes n'envoyant qu'en alphanumérique sans numéro entrant.

### 6.21 Numéros entrants & mots-clés — routage du MO

- **`inbound_numbers`** : shortcodes/long codes du fournisseur, rattachés à un connecteur et assignés à un compte SMPP (**dédié**) ou résolus par mot-clé (**partagé**).
- **`inbound_keywords`** : sur un numéro partagé, `keyword → account_id`, par priorité, sur le premier token du corps.
- **Résolution** : `dest_addr → inbound_numbers` → (dédié ? le compte) sinon (partagé ? mot-clé → compte). Aucune correspondance → compte de repli, sinon **file « MO non routés »** exposée au tableau de bord — jamais un abandon silencieux.
- La détection de mot-clé STOP (§6.20) s'exécute sur ce chemin, avant la résolution du compte.

### 6.22 Opérations SMPP optionnelles : `query_sm` & `cancel_sm`

- **`query_sm`** — état d'un message par son ID, résolu contre le magasin de statut/CDR.
- **`cancel_sm`** — annule un message pas encore envoyé au SMSC ; s'il a déjà été soumis, `ESME_RCANCELFAIL` ; s'il est inconnu, `ESME_RINVMSGID`. L'annulation est **réservée au canal SMPP** — pas de surface REST (voir [ADR-0009](adr/0009-annulation-reservee-smpp.md)).
- **Désactivables par compte** (`query_sm_enabled`/`cancel_sm_enabled`, défaut `true`) : une opération désactivée répond `ESME_RINVCMDID`.
- **`query_sm` est un vecteur de polling** : un client qui l'interroge en boucle reporte sa charge sur le magasin CDR ; il est soumis à une **limite de débit dédiée**. Les DLR restent le mécanisme poussé recommandé.

### 6.23 Stockage & chiffrement du contenu des messages

Le corps est la donnée la plus sensible (PII, OTP, bancaire). Son stockage est une **décision configurable**, défaut plateforme + surcharge par client (`customers.content_storage`, relève d'un accord de traitement de données) :

- **`off`** — le corps n'est jamais persisté (métadonnées quand même enregistrées).
- **`stored_plaintext`** — corps persisté en clair dans le CDR (réservé aux cas contractuellement autorisés ; déconseillé, audité).
- **`stored_encrypted`** (recommandé si stockage) — corps persisté chiffré avec la clé du client, lisible uniquement depuis le tableau de bord sous `content:read`.

**Cette politique ne gouverne qu'une surface : le stockage du corps dans le CDR pour consultation depuis le tableau de bord.** Logs et traces ne portent **jamais** le corps, sous aucune politique (§6.11) — c'est un invariant, pas un réglage, et il est testable.

**Chiffrement — enveloppe + clé par client** (`content_keys`) : chaque client a une clé de données enveloppée par une clé maître KMS. Une clé par client apporte l'isolation cryptographique et le **crypto-shred** — détruire la clé (`status=destroyed`) rend tout le contenu du client illisible d'un geste, sans réécrire le CDR (mécanisme d'effacement RGPD, §6.14.4). Rotation : nouvelle clé `active`, l'ancienne `retired` jusqu'à expiration des lignes qu'elle déchiffre.

**Rétention du contenu découplée** : `content_retention_days` (par client) indépendante et typiquement plus courte que la rétention CDR — réduit l'exposition PII sans sacrifier l'analytique.

**Honnêteté sur le périmètre du chiffrement** : le chiffrement au repos protège contre un vol de base/backup. `content:read` **est** la frontière d'accès (l'application déchiffre à la demande pour qui a la permission) — le chiffrement est de la défense en profondeur. Tout accès en lecture de contenu est audité.

**Interaction pipeline** : anti-spam, segmentation et détection opt-out lisent le clair **en mémoire, avant** stockage ; le chiffrement (ou le rejet du stockage) intervient uniquement à l'écriture du CDR. « Traiter le contenu » et « ne pas le stocker » ne sont pas en conflit.

---

## 7. Évaluation (Evaluation)

| Décision | Compromis |
|---|---|
| Client et compte SMPP comme entités distinctes (1..N) | Exprime « un client, plusieurs comptes SMPP » et borne la cardinalité des clés API ; coûte une entité et une jointure au provisioning. Chemin critique inchangé (auth résout compte→client à l'ingestion). |
| Exactement 1 clé API + 1 bind par compte, en contrainte de schéma | Rend la règle inviolable plutôt qu'une convention de prose. Coût nul. |
| Solde partagé (`balance_scope=customer`) vs par compte | Le partagé colle au commercial (crédits achetés une fois) au prix qu'un compte consomme les crédits de ses frères ; le par-compte isole les budgets et supprime le point de sérialisation Redis. Bascule verrouillée soldes-à-zéro pour éviter toute allocation arbitraire. |
| Soldes MT et MO séparés ; MO = compteur postpayé | Supprime le couplage où l'entrant non contrôlé bloquait l'envoi, et un vecteur d'abus. Contrepartie assumée : un solde MO ne peut rien bloquer (recours commercial). Bénéfice : élimine tout un sous-système (état restreint, blocage MT sur MO). |
| Broker durable (Kafka) comme couche d'ingestion vs écriture DB directe | Découple pics et traitement, offre rejeu/dead-letter au débit cible ; complexité opérationnelle accrue. Kafka retenu face à NATS JetStream pour la maturité ClickHouse, la sémantique consumer-group et l'antécédent à cette échelle. |
| Livraison au moins une fois + idempotence de facturation par `message_id` | Système plus simple/haut débit ; repousse une part d'idempotence côté client. L'idempotence par message_id ferme la double-facturation sous retry. |
| Autorité du solde = grand livre Postgres, cache Redis réhydraté | Garantie de zéro dépassement MT réellement stricte (ancrée sur le durable), au prix d'une brève fenêtre fail-closed à la perte de cache. |
| Routage à 3 niveaux (numéro exact → script → déclaratif) | Le numéro exact résout la portabilité et court-circuite la résolution quand la cible est connue ; le préfixe reste une approximation en marché porté. Filtre de Bloom en mémoire → coût quasi nul pour les 99 % sans override. Le court-circuit ne saute jamais la conformité. |
| Moteur de script embarqué (goja/Lua) vs FaaS externe | Évite un saut réseau et un domaine de panne sur le chemin critique, au prix d'un bac à sable strict et d'une enveloppe de capacité propre aux comptes scriptés. Garde primaire = compteur d'instructions (déterministe) plutôt que timeout mur. |
| Disjoncteur par connecteur, état hybride (cache Redis pour le routage futur + `fallback_chain` pour les messages en main) | Donne à `router-svc` la visibilité sur les pannes sans dépendance synchrone par message ; agrégation multi-pod par hash de sous-binds. Reroutage de masse borné pour éviter une tempête de republication. |
| Pool de binds par connecteur + partitionnement Kafka par shard | Lève le plafond de débit d'un bind unique ; `message_key` = ID logique garde les segments concaténés sur un bind. Coût : agrégation d'état sur plusieurs sous-binds. |
| Autorisation de sender ID (distincte de la réécriture) | Ferme l'usurpation d'expéditeur ; l'autorisation porte sur l'adresse revendiquée (ingestion), la réécriture sur la décision du fournisseur (pré-envoi). |
| Opt-out scopé au canal, avec union des portées à l'application | Un STOP sur un shortcode ne coupe que ce canal ; l'application bloque sur l'union (canal/compte/client/plateforme) pour qu'un opt-out large soit effectif. Filtre de Bloom (jamais de faux négatif) sur le chemin critique. |
| Numéros entrants/mots-clés comme source de vérité du MO | Rend le MO routable et supporte l'opt-out ; un MO non résolu part en file « non routés », pas à la poubelle. |
| `query_sm`/`cancel_sm` désactivables par compte | Corrige l'asymétrie SMPP/REST ; désactivables et débit-limités car `query_sm` reporte la charge de polling sur le CDR. Les DLR restent le mécanisme poussé. |
| Stockage de contenu configurable, chiffré par clé client, jamais loggué | Décision structurante rendue explicite ; clé par client → isolation + crypto-shred (effacement RGPD). Le corps ne fuit jamais dans logs/traces (invariant testable). `content:read` est la frontière d'accès, auditée. |
| Effacement RGPD asymétrique (client vs MSISDN) | Le crypto-shred efface un client efficacement ; effacer une personne impose une suppression ciblée ligne à ligne (clé partagée). Distinguer les deux évite de promettre un effacement par MSISDN instantané. |
| Groupes de clients organisationnels uniquement | Segmentation (filtrer, ventiler) sans niveau d'héritage ; ajout gratuit en performance (hors chemin critique). |
| Gestion des sous-ressources exclusivement admin (pas de libre-service) | Surface d'attaque réduite, modèle B2B ; coût : charge opérationnelle sur l'équipe admin. |
| Stockage polyglotte (Postgres + Redis + Kafka + ClickHouse) | Le bon outil par pattern d'accès, au prix d'une surface opérationnelle accrue. |
| Partitionnement quotidien + tiering + rétention par classe | Rend archivage et purge réalisables (par partition) ; rétentions différenciées (corps < CDR < grand livre < audit). |

**Ce qu'on revisiterait à mesure que le système grandit :** partitionnement Kafka plus fin au-delà d'un certain nombre de binds ; tuning batch-write ClickHouse à >10k/s ; modèle ML anti-spam si les patterns de fraude dépassent les règles statiques ; reroutage en masse si le volume de `fallback_chain` devient significatif ; classification de contenu (marketing/transactionnel/OTP) pour débloquer les quiet hours.

---

## 8. Composant distinctif (Distinctive Component)

**Pipeline d'ingestion double-protocole unifié, avec contrôle de débit adaptatif, routage résilient et conformité intégrée.**

Le défi central est de traiter SMPP et REST de façon identique après l'ingestion : `submit_sm` et `POST` convergent dans le même pipeline `mt.inbound`, garantissant que autorisation d'expéditeur, opt-out, anti-spam, routage et débit sont appliqués une seule fois, quel que soit le protocole.

Le **contrôleur de débit adaptatif** rend la passerelle sûre face au comportement réel des opérateurs : lecture des signaux d'erreur `submit_sm_resp` par connecteur, ajustement AIMD du débit effectif, repli du routage vers des connecteurs alternatifs — combinant throttling protocole (fenêtre SMPP), métier (token bucket) et piloté par rétroaction.

Le **routage résilient à cohérence d'état bout-en-bout** combine un état de disjoncteur agrégé (par hash de sous-binds, multi-pod) pour les décisions futures et une chaîne de repli portée sur chaque message pour les décisions déjà prises, sans dépendance synchrone sur le chemin critique — et un niveau de **routage par numéro exact** qui résout la portabilité et court-circuite la résolution sans jamais court-circuiter la conformité.

Le **routage scriptable par l'admin** permet d'exprimer une logique qui exigerait autrement un changement de code, dans un bac à sable strict à budget d'instructions.

La **facturation entièrement optionnelle** donne des soldes MT/MO séparés (le MO ne peut jamais bloquer le MT), un propriétaire de solde configurable, une garantie de zéro dépassement ancrée sur le durable, et disparaît entièrement quand elle est désactivée.

La **conformité intégrée au chemin critique** — autorisation de sender ID, opt-out scopé au canal appliqué avant tout coût, contenu jamais loggué et chiffré par client avec crypto-shred, effacement RGPD à la demande — fait de la conformité une propriété du système, pas une couche rapportée.
