-- 0001_init.up.sql — SMS gateway control plane (PostgreSQL 18)
--
-- Derived from db/schema_passerelle_sms.sql (the annotated reference schema).
-- golang-migrate applies this file as a single statement via the postgres/pgx driver; PostgreSQL's
-- simple-query protocol runs the whole file in ONE implicit transaction, so it is atomic WITHOUT an
-- explicit BEGIN/COMMIT (do not add them — they interfere with golang-migrate's version tracking).
-- Requires PostgreSQL 18 for the native uuidv7() generator (RFC 9562).
-- The ClickHouse CDR store uses a SEPARATE migration set (different driver); see the appendix in
-- db/schema_passerelle_sms.sql. Redis/Kafka are not schema-migrated.

CREATE SCHEMA IF NOT EXISTS control_plane;
CREATE SCHEMA IF NOT EXISTS dashboard;   -- operators live here; defined in full by the dashboard spec

SET search_path = control_plane, public;

-- -----------------------------------------------------------------------------------------------------
-- 0. Helpers
-- -----------------------------------------------------------------------------------------------------

-- Postgres 18 ships uuidv7() natively. This wrapper is only a safety net for environments where the
-- extension form is used instead; on PG18 the built-in shadows nothing and this block is a no-op.
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'uuidv7') THEN
    RAISE EXCEPTION 'uuidv7() not available — this DDL targets PostgreSQL 18 (RFC 9562 native generator)';
  END IF;
END$$;

-- Generic updated_at bump, attached via triggers to every table carrying an updated_at column.
CREATE OR REPLACE FUNCTION control_plane.touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END$$;

-- -----------------------------------------------------------------------------------------------------
-- 0b. External stub — operators (owned by the dashboard schema/spec; stubbed here so FKs resolve)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dashboard.operators (
  id          uuid PRIMARY KEY DEFAULT uuidv7(),
  email       text NOT NULL UNIQUE,
  display_name text,
  status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at  timestamptz NOT NULL DEFAULT now()
);
COMMENT ON TABLE dashboard.operators IS
  'STUB — canonical definition lives in the Admin Dashboard spec. Present only to satisfy created_by FKs.';

-- =====================================================================================================
-- DOMAIN MODEL — a CUSTOMER owns one or more SMPP ACCOUNTS.
--   customer_groups 1 ─ N customers 1 ─ N smpp_accounts 1 ─ 2 credentials (1 smpp_bind + 1 api_key)
-- Level ownership (§6.18):
--   CUSTOMER : billing config, balances (via balance_scope), sender IDs, group, reputation
--   ACCOUNT  : SMPP bind identity + API key, channels, rate limits/quotas, max_sessions, webhooks
-- =====================================================================================================

-- -----------------------------------------------------------------------------------------------------
-- 1. Customer groups (§6.17) — flat organizational segmentation, 0..1 per customer
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.customer_groups (
  id          uuid PRIMARY KEY DEFAULT uuidv7(),
  name        text NOT NULL UNIQUE,
  description text,
  status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active','archived')),
  created_by  uuid REFERENCES dashboard.operators(id),
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------------------------------
-- 2. Rate plans (§3.1) — SMS balance is an INTEGER credit count, never monetary. Priced PER SEGMENT.
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.rate_plans (
  id                            uuid PRIMARY KEY DEFAULT uuidv7(),
  name                          text NOT NULL,
  credits_per_segment_mt_json   jsonb NOT NULL,  -- integer credits per MT segment, by MCC-MNC/country + sender type
  credits_per_segment_mo_json   jsonb NOT NULL,  -- integer credits per MO segment
  billing_mode                  text NOT NULL DEFAULT 'either'
                                  CHECK (billing_mode IN ('prepaid','postpaid','either')),
  charge_on                     text NOT NULL DEFAULT 'submission'
                                  CHECK (charge_on IN ('submission','delivery')),  -- delivery -> refund on fail/expire
  status                        text NOT NULL DEFAULT 'active'
                                  CHECK (status IN ('active','disabled')),
  created_at                    timestamptz NOT NULL DEFAULT now(),
  updated_at                    timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------------------------------
-- 3. External billing providers (§6.10) — referenced by billing_customers and customers via config
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.external_billing_providers (
  id                 uuid PRIMARY KEY DEFAULT uuidv7(),
  name               text NOT NULL,
  base_url           text NOT NULL,
  auth_config_json   jsonb NOT NULL DEFAULT '{}'::jsonb,
  mode               text NOT NULL
                       CHECK (mode IN ('balance_check','consume_delegate_async','consume_delegate_sync','both')),
  cache_ttl_ms       integer NOT NULL DEFAULT 1000 CHECK (cache_ttl_ms >= 0),
  sync_call_timeout_ms integer CHECK (sync_call_timeout_ms IS NULL OR sync_call_timeout_ms > 0),
  failure_policy     text NOT NULL DEFAULT 'fail_open' CHECK (failure_policy IN ('fail_open','fail_closed')),
  status             text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------------------------------
-- 4. Customers (§6.18) — billing config, sender IDs, group, reputation, content policy.
--    (content_key_id FK is added later; customers <-> content_keys is a circular reference.)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.customers (
  id                     uuid PRIMARY KEY DEFAULT uuidv7(),
  name                   text NOT NULL,
  status                 text NOT NULL DEFAULT 'active'
                           CHECK (status IN ('active','suspended','closed')),  -- suspending cascades to accounts
  group_id               uuid REFERENCES control_plane.customer_groups(id) ON DELETE SET NULL,

  -- Billing CONFIG (balances live in `balances`)
  rate_plan_id           uuid REFERENCES control_plane.rate_plans(id),
  billing_enabled        boolean NOT NULL DEFAULT false,          -- opt-in master switch for this customer
  billing_mode           text CHECK (billing_mode IN ('prepaid','postpaid')),  -- MT only; MO is always a meter (§6.9)
  overdraft_enabled      boolean NOT NULL DEFAULT false,          -- prepaid MT only
  overdraft_limit        integer CHECK (overdraft_limit IS NULL OR overdraft_limit >= 0),  -- max negative MT balance
  balance_scope          text NOT NULL DEFAULT 'customer'
                           CHECK (balance_scope IN ('customer','smpp_account')),  -- who owns balances (§6.9)
  mo_billing_floor       integer,                                 -- how negative the MO meter may run before stop+alert

  -- Content storage policy (§6.23) — governs CDR storage ONLY; logs/traces NEVER carry the body (§6.11)
  content_storage        text NOT NULL DEFAULT 'inherit'
                           CHECK (content_storage IN ('inherit','off','stored_plaintext','stored_encrypted')),
  content_retention_days integer CHECK (content_retention_days IS NULL OR content_retention_days >= 0),
  content_key_id         uuid,                                    -- FK -> content_keys, added by ALTER below

  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now(),

  -- overdraft only makes sense with a limit
  CONSTRAINT customers_overdraft_ck CHECK (NOT overdraft_enabled OR overdraft_limit IS NOT NULL)
);
CREATE INDEX customers_group_idx ON control_plane.customers(group_id);

-- -----------------------------------------------------------------------------------------------------
-- 5. Content keys (§6.23) — per-customer data keys for content encryption at rest
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.content_keys (
  id           uuid PRIMARY KEY DEFAULT uuidv7(),
  customer_id  uuid NOT NULL REFERENCES control_plane.customers(id) ON DELETE CASCADE,
  wrapped_key  bytea NOT NULL,          -- KMS-wrapped data key; plaintext exists only transiently in memory
  kms_key_ref  text NOT NULL,
  status       text NOT NULL DEFAULT 'active'
                 CHECK (status IN ('active','retired','destroyed')),  -- destroyed = crypto-shred (RGPD)
  created_at   timestamptz NOT NULL DEFAULT now(),
  retired_at   timestamptz,
  destroyed_at timestamptz
);
CREATE INDEX content_keys_customer_idx ON control_plane.content_keys(customer_id);
-- at most one active key per customer
CREATE UNIQUE INDEX content_keys_one_active_idx
  ON control_plane.content_keys(customer_id) WHERE status = 'active';

-- resolve the circular reference now that both tables exist
ALTER TABLE control_plane.customers
  ADD CONSTRAINT customers_content_key_fk
  FOREIGN KEY (content_key_id) REFERENCES control_plane.content_keys(id);

-- -----------------------------------------------------------------------------------------------------
-- 6. SMPP accounts (§6.18) — one technical account of a customer (per app/env/brand)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.smpp_accounts (
  id                 uuid PRIMARY KEY DEFAULT uuidv7(),
  customer_id        uuid NOT NULL REFERENCES control_plane.customers(id) ON DELETE CASCADE,
  name               text NOT NULL,                       -- unique within the customer
  status             text NOT NULL DEFAULT 'active'
                       CHECK (status IN ('active','suspended','closed')),  -- effective = min(customer, this)
  smpp_enabled       boolean NOT NULL DEFAULT true,        -- may open SMPP binds?
  rest_enabled       boolean NOT NULL DEFAULT true,        -- may call the REST API?
  sender_id_policy   text NOT NULL DEFAULT 'strict'
                       CHECK (sender_id_policy IN ('strict','allow_unregistered_numeric','disabled')),  -- §6.19
  query_sm_enabled   boolean NOT NULL DEFAULT true,        -- optional SMPP op (§6.22)
  cancel_sm_enabled  boolean NOT NULL DEFAULT true,
  allowed_bind_types text NOT NULL DEFAULT 'trx'
                       CHECK (allowed_bind_types IN ('tx','rx','trx')),
  max_sessions       integer NOT NULL DEFAULT 1 CHECK (max_sessions >= 0),  -- enforced at bind (§6.3)
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT smpp_accounts_channel_ck CHECK (smpp_enabled OR rest_enabled),
  CONSTRAINT smpp_accounts_name_uq UNIQUE (customer_id, name)
);
CREATE INDEX smpp_accounts_customer_idx ON control_plane.smpp_accounts(customer_id);

-- -----------------------------------------------------------------------------------------------------
-- 7. Credentials (§6.3/§6.18) — EXACTLY two rows per account: one smpp_bind, one api_key
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.credentials (
  id                   uuid PRIMARY KEY DEFAULT uuidv7(),
  account_id           uuid NOT NULL REFERENCES control_plane.smpp_accounts(id) ON DELETE CASCADE,
  type                 text NOT NULL CHECK (type IN ('smpp_bind','api_key')),
  system_id            text,           -- set for smpp_bind
  password_hash        text,           -- set for smpp_bind
  api_key_hash         text,           -- set for api_key
  status               text NOT NULL DEFAULT 'active'
                         CHECK (status IN ('active','disabled','revoked')),
  last_used_at         timestamptz,
  previous_secret_hash text,           -- set only during a manual rotation grace window (§6.3)
  grace_expires_at     timestamptz,
  created_at           timestamptz NOT NULL DEFAULT now(),
  rotated_at           timestamptz,
  -- the cardinality rule, enforced by the schema (§6.18)
  CONSTRAINT credentials_one_per_type_uq UNIQUE (account_id, type),
  -- shape the row by type
  CONSTRAINT credentials_shape_ck CHECK (
    (type = 'smpp_bind' AND system_id IS NOT NULL AND password_hash IS NOT NULL AND api_key_hash IS NULL)
    OR
    (type = 'api_key'   AND api_key_hash IS NOT NULL AND system_id IS NULL AND password_hash IS NULL)
  )
);
-- system_id must be globally unique among live bind credentials (bind auth resolves by system_id)
CREATE UNIQUE INDEX credentials_system_id_uq
  ON control_plane.credentials(system_id) WHERE type = 'smpp_bind' AND status <> 'revoked';

-- -----------------------------------------------------------------------------------------------------
-- 8. Sender IDs (§6.19) — CUSTOMER-level; carrier/regulatory registration negotiated once per customer
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.sender_ids (
  id          uuid PRIMARY KEY DEFAULT uuidv7(),
  customer_id uuid NOT NULL REFERENCES control_plane.customers(id) ON DELETE CASCADE,
  address     text NOT NULL,          -- alphanumeric or MSISDN
  status      text NOT NULL DEFAULT 'pending_carrier_approval'
                CHECK (status IN ('pending_carrier_approval','active','disabled')),
  created_by  uuid REFERENCES dashboard.operators(id),
  approved_at timestamptz,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT sender_ids_uq UNIQUE (customer_id, address)
);
CREATE INDEX sender_ids_lookup_idx ON control_plane.sender_ids(customer_id, address) WHERE status = 'active';

-- -----------------------------------------------------------------------------------------------------
-- 9. SMSC connectors (§6.8/§6.13/§6.15) — outbound ESME links to carrier SMSCs
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.smsc_connectors (
  id                              uuid PRIMARY KEY DEFAULT uuidv7(),
  name                            text NOT NULL UNIQUE,
  host                            text NOT NULL,
  port                            integer NOT NULL CHECK (port BETWEEN 1 AND 65535),
  bind_type                       text NOT NULL DEFAULT 'trx' CHECK (bind_type IN ('tx','rx','trx')),
  system_id                       text NOT NULL,
  password_hash                   text NOT NULL,
  vendor_profile                  text,     -- optional preset pre-filling the fields below; explicit values override

  system_type                     text NOT NULL DEFAULT '',
  interface_version               smallint NOT NULL DEFAULT 52,   -- 0x34=52 -> v3.4 ; 0x50=80 -> v5.0
  addr_ton                        smallint NOT NULL DEFAULT 0,
  addr_npi                        smallint NOT NULL DEFAULT 1,
  address_range                   text NOT NULL DEFAULT '',
  source_addr_ton                 smallint NOT NULL DEFAULT 5,
  source_addr_npi                 smallint NOT NULL DEFAULT 0,
  dest_addr_ton                   smallint NOT NULL DEFAULT 1,
  dest_addr_npi                   smallint NOT NULL DEFAULT 1,
  data_coding_default             smallint,     -- per-connector coding override (§6.6); null = auto-detected
  registered_delivery_default     smallint NOT NULL DEFAULT 1,   -- request DLR
  replace_if_present_flag_default smallint NOT NULL DEFAULT 0,
  esm_class_default               smallint NOT NULL DEFAULT 0,
  priority_flag_default           smallint NOT NULL DEFAULT 0,
  validity_period_default         text,
  sm_default_msg_id               smallint NOT NULL DEFAULT 0,

  enquire_link_interval_sec       integer NOT NULL DEFAULT 30 CHECK (enquire_link_interval_sec > 0),
  enquire_link_max_missed         integer NOT NULL DEFAULT 3  CHECK (enquire_link_max_missed > 0),
  bind_timeout_ms                 integer NOT NULL DEFAULT 5000 CHECK (bind_timeout_ms > 0),
  response_timeout_ms             integer NOT NULL DEFAULT 5000 CHECK (response_timeout_ms > 0),
  window_size                     integer NOT NULL DEFAULT 10 CHECK (window_size > 0),  -- max outstanding submit_sm
  bind_pool_size                  integer NOT NULL DEFAULT 1 CHECK (bind_pool_size BETWEEN 1 AND 32),  -- §6.8
  throughput_limit_per_sec        integer CHECK (throughput_limit_per_sec IS NULL OR throughput_limit_per_sec > 0),

  tls_enabled                     boolean NOT NULL DEFAULT false,
  tls_config_json                 jsonb,
  priority_tier                   integer NOT NULL DEFAULT 0,

  -- coarse config status. Live health is reported at runtime via link_status + breaker_state, NEVER conflated.
  status                          text NOT NULL DEFAULT 'active'
                                    CHECK (status IN ('active','degraded','disabled')),

  auto_reconnect_enabled          boolean NOT NULL DEFAULT false,   -- opt-in (§6.13)
  reconnect_initial_delay_ms      integer NOT NULL DEFAULT 1000 CHECK (reconnect_initial_delay_ms > 0),
  reconnect_multiplier            numeric(4,2) NOT NULL DEFAULT 2.0 CHECK (reconnect_multiplier >= 1.0),
  reconnect_max_delay_ms          integer NOT NULL DEFAULT 60000 CHECK (reconnect_max_delay_ms > 0),
  reconnect_jitter_pct            integer NOT NULL DEFAULT 20 CHECK (reconnect_jitter_pct BETWEEN 0 AND 100),
  reconnect_max_attempts          integer NOT NULL DEFAULT 0 CHECK (reconnect_max_attempts >= 0),  -- 0 = infinite

  created_at                      timestamptz NOT NULL DEFAULT now(),
  updated_at                      timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------------------------------
-- 10. Routes (§6.1) — declarative matching + distribution strategy
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.routes (
  id                    uuid PRIMARY KEY DEFAULT uuidv7(),
  name                  text NOT NULL,
  priority              integer NOT NULL DEFAULT 100,    -- lower = evaluated first
  match_account_id      uuid REFERENCES control_plane.smpp_accounts(id) ON DELETE CASCADE,
  match_customer_id     uuid REFERENCES control_plane.customers(id) ON DELETE CASCADE,  -- match every account
  match_sender_pattern  text,     -- regex/glob
  match_dest_pattern    text,     -- MSISDN prefix / MCC-MNC
  match_content_pattern text,     -- regex/keyword
  distribution_strategy text NOT NULL DEFAULT 'static'
                          CHECK (distribution_strategy IN
                            ('static','round_robin','weighted','failover_priority','least_loaded','hash_based')),
  target_connector_id   uuid REFERENCES control_plane.smsc_connectors(id),  -- used only when strategy = static
  fallback_route_id     uuid REFERENCES control_plane.routes(id) ON DELETE SET NULL,  -- self-fk
  status                text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  -- a static route must name exactly one connector; non-static routes use route_targets instead
  CONSTRAINT routes_static_target_ck CHECK (
    (distribution_strategy = 'static' AND target_connector_id IS NOT NULL)
    OR (distribution_strategy <> 'static')
  )
);
CREATE INDEX routes_priority_idx ON control_plane.routes(priority) WHERE status = 'active';

-- -----------------------------------------------------------------------------------------------------
-- 11. Route targets (§6.1) — used when distribution_strategy != static; >= 2 rows expected
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.route_targets (
  route_id     uuid NOT NULL REFERENCES control_plane.routes(id) ON DELETE CASCADE,
  connector_id uuid NOT NULL REFERENCES control_plane.smsc_connectors(id) ON DELETE CASCADE,
  weight       integer NOT NULL DEFAULT 1 CHECK (weight > 0),        -- used by 'weighted'
  priority     integer NOT NULL DEFAULT 0,                            -- used by 'failover_priority'
  PRIMARY KEY (route_id, connector_id)
);

-- -----------------------------------------------------------------------------------------------------
-- 12. Routing scripts (§6.2) — admin-authored, scoped (NOT bound to a route)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.routing_scripts (
  id               uuid PRIMARY KEY DEFAULT uuidv7(),
  scope            text NOT NULL CHECK (scope IN ('platform','customer','smpp_account')),
  scope_id         uuid,          -- matching scope; null for platform. Polymorphic -> no single FK.
  name             text NOT NULL,
  language         text NOT NULL CHECK (language IN ('js','lua')),
  source_code      text NOT NULL,
  checksum         text NOT NULL,
  status           text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft','active','disabled')),
  timeout_ms       integer NOT NULL DEFAULT 2 CHECK (timeout_ms > 0 AND timeout_ms <= 20),  -- hard cap ~20ms
  max_instructions bigint,
  max_memory_kb    integer,
  created_by       uuid REFERENCES dashboard.operators(id),
  created_at       timestamptz NOT NULL DEFAULT now(),
  published_at     timestamptz,
  CONSTRAINT routing_scripts_scope_ck CHECK (
    (scope = 'platform' AND scope_id IS NULL) OR (scope <> 'platform' AND scope_id IS NOT NULL)
  )
);
-- at most one ACTIVE script per (scope, scope_id); NULLS NOT DISTINCT folds the platform row (§6.2)
CREATE UNIQUE INDEX routing_scripts_one_active_idx
  ON control_plane.routing_scripts(scope, scope_id) NULLS NOT DISTINCT
  WHERE status = 'active';

-- -----------------------------------------------------------------------------------------------------
-- 13. Rate limits (§6.4) — operational governor, always below the connector's technical ceiling
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.rate_limits (
  id             uuid PRIMARY KEY DEFAULT uuidv7(),
  entity_type    text NOT NULL CHECK (entity_type IN ('smpp_account','connector','route')),  -- no customer/group
  entity_id      uuid NOT NULL,      -- polymorphic -> no single FK
  max_per_sec    integer CHECK (max_per_sec IS NULL OR max_per_sec > 0),
  max_per_day    integer CHECK (max_per_day IS NULL OR max_per_day > 0),
  burst_capacity integer CHECK (burst_capacity IS NULL OR burst_capacity >= 0),
  created_at     timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT rate_limits_uq UNIQUE (entity_type, entity_id)
);
-- NOTE (§6.4): for entity_type='connector', max_per_sec MUST be <= smsc_connectors.throughput_limit_per_sec
-- when the latter is set. Enforced in the application on write (cross-table CHECKs are not portable here).

-- -----------------------------------------------------------------------------------------------------
-- 14. Anti-spam rules (§6.5)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.antispam_rules (
  id          uuid PRIMARY KEY DEFAULT uuidv7(),
  rule_type   text NOT NULL CHECK (rule_type IN ('velocity','content_blacklist','duplicate','reputation')),
  scope       text NOT NULL CHECK (scope IN ('global','customer','smpp_account')),  -- resolve acct->cust->global
  scope_id    uuid,      -- polymorphic; null for global
  config_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  action      text NOT NULL CHECK (action IN ('block','flag','throttle')),
  status      text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT antispam_scope_ck CHECK (
    (scope = 'global' AND scope_id IS NULL) OR (scope <> 'global' AND scope_id IS NOT NULL)
  )
);
CREATE INDEX antispam_scope_idx ON control_plane.antispam_rules(scope, scope_id) WHERE status = 'active';

-- -----------------------------------------------------------------------------------------------------
-- 15. Webhooks (§6.18) — per SMPP ACCOUNT
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.webhooks (
  id                uuid PRIMARY KEY DEFAULT uuidv7(),
  account_id        uuid NOT NULL REFERENCES control_plane.smpp_accounts(id) ON DELETE CASCADE,
  event_type        text NOT NULL CHECK (event_type IN ('mo','dlr')),
  url               text NOT NULL,
  secret            text NOT NULL,        -- HMAC-SHA256 signing secret
  retry_policy_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  status            text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT webhooks_uq UNIQUE (account_id, event_type)
);

-- -----------------------------------------------------------------------------------------------------
-- 16. Sender ID rewrite rules (§6.16) — provider-side, evaluated just before dispatch
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.sender_id_rewrite_rules (
  id                   uuid PRIMARY KEY DEFAULT uuidv7(),
  scope                text NOT NULL CHECK (scope IN ('platform','customer','smpp_account','connector')),
  scope_id             uuid,          -- matching scope; null for platform
  direction            text NOT NULL DEFAULT 'mt' CHECK (direction IN ('mt','mo')),
  match_sender_pattern text,
  match_dest_pattern   text,
  rewrite_type         text NOT NULL CHECK (rewrite_type IN ('static','fallback_pool','truncate','sanitize')),
  rewrite_to           text,          -- used when static
  fallback_pool_json   jsonb,         -- round-robin list, used when fallback_pool
  max_length           integer CHECK (max_length IS NULL OR max_length > 0),
  sanitize_charset_json jsonb,
  priority             integer NOT NULL DEFAULT 100,
  reason               text,
  status               text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_by           uuid REFERENCES dashboard.operators(id),
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT sender_rewrite_scope_ck CHECK (
    (scope = 'platform' AND scope_id IS NULL) OR (scope <> 'platform' AND scope_id IS NOT NULL)
  ),
  CONSTRAINT sender_rewrite_static_ck CHECK (rewrite_type <> 'static' OR rewrite_to IS NOT NULL)
);
CREATE INDEX sender_rewrite_scope_idx
  ON control_plane.sender_id_rewrite_rules(scope, scope_id, priority) WHERE status = 'active';

-- -----------------------------------------------------------------------------------------------------
-- 17. Inbound numbers (§6.21) — shortcodes / long codes owned by the provider
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.inbound_numbers (
  id           uuid PRIMARY KEY DEFAULT uuidv7(),
  address      text NOT NULL,
  number_type  text NOT NULL CHECK (number_type IN ('shortcode','longcode','alphanumeric')),
  country_code text NOT NULL,
  mccmnc       text,
  connector_id uuid REFERENCES control_plane.smsc_connectors(id) ON DELETE SET NULL,  -- which link delivers MO
  account_id   uuid REFERENCES control_plane.smpp_accounts(id) ON DELETE SET NULL,    -- dedicated; NULL = shared
  status       text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT inbound_numbers_uq UNIQUE (address, country_code)
);
CREATE INDEX inbound_numbers_account_idx ON control_plane.inbound_numbers(account_id);

-- -----------------------------------------------------------------------------------------------------
-- 18. Inbound keywords (§6.21) — for SHARED inbound numbers only
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.inbound_keywords (
  id                uuid PRIMARY KEY DEFAULT uuidv7(),
  inbound_number_id uuid NOT NULL REFERENCES control_plane.inbound_numbers(id) ON DELETE CASCADE,
  keyword           text NOT NULL,
  match_type        text NOT NULL DEFAULT 'prefix' CHECK (match_type IN ('exact','prefix','regex')),
  account_id        uuid NOT NULL REFERENCES control_plane.smpp_accounts(id) ON DELETE CASCADE,
  priority          integer NOT NULL DEFAULT 0,
  status            text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX inbound_keywords_lookup_idx
  ON control_plane.inbound_keywords(inbound_number_id, priority) WHERE status = 'active';

-- -----------------------------------------------------------------------------------------------------
-- 19. Exact routes (§6.1) — MSISDN -> target; typically bulk-loaded from an MNP/portability database
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.exact_routes (
  msisdn      text PRIMARY KEY,        -- E.164
  target_type text NOT NULL CHECK (target_type IN ('connector','route')),
  target_id   uuid NOT NULL,           -- polymorphic (smsc_connectors.id | routes.id)
  source      text NOT NULL DEFAULT 'manual' CHECK (source IN ('mnp_import','manual','carrier_feed')),
  imported_at timestamptz,
  updated_at  timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------------------------------
-- 20. Suppressions (§6.20) — opt-out list, PER CHANNEL
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.suppressions (
  id         uuid PRIMARY KEY DEFAULT uuidv7(),
  scope      text NOT NULL CHECK (scope IN ('inbound_number','smpp_account','customer','platform')),
  scope_id   uuid,          -- inbound_numbers.id | smpp_accounts.id | customers.id ; null for platform
  msisdn     text NOT NULL, -- E.164, normalized at write
  source     text NOT NULL CHECK (source IN ('mo_stop','admin','import','carrier','regulator')),
  reason     text,
  created_at timestamptz NOT NULL DEFAULT now(),
  -- NULLS NOT DISTINCT so the platform scope (scope_id NULL) is unique per msisdn
  CONSTRAINT suppressions_uq UNIQUE NULLS NOT DISTINCT (scope, scope_id, msisdn),
  CONSTRAINT suppressions_scope_ck CHECK (
    (scope = 'platform' AND scope_id IS NULL) OR (scope <> 'platform' AND scope_id IS NOT NULL)
  )
);
-- opt-out check hot path: given a msisdn, find any applicable scope fast
CREATE INDEX suppressions_msisdn_idx ON control_plane.suppressions(msisdn);

-- -----------------------------------------------------------------------------------------------------
-- 21. Opt-out keywords (§6.20) — per country/locale; seeded defaults, admin-editable
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.opt_out_keywords (
  id                  uuid PRIMARY KEY DEFAULT uuidv7(),
  country_code        text,             -- null = applies to all
  keyword             text NOT NULL,    -- STOP, ARRET, START, UNSTOP, HELP...
  action              text NOT NULL CHECK (action IN ('suppress','unsuppress','help')),
  match_type          text NOT NULL DEFAULT 'exact' CHECK (match_type IN ('exact','prefix')),
  auto_reply_template text,             -- auto-reply is an MT, never billed (§6.20)
  status              text NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT opt_out_keywords_uq UNIQUE NULLS NOT DISTINCT (country_code, keyword)
);

-- -----------------------------------------------------------------------------------------------------
-- 22. Balances (§6.9) — THE balance table. One row per (owner, direction). owner_id is polymorphic.
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.balances (
  owner_type text NOT NULL CHECK (owner_type IN ('customer','smpp_account')),  -- decided by balance_scope
  owner_id   uuid NOT NULL,       -- customers.id OR smpp_accounts.id
  direction  text NOT NULL CHECK (direction IN ('mt','mo')),                    -- MT and MO are separate
  credits    integer NOT NULL DEFAULT 0,     -- always an integer count, never monetary (may go negative)
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (owner_type, owner_id, direction)
);

-- -----------------------------------------------------------------------------------------------------
-- 23. Billing customers (§6.9) — billing config per customer (balances live in `balances`)
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.billing_customers (
  customer_id                  uuid PRIMARY KEY REFERENCES control_plane.customers(id) ON DELETE CASCADE,
  billing_mode                 text NOT NULL CHECK (billing_mode IN ('prepaid','postpaid')),   -- MT only
  overdraft_enabled            boolean NOT NULL DEFAULT false,
  overdraft_limit              integer CHECK (overdraft_limit IS NULL OR overdraft_limit >= 0),
  credit_limit                 integer CHECK (credit_limit IS NULL OR credit_limit >= 0),  -- postpaid MT soft-limit
  credit_limit_is_hard         boolean NOT NULL DEFAULT false,
  external_billing_provider_id uuid REFERENCES control_plane.external_billing_providers(id) ON DELETE SET NULL,
  updated_at                   timestamptz NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------------------------------
-- 24. Billing ledger (§6.9/§6.14) — append-only, PARTITIONED BY DAY on created_at
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.billing_ledger (
  id            uuid NOT NULL DEFAULT uuidv7(),
  owner_type    text NOT NULL CHECK (owner_type IN ('customer','smpp_account')),
  owner_id      uuid NOT NULL,
  direction     text NOT NULL CHECK (direction IN ('mt','mo')),
  customer_id   uuid NOT NULL REFERENCES control_plane.customers(id) ON DELETE CASCADE,
  account_id    uuid REFERENCES control_plane.smpp_accounts(id) ON DELETE SET NULL,  -- attribution for shared pool
  message_id    uuid,      -- null for manual top-ups/adjustments (lives in the CDR store; no FK here)
  entry_type    text NOT NULL CHECK (entry_type IN ('reserve','capture','release','refund','topup','adjustment')),
  credits       integer NOT NULL,          -- signed
  balance_after integer NOT NULL,
  reference     text,
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (id, created_at)              -- partition key must be part of the PK
) PARTITION BY RANGE (created_at);

-- Idempotency under at-least-once delivery (§6.9): one (message_id, entry_type) per message.
-- Postgres requires every UNIQUE index on a partitioned table to include the partition key
-- (created_at), so this index is a *same-partition* (same-day) backstop. Reserve and capture for a
-- given message happen seconds apart (message_id is a time-ordered UUIDv7), so they share a day
-- partition in the overwhelming majority of cases. The AUTHORITATIVE cross-partition idempotency
-- guard is the application's Redis reservation key `billing:reservation:{message_id}` plus an
-- app-level existence check against the ledger before capture (§6.9) — this index cannot span
-- partitions and must not be relied on alone at a day boundary.
CREATE UNIQUE INDEX billing_ledger_idem_idx
  ON control_plane.billing_ledger(message_id, entry_type, created_at)
  WHERE message_id IS NOT NULL;
CREATE INDEX billing_ledger_customer_idx ON control_plane.billing_ledger(customer_id, created_at);

-- Seed partitions. In production a scheduler (pg_partman / cron) creates the next day's partition ahead
-- of time and detaches old ones to object storage (§6.14.2). Two examples + a catch-all default:
CREATE TABLE control_plane.billing_ledger_default
  PARTITION OF control_plane.billing_ledger DEFAULT;

-- -----------------------------------------------------------------------------------------------------
-- 25. updated_at triggers
-- -----------------------------------------------------------------------------------------------------
DO $$
DECLARE
  t text;
  tables text[] := ARRAY[
    'customer_groups','rate_plans','external_billing_providers','customers','smpp_accounts',
    'sender_ids','smsc_connectors','routes','rate_limits','antispam_rules','webhooks',
    'sender_id_rewrite_rules','inbound_numbers','inbound_keywords','exact_routes','opt_out_keywords',
    'balances','billing_customers'
  ];
BEGIN
  FOREACH t IN ARRAY tables LOOP
    EXECUTE format(
      'CREATE TRIGGER %I_touch BEFORE UPDATE ON control_plane.%I
         FOR EACH ROW EXECUTE FUNCTION control_plane.touch_updated_at()', t, t);
  END LOOP;
END$$;
