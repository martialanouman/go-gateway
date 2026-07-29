-- =====================================================================================================
--  SMS Gateway — Control-plane DDL
--  Target: PostgreSQL 18  (native uuidv7(), NULLS NOT DISTINCT, declarative partitioning)
--  Companion spec: specification-technique-passerelle-sms.md  (RESHADED, v2.0, §3.1)
--
--  Convention (per team): all schema, identifiers and comments are in English; only prose docs are
--  in French. All primary keys `id` are UUIDv7 (RFC 9562) unless stated otherwise. message_id and
--  trace_id are generated at ingress by the application and never appear here (they live in the CDR
--  columnar store, §3.4). See the ClickHouse appendix at the bottom of this file for the CDR DDL,
--  and the Redis / Kafka appendix for the non-relational keyspaces (given as reference comments only —
--  they are NOT executable SQL and are fenced off).
--
--  Run order matters (a couple of circular FKs are resolved with deferred ALTERs). Idempotent-ish:
--  uses IF NOT EXISTS where safe. Meant to be applied by a migration tool, not hand-run in prod.
-- =====================================================================================================

BEGIN;

SET client_min_messages = warning;

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
  CONSTRAINT customers_overdraft_ck CHECK (NOT overdraft_enabled OR overdraft_limit IS NOT NULL),
  -- An account-scoped balance may not carry a customer-level overdraft: the limit would apply to EACH
  -- account balance independently, multiplying the customer's credit exposure by the account count (§6.9,
  -- step-142c). Account-scoped balances are strict prepaid or soft postpaid only.
  CONSTRAINT customers_account_scope_no_overdraft_ck
    CHECK (balance_scope <> 'smpp_account' OR NOT overdraft_enabled)
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
-- 18b. Unrouted MO (§6.21) — mobile-originated messages that resolved to no account, kept for the
-- operator to see (list-unrouted-mo) and fix the config. A configuration anomaly, not a billable CDR:
-- a distinct table, never the message body (invariant a). Volume is low (an anomaly, not a flow).
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.unrouted_mo (
  id                uuid PRIMARY KEY DEFAULT uuidv7(),
  received_at       timestamptz NOT NULL DEFAULT now(),
  connector_id      uuid,                                   -- the link the MO arrived on (no FK: M2 env-injected)
  inbound_number_id uuid REFERENCES control_plane.inbound_numbers(id) ON DELETE SET NULL, -- null if the number is unknown
  source_addr       text NOT NULL,                          -- the subscriber MSISDN (E.164)
  dest_addr         text NOT NULL,                          -- the inbound number the MO targeted
  segment_count     integer NOT NULL DEFAULT 1,
  encoding          text NOT NULL,
  reason            text NOT NULL CHECK (reason IN ('unknown_number','number_disabled','no_keyword_match'))
);
-- Keyset pagination for list-unrouted-mo: newest first, id breaks ties.
CREATE INDEX unrouted_mo_page_idx ON control_plane.unrouted_mo(received_at DESC, id DESC);

-- -----------------------------------------------------------------------------------------------------
-- 19. Exact routes (§6.1) — MSISDN -> target; typically bulk-loaded from an MNP/portability database
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.exact_routes (
  msisdn      text PRIMARY KEY,        -- E.164 minus "+", digits only (see CHECK) — the L0 Bloom/Redis key form
  target_type text NOT NULL CHECK (target_type IN ('connector','route')),
  target_id   uuid NOT NULL,           -- polymorphic (smsc_connectors.id | routes.id)
  source      text NOT NULL DEFAULT 'manual' CHECK (source IN ('mnp_import','manual','carrier_feed')),
  imported_at timestamptz,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  -- The L0 Bloom snapshot and the exactroute:{msisdn} confirmation key are built on this exact form,
  -- and the router queries it with e164.Normalize output (digits only). A write path storing a
  -- non-normalized value ("+225…") would silently defeat every override — a routing false negative.
  CONSTRAINT exact_routes_msisdn_canonical_ck CHECK (msisdn ~ '^[1-9][0-9]+$')
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
  ),
  -- Enforce the canonical E.164-minus-"+" form (digits only, country code 1-9). No write path (admin,
  -- import, carrier, MO STOP) may store a non-normalized value: the opt-out Bloom and exact lookup key
  -- on this exact form, so a "+225…" or padded value would silently defeat suppression (§6.20).
  CONSTRAINT suppressions_msisdn_canonical_ck CHECK (msisdn ~ '^[1-9][0-9]+$')
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
  updated_at                   timestamptz NOT NULL DEFAULT now(),
  -- An enabled limit MUST carry its value (§6.9, step-142b): a flag without a value would fail closed to
  -- strict prepaid on the hot path and silently cut the customer off — reject it at write time instead.
  CONSTRAINT billing_customers_overdraft_limit_ck   CHECK (NOT overdraft_enabled OR overdraft_limit IS NOT NULL),
  CONSTRAINT billing_customers_hard_credit_limit_ck CHECK (NOT credit_limit_is_hard OR credit_limit IS NOT NULL)
);

-- An account-scoped balance may not carry an overdraft or a HARD credit limit (§6.9, step-142c): the
-- customer-level figure would apply per account and multiply the credit exposure by the account count.
-- billing_customers has no balance_scope, so this cross-table trigger consults the owning customer. It
-- raises with ERRCODE 23514 (check_violation) so the repo translates it to a clean 422, like a table CHECK.
CREATE OR REPLACE FUNCTION control_plane.forbid_account_scope_credit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF (NEW.overdraft_enabled OR NEW.credit_limit_is_hard)
     AND (SELECT balance_scope FROM control_plane.customers WHERE id = NEW.customer_id) = 'smpp_account' THEN
    RAISE EXCEPTION 'overdraft/hard credit limit is not allowed on an account-scoped balance (customer %)', NEW.customer_id
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER billing_customers_no_account_scope_credit
  BEFORE INSERT OR UPDATE ON control_plane.billing_customers
  FOR EACH ROW EXECUTE FUNCTION control_plane.forbid_account_scope_credit();

-- Reverse direction: flipping a customer to account-scoped must re-check its billing_customers row (the
-- customers CHECK above sees only customers.overdraft_enabled, not credit_limit_is_hard, which lives only
-- in billing_customers). Fires only when balance_scope is written (§6.9, step-142c).
CREATE OR REPLACE FUNCTION control_plane.forbid_account_scope_flip_with_credit()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.balance_scope = 'smpp_account'
     AND EXISTS (SELECT 1 FROM control_plane.billing_customers
                 WHERE customer_id = NEW.id AND (overdraft_enabled OR credit_limit_is_hard)) THEN
    RAISE EXCEPTION 'cannot switch customer % to account-scoped: it has an overdraft or hard credit limit', NEW.id
      USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER customers_no_account_scope_flip_with_credit
  BEFORE UPDATE OF balance_scope ON control_plane.customers
  FOR EACH ROW EXECUTE FUNCTION control_plane.forbid_account_scope_flip_with_credit();

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
CREATE TABLE control_plane.billing_ledger_2026_07_14
  PARTITION OF control_plane.billing_ledger
  FOR VALUES FROM ('2026-07-14 00:00:00+00') TO ('2026-07-15 00:00:00+00');
CREATE TABLE control_plane.billing_ledger_2026_07_15
  PARTITION OF control_plane.billing_ledger
  FOR VALUES FROM ('2026-07-15 00:00:00+00') TO ('2026-07-16 00:00:00+00');

-- -----------------------------------------------------------------------------------------------------
-- 24b. Billing idempotency (§6.9, invariant c) — AUTHORITATIVE cross-partition guard, NOT partitioned.
-- The billing_ledger idem index can only dedup within one day partition (Postgres requires the partition
-- key in a unique index). A message replayed after its Redis hold lapsed AND across a day boundary would
-- escape it and DOUBLE-DEBIT. RecordDurable claims (message_id, entry_type) here first, in the same tx;
-- a duplicate INSERT conflicts (0 rows) and the movement is skipped — the INSERT is the lock, no TOCTOU.
-- Restricted to message-bearing lifecycle types (top-ups/adjustments carry no message_id).
-- -----------------------------------------------------------------------------------------------------
CREATE TABLE control_plane.billing_idempotency (
  message_id uuid NOT NULL,
  entry_type text NOT NULL CHECK (entry_type IN ('reserve','capture','release','refund')),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (message_id, entry_type)
);
CREATE INDEX billing_idempotency_created_idx ON control_plane.billing_idempotency(created_at);

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

COMMIT;

-- =====================================================================================================
--  APPENDIX A — CDR / analytics store (ClickHouse dialect, §3.4) — VERSIONED write model (§1.10)
--  NOT PostgreSQL. Do NOT run this against Postgres. Authoritative CDR DDL reference; the shipped
--  ClickHouse migration (migrations/clickhouse/0001_cdr.up.sql) is derived from it and MUST agree.
--  message_id / trace_id are UUIDv7 generated at ingress; the sink carries them, it does not generate.
--
--  Per-message mutation is infeasible at 8000 msg/s, so a status change is a NEW row with the same
--  message_id and a higher `version`. ReplacingMergeTree keeps the highest-version row per sorting
--  key; a read takes the latest version explicitly (argMax / FINAL), correct even before a merge.
--
--  `version` is a LIFECYCLE RANK, not a timestamp: the later stage always supersedes, independent of
--  which service wrote the row or of clock skew between hosts. Ranks (spaced for M4+):
--      accepted=10  rerouted=15  enroute=20  rejected=20  delivered=40  failed=50  expired=50  cancelled=60  (rerouted < enroute: a fallback step superseded by the destination's enroute, step-125)
--
--  `submitted_at` is IMMUTABLE and repeated on every row for a message, keeping all of a message's
--  status rows in one partition even when a later status arrives on another day.
--
--  The status enum is the full REST MessageStatus lifecycle: it adds `accepted` (the pre-dispatch
--  row that keeps GET /messages/{id} 404-free, §1.10) and `cancelled` (M3) to the six the
--  connector/DLR path writes.
-- =====================================================================================================
/*
CREATE TABLE cdr
(
    message_id            UUID,                 -- application-generated at ingress, before Kafka or any DB
    trace_id              UUID,                 -- correlates to the full OpenTelemetry trace (§6.11)
    account_id            UUID,                 -- the SMPP account that sent/received
    customer_id           UUID,                 -- denormalized owner (an account never changes customer)
    direction             Enum8('mt' = 1, 'mo' = 2),
    source_addr           String,               -- address actually used on the wire (post-rewrite, if any)
    dest_addr             String,
    original_source_addr  Nullable(String),     -- set when a sender_id_rewrite_rule changed it (§6.16)
    connector_id          Nullable(UUID),
    route_id              Nullable(UUID),
    routing_script_id     Nullable(UUID),
    submitted_at          DateTime64(3),
    delivered_at          Nullable(DateTime64(3)),
    status                Enum8('accepted'=1,'enroute'=2,'delivered'=3,'failed'=4,'expired'=5,'rejected'=6,'rerouted'=7,'cancelled'=8),
    error_code            Nullable(String),
    segment_count         UInt16,
    encoding              Enum8('gsm7'=1,'ucs2'=2,'binary'=3),
    content_ciphertext    Nullable(String),     -- present only when content_storage is stored_* ; NEVER in logs
    content_key_id        Nullable(UUID),       -- which content_keys row decrypts it; destroyed key => unreadable
    latency_ms            Nullable(UInt32),
    billed                UInt8,                -- 0/1
    credits_charged       Nullable(Int32),
    version               UInt64                -- lifecycle rank; ReplacingMergeTree keeps the max
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(submitted_at)               -- daily partitions, TTL tiering (§6.14)
ORDER BY (customer_id, account_id, submitted_at, message_id)  -- all four immutable per message
TTL toDate(submitted_at) + INTERVAL 90 DAY;     -- CDR retention (configurable); body has its own shorter TTL
*/

-- =====================================================================================================
--  APPENDIX B — Redis / Dragonfly keyspaces (§3.2) and Kafka topics (§3.3)
--  Reference only — NOT SQL. The low-latency operational state and the durable data plane.
-- =====================================================================================================
/*
-- Redis / Dragonfly (cluster mode):
sess:{account_id}                                  -- sorted-set of "{pod_id}:{bind_id}" (score = expiry ts); atomic max_sessions quota at bind (Lua, §6.3, inv. d)
session:{bind_id}                                  -- session metadata + TTL heartbeat
ratelimit:{entity_type}:{entity_id}:{window}       -- atomic token-bucket counters (Lua)
dedupe:{account_id}:{content_hash}                  -- short-TTL set for duplicate-message anti-spam
reputation:{account_id}                             -- rolling spam-score, decayed
exactroute:{msisdn}                                 -- exact-number routing entry; read on Bloom possible-hit (§6.1)
suppress:{scope}:{scope_id}:{msisdn}                -- opt-out entry; read on Bloom possible-hit (§6.20)
billing:balance:{direction}:{owner_type}:{owner_id} -- cached balance; atomic Lua MT reserve/capture/release
billing:reservation:{message_id}                    -- short-TTL MT hold; cleared on capture/release
retry:delayed:{connector_id}                        -- sorted-set delay queue (score = due ts)
breaker:binds:{connector_id}                        -- HASH of per (pod_id, bind_index) sub-bind states
breaker:state:{connector_id}                        -- derived connector aggregate (closed|open|half_open)
connectorload:{connector_id}                        -- in-flight gauge per connector, for least_loaded (§6.1)
config:changed                                      -- pub/sub channel: Admin API announces a control-plane mutation; config-sync coalesces these
breaker:events                                      -- pub/sub channel for routing-snapshot invalidation (config-sync M7, circuit breaker M8)

-- Kafka topics (data plane):
mt.inbound        -- raw submissions (SMPP/REST), pre-routing. Partitioned by customer/account hash.
mt.routed         -- post-routing. Keyed by message_id (all of a message's segments share it → one partition, in order). The connector pool sub-shards a batch across its binds in memory by hash(message_id)%bind_pool_size (step-124).
mo.inbound        -- raw deliver_sm from SMSC connectors, pre-routing to accounts
dlr.events        -- delivery receipt events, correlated to original message ID
mt.dead-letter / mo.dead-letter   -- failed/expired after retry exhaustion (incl. exhausted fallback_chain)
mt.reroute-park   -- durable parking for large fallback reroute bursts; drained rate-limited (§6.15)
*/
