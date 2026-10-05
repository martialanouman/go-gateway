// Package bindfailure keeps the recent refused SMPP binds of each account, for the Admin API's
// diagnostic (BO §6.14). Only a bind whose system_id resolves a credential is recorded: an unknown
// system_id belongs to no account, and must not let an attacker mint keys.
package bindfailure

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Retention and Cap bound each account's list, declared to the contract: a burst rotates it, never grows it.
const (
	Retention = 24 * time.Hour
	Cap       = 200
)

// Reason is the internal cause of a refused bind, never shown to the ESME (§11.3).
type Reason string

// The closed set of reasons, mirrored by the Admin contract's enum.
const (
	ReasonPasswordMismatch    Reason = "password_mismatch"
	ReasonCredentialRevoked   Reason = "credential_revoked"
	ReasonCredentialDisabled  Reason = "credential_disabled"
	ReasonAccountInactive     Reason = "account_inactive"
	ReasonSMPPChannelDisabled Reason = "smpp_channel_disabled"
	ReasonBindTypeNotAllowed  Reason = "bind_type_not_allowed"
	ReasonMaxSessions         Reason = "max_sessions_exceeded"
	ReasonThrottled           Reason = "throttled"
	ReasonRegistryUnavailable Reason = "registry_unavailable"
)

// Failure is one refused bind. It has no field able to carry the presented secret (invariant a).
type Failure struct {
	At            time.Time `json:"at"`
	RemoteIP      string    `json:"remote_ip"`
	BindType      string    `json:"bind_type"`
	CommandStatus string    `json:"command_status"`
	Reason        Reason    `json:"reason"`
}

// Log stores the failures in one capped Redis list per account.
type Log struct {
	rdb *redis.Client
}

// New returns a Log backed by rdb.
func New(rdb *redis.Client) *Log { return &Log{rdb: rdb} }

// Record prepends f to the account's list.
func (l *Log) Record(ctx context.Context, accountID uuid.UUID, f Failure) error {
	entry, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("bindfailure: encode: %w", err)
	}
	key := key(accountID)
	_, err = l.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.LPush(ctx, key, entry)
		p.LTrim(ctx, key, 0, Cap-1)
		p.Expire(ctx, key, Retention)
		return nil
	})
	if err != nil {
		return fmt.Errorf("bindfailure: record: %w", err)
	}
	return nil
}

// List returns the account's failures at or after since, newest first.
// The key's TTL slides with each failure, so entries older than Retention can outlive it: since is
// what bounds the answer.
func (l *Log) List(ctx context.Context, accountID uuid.UUID, since time.Time) ([]Failure, error) {
	raw, err := l.rdb.LRange(ctx, key(accountID), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("bindfailure: list: %w", err)
	}
	out := make([]Failure, 0, len(raw))
	for _, r := range raw {
		var f Failure
		if err := json.Unmarshal([]byte(r), &f); err != nil {
			return nil, fmt.Errorf("bindfailure: decode: %w", err)
		}
		if f.At.Before(since) {
			continue
		}
		out = append(out, f)
	}
	return out, nil
}

func key(accountID uuid.UUID) string { return "bindfail:log:{" + accountID.String() + "}" }
