package modlrrouter

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
	"github.com/martialanouman/go-gateway/internal/platform/encoding"
	"github.com/martialanouman/go-gateway/internal/platform/msg"
)

// stopAckNamespace derives the deterministic message id of a STOP auto-reply from the MO's own id, so
// a redelivered mo.inbound never emits a second confirmation.
var stopAckNamespace = uuid.MustParse("7c3a1f9e-0b2d-5a4c-9e10-000000000047")

// OptOutKeywordLister loads the active opt-out keywords. *postgres.OptOutKeywordRepo satisfies it.
type OptOutKeywordLister interface {
	ListActive(ctx context.Context) ([]cp.OptOutKeyword, error)
}

// SuppressionWriter writes and removes suppressions. *postgres.SuppressionRepo satisfies it.
type SuppressionWriter interface {
	Create(ctx context.Context, in cp.NewSuppression) (bool, error)
	DeleteByKey(ctx context.Context, scope cp.SuppressionScope, scopeID *uuid.UUID, msisdn string) (bool, error)
}

type compiledOptOutKeyword struct {
	country   string // "" = every country
	keyword   string // upper-cased, trimmed
	matchType cp.OptOutMatchType
	action    cp.OptOutAction
	template  *string
}

// OptOutKeywords is an immutable, compiled snapshot of the active opt-out keywords, sorted by
// specificity so Match returns the most specific keyword first: a country-specific rule outranks a
// global one, an exact match outranks a prefix, and among prefixes the longest wins.
type OptOutKeywords struct {
	compiled []compiledOptOutKeyword
}

// LoadOptOutKeywords reads the active opt-out keywords once and compiles them for matching.
func LoadOptOutKeywords(ctx context.Context, lister OptOutKeywordLister) (*OptOutKeywords, error) {
	rows, err := lister.ListActive(ctx)
	if err != nil {
		return nil, fmt.Errorf("modlrrouter: load opt-out keywords: %w", err)
	}
	compiled := make([]compiledOptOutKeyword, 0, len(rows))
	for _, k := range rows {
		country := ""
		if k.CountryCode != nil {
			country = strings.ToUpper(strings.TrimSpace(*k.CountryCode))
		}
		compiled = append(compiled, compiledOptOutKeyword{
			country:   country,
			keyword:   strings.ToUpper(strings.TrimSpace(k.Keyword)),
			matchType: k.MatchType,
			action:    k.Action,
			template:  k.AutoReplyTemplate,
		})
	}
	sort.SliceStable(compiled, func(i, j int) bool {
		a, b := compiled[i], compiled[j]
		if (a.country != "") != (b.country != "") {
			return a.country != "" // country-specific first
		}
		if (a.matchType == cp.OptOutMatchExact) != (b.matchType == cp.OptOutMatchExact) {
			return a.matchType == cp.OptOutMatchExact // exact before prefix
		}
		return len(a.keyword) > len(b.keyword) // longest keyword first
	})
	return &OptOutKeywords{compiled: compiled}, nil
}

// OptOutMatch is the effect of a matched opt-out keyword: the action to apply and the auto-reply
// template to send (nil when the keyword configures none).
type OptOutMatch struct {
	Action   cp.OptOutAction
	Template *string
}

// Match returns the most specific keyword the body triggers in the given country, if any. The body is
// matched in memory (invariant a: never logged). Matching is case-insensitive on the trimmed body.
func (k *OptOutKeywords) Match(country, body string) (OptOutMatch, bool) {
	country = strings.ToUpper(strings.TrimSpace(country))
	text := strings.ToUpper(strings.TrimSpace(body))
	for _, ck := range k.compiled {
		if ck.country != "" && ck.country != country {
			continue
		}
		switch ck.matchType {
		case cp.OptOutMatchExact:
			if text == ck.keyword {
				return OptOutMatch{Action: ck.action, Template: ck.template}, true
			}
		case cp.OptOutMatchPrefix:
			if strings.HasPrefix(text, ck.keyword) {
				return OptOutMatch{Action: ck.action, Template: ck.template}, true
			}
		}
	}
	return OptOutMatch{}, false
}

// StopDeps are the STOP detector's collaborators.
type StopDeps struct {
	Keywords *OptOutKeywords
	Suppress SuppressionWriter
	Producer Producer
	Tracer   trace.Tracer
	Logger   *slog.Logger
}

// StopDetector applies opt-out keywords to a mobile-originated message: a matched STOP writes a
// suppression scoped to the inbound number, START removes it, and a keyword with a configured
// template emits a never-billed auto-reply. It NEVER interrupts delivery — the MO is still routed to
// the account by the caller (§6.20).
type StopDetector struct {
	deps StopDeps
}

// NewStopDetector builds a STOP detector. A nil logger defaults to slog.Default.
func NewStopDetector(deps StopDeps) *StopDetector {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	return &StopDetector{deps: deps}
}

// StopInput is everything the detector needs about one MO, resolved by the caller. Body is the
// already-revealed body (matched in memory, never logged); From/inbound are E.164/normalized.
type StopInput struct {
	MO              pipeline.MOInbound
	InboundNumber   string // normalized inbound number (the MO destination) — the auto-reply's source
	From            string // normalized sender MSISDN — the suppression key and auto-reply destination
	Body            []byte
	InboundNumberID uuid.UUID
	Country         string
	AccountID       uuid.UUID // owning account for CDR attribution (zero for a shared number)
	CustomerID      uuid.UUID
}

// Detect matches in.Body against the opt-out keywords and applies the effect. It returns nil when no
// keyword matches (the common case) or after applying one. An error means a transient failure (the
// suppression write or the auto-reply publish) — the caller reprocesses the MO, and every effect is
// idempotent (suppression write, deterministic auto-reply id).
func (d *StopDetector) Detect(ctx context.Context, in StopInput) error {
	ctx, span := d.deps.Tracer.Start(ctx, "mo.opt_out")
	defer span.End()

	kw, ok := d.deps.Keywords.Match(in.Country, string(in.Body))
	if !ok {
		return nil
	}

	// The suppression key and the auto-reply recipient are both in.From, which must be a canonical
	// MSISDN: the suppressions CHECK rejects a non-canonical value, and we cannot address a reply to a
	// non-number. If the SMSC delivered the source in a non-canonical form (national format,
	// alphanumeric, shortcode) that does not normalize to E.164, skip the opt-out effect — log ids
	// only — rather than let a deterministic CHECK violation wedge the MO. The caller still delivers
	// the MO: a STOP must NEVER interrupt delivery (§6.20).
	if !e164.IsValid(in.From) {
		d.deps.Logger.WarnContext(ctx, "modlrrouter: opt-out keyword from a non-canonical source, skipping effect",
			"inbound_number_id", in.InboundNumberID, "action", string(kw.Action))
		return nil
	}
	span.SetAttributes(attribute.String("opt_out.action", string(kw.Action)))

	switch kw.Action {
	case cp.OptOutActionSuppress:
		created, err := d.deps.Suppress.Create(ctx, cp.NewSuppression{
			Scope:   cp.SuppressionScopeInboundNumber,
			ScopeID: &in.InboundNumberID,
			MSISDN:  in.From,
			Source:  cp.SuppressionSourceMOStop,
		})
		if err != nil {
			return fmt.Errorf("modlrrouter: write STOP suppression: %w", err)
		}
		d.deps.Logger.InfoContext(ctx, "modlrrouter: opt-out STOP applied",
			"inbound_number_id", in.InboundNumberID, "created", created)
	case cp.OptOutActionUnsuppress:
		removed, err := d.deps.Suppress.DeleteByKey(ctx, cp.SuppressionScopeInboundNumber, &in.InboundNumberID, in.From)
		if err != nil {
			return fmt.Errorf("modlrrouter: remove suppression on START: %w", err)
		}
		d.deps.Logger.InfoContext(ctx, "modlrrouter: opt-out START applied",
			"inbound_number_id", in.InboundNumberID, "removed", removed)
	case cp.OptOutActionHelp:
		// A help keyword only replies; it changes no suppression.
	}

	return d.autoReply(ctx, in, kw)
}

// autoReply publishes the keyword's auto-reply, if any, as a never-billed system MT straight to
// mt.routed — bypassing the client compliance pipeline (which would otherwise block a message sent
// from an unregistered inbound number to a just-suppressed recipient). The body is the configured
// template only, never an echo of the sender-controlled MO text.
func (d *StopDetector) autoReply(ctx context.Context, in StopInput, kw OptOutMatch) error {
	if kw.Template == nil || strings.TrimSpace(*kw.Template) == "" {
		return nil
	}
	messageID := uuid.NewSHA1(stopAckNamespace, []byte(moMessageID(in.MO, in.Body).String()))
	reply := pipeline.RoutedMT{
		MessageID:    messageID,
		TraceID:      moTraceID(messageID),
		AccountID:    in.AccountID,
		CustomerID:   in.CustomerID,
		From:         in.InboundNumber,
		To:           in.From,
		Body:         msg.NewBodyString(*kw.Template),
		Encoding:     encoding.Resolve("auto"), // M5 single-segment stub, as the router pipeline does
		ConnectorID:  in.MO.ConnectorID,
		SegmentSeq:   1, // a single-segment reply: one per-segment CDR row (step-082c), never a seq-0 placeholder
		SegmentCount: 1,
		SubmittedAt:  in.MO.ReceivedAt,
		Billable:     false, // a STOP auto-reply is never billed (§6.20)
	}
	rec, err := pipeline.EncodeRouted(reply)
	if err != nil {
		// Encoding our own envelope failing is a deterministic bug, not transient — skip the reply
		// rather than wedge the MO (which is still delivered). Ids only, never the body.
		d.deps.Logger.ErrorContext(ctx, "modlrrouter: encode auto-reply, skipping", "message_id", messageID, "err", err)
		return nil
	}
	if err := d.deps.Producer.Produce(ctx, rec); err != nil {
		return fmt.Errorf("modlrrouter: publish auto-reply %s: %w", messageID, err)
	}
	return nil
}
