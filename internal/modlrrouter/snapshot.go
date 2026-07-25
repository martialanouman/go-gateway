package modlrrouter

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/google/uuid"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/platform/e164"
)

// The snapshot sources, declared consumer-side. The Postgres repos satisfy them structurally:
// *InboundNumberRepo.List, *InboundKeywordRepo.ListAll, *AccountRepo.ListAccountCustomers.
type (
	// NumberLister lists every inbound number.
	NumberLister interface {
		List(ctx context.Context) ([]cp.InboundNumber, error)
	}
	// KeywordLister lists every active keyword across shared numbers.
	KeywordLister interface {
		ListAll(ctx context.Context) ([]cp.InboundKeyword, error)
	}
	// CustomerLister returns the account -> customer map.
	CustomerLister interface {
		ListAccountCustomers(ctx context.Context) (map[uuid.UUID]uuid.UUID, error)
	}
)

// Snapshot is the immutable in-memory routing table the MO router resolves against: inbound numbers
// indexed by their normalised address, each shared number's keywords in priority order (regexes
// pre-compiled), and an account -> customer map. It is built once at boot (cold load) and read
// lock-free by the hot path; a hot reload is a later milestone.
type Snapshot struct {
	numbers  map[string]cp.InboundNumber
	keywords map[uuid.UUID][]compiledKeyword
	customer map[uuid.UUID]uuid.UUID
}

// compiledKeyword is one keyword ready to match: the upper-cased needle for exact/prefix, or a
// compiled regexp. accountID is the account the match routes to.
type compiledKeyword struct {
	matchType cp.MatchType
	keyword   string
	accountID uuid.UUID
	re        *regexp.Regexp
}

// LoadSnapshot reads the routing config once and compiles it. It is the cold load the service runs at
// boot; a read failure aborts startup.
func LoadSnapshot(ctx context.Context, logger *slog.Logger, numbers NumberLister, keywords KeywordLister, customers CustomerLister) (*Snapshot, error) {
	nums, err := numbers.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("modlrrouter: load inbound numbers: %w", err)
	}
	kws, err := keywords.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("modlrrouter: load inbound keywords: %w", err)
	}
	custs, err := customers.ListAccountCustomers(ctx)
	if err != nil {
		return nil, fmt.Errorf("modlrrouter: load account customers: %w", err)
	}
	return compileSnapshot(logger, nums, kws, custs), nil
}

// compileSnapshot builds the routing table. An inbound number is indexed by its normalised address so
// a long code stored "+225…" matches an MO addressed "225…"; a short code (not E.164) is indexed
// as-is. A keyword with an invalid regexp is dropped with a warning rather than failing the load.
func compileSnapshot(logger *slog.Logger, numbers []cp.InboundNumber, keywords []cp.InboundKeyword, customers map[uuid.UUID]uuid.UUID) *Snapshot {
	s := &Snapshot{
		numbers:  make(map[string]cp.InboundNumber, len(numbers)),
		keywords: make(map[uuid.UUID][]compiledKeyword),
		customer: customers,
	}
	for _, n := range numbers {
		s.numbers[normalizeAddr(n.Address)] = n
	}
	for _, kw := range keywords {
		ck := compiledKeyword{
			matchType: kw.MatchType,
			keyword:   strings.ToUpper(strings.TrimSpace(kw.Keyword)),
			accountID: kw.AccountID,
		}
		if kw.MatchType == cp.MatchRegex {
			re, err := regexp.Compile(kw.Keyword)
			if err != nil {
				logger.Warn("modlrrouter: dropping keyword with invalid regexp", "keyword_id", kw.ID, "err", err)
				continue
			}
			ck.re = re
		}
		s.keywords[kw.InboundNumberID] = append(s.keywords[kw.InboundNumberID], ck)
	}
	return s
}

// resolution is the outcome of routing one MO: routed (with the resolved account/customer/number) or
// not (with the reason to file it under).
type resolution struct {
	routed          bool
	accountID       uuid.UUID
	customerID      uuid.UUID
	inboundNumberID *uuid.UUID
	reason          cp.UnroutedReason
}

// resolve routes an MO addressed to dest, matching keywords against body IN MEMORY (invariant a: the
// caller reveals the body once and never logs it). A dedicated number routes to its account; a shared
// number routes to the first keyword that matches by priority; an unknown or disabled number, or a
// shared number no keyword matched, is unrouted.
func (s *Snapshot) resolve(dest string, body []byte) resolution {
	num, ok := s.numbers[normalizeAddr(dest)]
	if !ok {
		return resolution{reason: cp.UnroutedUnknownNumber}
	}
	id := num.ID
	if num.Status != cp.InboundNumberActive {
		return resolution{reason: cp.UnroutedNumberDisabled, inboundNumberID: &id}
	}
	if num.AccountID != nil {
		return s.route(*num.AccountID, id)
	}
	text := string(body)
	for _, ck := range s.keywords[num.ID] {
		if ck.matches(text) {
			return s.route(ck.accountID, id)
		}
	}
	return resolution{reason: cp.UnroutedNoKeywordMatch, inboundNumberID: &id}
}

// route builds a routed resolution, resolving the account's customer from the snapshot.
func (s *Snapshot) route(accountID, inboundNumberID uuid.UUID) resolution {
	return resolution{
		routed:          true,
		accountID:       accountID,
		customerID:      s.customer[accountID],
		inboundNumberID: &inboundNumberID,
	}
}

// matches reports whether text satisfies the keyword. Exact and prefix are case-insensitive on the
// trimmed text (SMS keywords like STOP are conventionally case-insensitive); a regexp controls its own
// case via its pattern.
func (ck compiledKeyword) matches(text string) bool {
	switch ck.matchType {
	case cp.MatchExact:
		return strings.EqualFold(strings.TrimSpace(text), ck.keyword)
	case cp.MatchPrefix:
		return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(text)), ck.keyword)
	case cp.MatchRegex:
		return ck.re != nil && ck.re.MatchString(text)
	default:
		return false
	}
}

// normalizeAddr renders an address in the form the snapshot is keyed by: the E.164 form for a real
// number, or the trimmed raw value for a short code / alphanumeric sender that is not E.164.
func normalizeAddr(a string) string {
	if n, err := e164.Normalize(a); err == nil {
		return n
	}
	return strings.TrimSpace(a)
}
