// Package promscrape is the HTTP half of a Prometheus exposition reader: it turns a peer's endpoint
// into a timestamped body, and nothing more.
//
// It exists because the load harness now reads three peers — the fake SMSC, the gateway, and the
// Redpanda broker (step-201e D2) — and the first two were written as deliberate twins:
//
//	"It mirrors smscmetrics.Client deliberately rather than sharing it: the two read different peers'
//	 metric contracts, and the shape (origin or full URL, credentials never echoed, early timestamp) is
//	 the part worth having identical. Extracting a common scraper is worth doing the day a third one
//	 appears."
//
// That day is this one. What the twins had in common — URL handling, credential redaction, the early
// stamp, the status guard — lives here. What they did NOT have in common stays where it was: parsing
// is a peer's metric contract, so every reader keeps its own Parse, its own metric names and its own
// derived types.
//
// The one thing this package adds rather than merely moves is DefaultPath. The two existing readers
// hard-code "/metrics", and Redpanda serves its curated redpanda_* series at /public_metrics while
// /metrics carries thousands of internal vectorized_* ones. A reader pointed at the wrong one gets a
// 200 full of series it does not recognise, which reads exactly like a healthy peer at rest — so the
// path is a parameter, and every caller states it.
package promscrape

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Config names a peer's exposition. It is a struct rather than three string parameters because three
// adjacent strings can be swapped at a call site without the compiler noticing.
type Config struct {
	// Namespace prefixes every error this client produces, so a run with several readers says which
	// one failed. Conventionally the reader's package name.
	Namespace string

	// Endpoint is the peer's origin ("http://broker:9644") or a full URL. It may carry credentials;
	// they are used for the request and never repeated back.
	Endpoint string

	// DefaultPath is the path to scrape when Endpoint carries none. Required: defaulting it silently
	// is how a reader ends up pointed at the wrong one of a peer's two expositions.
	DefaultPath string
}

// Client scrapes one peer's exposition. It is safe for concurrent use.
type Client struct {
	// url is the URL actually requested, userinfo included. It never leaves the client: everything
	// logged or wrapped in an error uses redacted instead.
	url       string
	redacted  string
	namespace string
	http      *http.Client
}

// Option adjusts a Client at construction.
type Option func(*Client)

// WithHTTPClient overrides the HTTP client, for tests and for callers that need their own transport.
func WithHTTPClient(c *http.Client) Option {
	return func(cl *Client) { cl.http = c }
}

// New validates cfg and builds a client.
//
// An endpoint may carry credentials ("https://scrape:secret@broker:9644"). They are used for the
// request and never repeated back: no error returned here, nor any produced later by this client,
// echoes the raw endpoint.
func New(cfg Config, opts ...Option) (*Client, error) {
	ns := cfg.Namespace
	if ns == "" {
		ns = "promscrape"
	}
	if cfg.DefaultPath == "" {
		return nil, fmt.Errorf("%s: a default path is required: a bare origin would be scraped at /", ns)
	}

	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		// url.Error prints the raw URL it failed on, credentials included, so only the reason is
		// reported. The endpoint itself cannot be shown: it is precisely the string that could not be
		// parsed, hence could not be redacted either.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		return nil, fmt.Errorf("%s: parse endpoint: %w", ns, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("%s: endpoint %q needs an http or https scheme", ns, u.Redacted())
	}
	if u.Host == "" {
		return nil, fmt.Errorf("%s: endpoint %q has no host", ns, u.Redacted())
	}
	if p := strings.TrimSuffix(u.Path, "/"); p == "" {
		u.Path = cfg.DefaultPath
	}

	c := &Client{url: u.String(), redacted: u.Redacted(), namespace: ns, http: &http.Client{}}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// URL reports the URL the client scrapes, after any default path was appended, with any password
// masked. It is a diagnostic accessor — callers log it — so it returns the redacted form rather than
// url.URL.String, which does not mask userinfo.
func (c *Client) URL() string { return c.redacted }

// Scrape takes one reading and hands it to read, along with the instant just before the request went
// out — not the instant the body finished arriving.
//
// The stamp is deliberately early, and the asymmetry matters. The peer gathers its counters somewhere
// between the request leaving and the body being read, so an early stamp can only put the instant
// BEFORE the counters it labels. Applied to both readings of a pair, that makes the measured window no
// shorter than the real one, so a derived rate is understated rather than overstated. Stamping late
// does the opposite whenever the first scrape is the slower of the two — the usual case, with a TCP
// connection to open and a cold registry to encode against a keep-alive second scrape — and these
// numbers are ceilings a run has to stay under.
//
// read is called at most once, and only on a 200. The body is closed however read returns, and read's
// error reaches the caller unwrapped by transport concerns: a parser that failed did not fail to
// scrape.
//
// The context governs the whole exchange, connection included.
func (c *Client) Scrape(ctx context.Context, read func(body io.Reader, at time.Time) error) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return fmt.Errorf("%s: build request: %w", c.namespace, err)
	}
	at := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s: scrape %s: %w", c.namespace, c.redacted, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: scrape %s: status %d, want 200", c.namespace, c.redacted, resp.StatusCode)
	}
	return read(resp.Body, at)
}
