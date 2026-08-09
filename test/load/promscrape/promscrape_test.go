package promscrape_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/martialanouman/go-gateway/test/load/promscrape"
)

func cfg(endpoint, defaultPath string) promscrape.Config {
	return promscrape.Config{Namespace: "peermetrics", Endpoint: endpoint, DefaultPath: defaultPath}
}

// TestNewUsesTheDefaultPathOnlyForABareOrigin is the reason this package exists rather than a third
// copy of the same client.
//
// Redpanda serves TWO expositions on its admin port: /metrics carries the internal vectorized_* series
// and /public_metrics the curated redpanda_* ones. The two existing readers hard-code "/metrics", so a
// copy aimed at Redpanda would get a 200 full of series it does not know — the worst failure available
// to a scraper, because it looks like a healthy broker at rest.
func TestNewUsesTheDefaultPathOnlyForABareOrigin(t *testing.T) {
	c, err := promscrape.New(cfg("http://broker:9644", "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if got := c.URL(); got != "http://broker:9644/public_metrics" {
		t.Errorf("a bare origin must take the default path, got %s", got)
	}

	// An explicit path is the caller's decision and must survive untouched.
	c, err = promscrape.New(cfg("http://broker:9644/metrics", "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if got := c.URL(); got != "http://broker:9644/metrics" {
		t.Errorf("an explicit path must not be replaced by the default, got %s", got)
	}

	// A trailing slash is a bare origin spelled differently.
	c, err = promscrape.New(cfg("http://broker:9644/", "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if got := c.URL(); got != "http://broker:9644/public_metrics" {
		t.Errorf("a trailing slash is still a bare origin, got %s", got)
	}
}

// TestNewRefusesAnEndpointItCannotScrape: every rejection names the namespace, so a failure in a run
// with three readers says which one failed.
func TestNewRefusesAnEndpointItCannotScrape(t *testing.T) {
	for _, tc := range []struct{ name, endpoint string }{
		{"no scheme", "broker:9644"},
		{"wrong scheme", "ftp://broker:9644"},
		{"no host", "http:///metrics"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := promscrape.New(cfg(tc.endpoint, "/public_metrics"))
			if err == nil {
				t.Fatalf("%q must be refused", tc.endpoint)
			}
			if !strings.Contains(err.Error(), "peermetrics") {
				t.Errorf("the error must name the reader that produced it, got: %v", err)
			}
		})
	}

	if _, err := promscrape.New(promscrape.Config{Namespace: "peermetrics", Endpoint: "http://b:1"}); err == nil {
		t.Error("a missing default path must be refused: a bare origin would be scraped at /")
	}
}

// TestClientNeverEchoesCredentials pins the one property that is a leak rather than a bug.
//
// url.Error prints the raw URL it failed on, credentials and all, so an endpoint that fails to parse
// cannot be quoted back — it is precisely the string that could not be redacted.
func TestClientNeverEchoesCredentials(t *testing.T) {
	c, err := promscrape.New(cfg("http://scrape:hunter2@broker:9644", "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if strings.Contains(c.URL(), "hunter2") {
		t.Errorf("URL must mask the password, got %s", c.URL())
	}

	// The same holds for every error produced later by the client, not just at construction.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	withCreds := strings.Replace(srv.URL, "http://", "http://scrape:hunter2@", 1)
	c, err = promscrape.New(cfg(withCreds, "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	err = c.Scrape(context.Background(), func(io.Reader, time.Time) error { return nil })
	if err == nil {
		t.Fatal("a 500 must fail")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("a scrape error must not echo the password: %v", err)
	}
}

// TestScrapeStampsTheReadingBeforeTheRequest keeps every derived rate an understatement.
//
// The peer gathers its counters somewhere between the request leaving and the body being read. An
// early stamp can only place At before the counters it labels, so a pair of readings spans a window no
// shorter than the real one — and a rate divided by it is understated, never overstated. That is the
// direction a ceiling has to err in.
func TestScrapeStampsTheReadingBeforeTheRequest(t *testing.T) {
	const delay = 80 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(delay)
		_, _ = fmt.Fprintln(w, "peer_up 1")
	}))
	defer srv.Close()

	c, err := promscrape.New(cfg(srv.URL, "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	before := time.Now()
	var stamped time.Time
	if err := c.Scrape(context.Background(), func(_ io.Reader, at time.Time) error {
		stamped = at
		return nil
	}); err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if stamped.Sub(before) >= delay {
		t.Errorf("the reading was stamped after the response arrived (%v into a %v request): "+
			"a late stamp overstates every rate derived from it", stamped.Sub(before), delay)
	}
}

// TestScrapeRefusesANonOKStatus: a reading that cannot be trusted must not read as a passing one.
func TestScrapeRefusesANonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintln(w, "no such endpoint")
	}))
	defer srv.Close()

	c, err := promscrape.New(cfg(srv.URL, "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	called := false
	err = c.Scrape(context.Background(), func(io.Reader, time.Time) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("a 404 must fail")
	}
	if called {
		t.Error("the body of a non-200 must never reach the parser")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("the error must carry the status, got: %v", err)
	}
}

// TestScrapeHonoursContextCancellation: the context governs the whole exchange, connection included.
func TestScrapeHonoursContextCancellation(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = fmt.Fprintln(w, "peer_up 1")
	}))
	defer srv.Close()
	defer close(release)

	c, err := promscrape.New(cfg(srv.URL, "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Scrape(ctx, func(io.Reader, time.Time) error { return nil }); err == nil {
		t.Fatal("a scrape outliving its context must fail")
	}
}

// TestScrapeSurfacesTheReadersError: the client owns the response body, so a parser that fails must
// still leave the connection reclaimed — and its error must reach the caller unchanged rather than
// being reported as a transport failure.
func TestScrapeSurfacesTheReadersError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "peer_up 1")
	}))
	defer srv.Close()

	c, err := promscrape.New(cfg(srv.URL, "/public_metrics"))
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	sentinel := errors.New("parse failed")
	err = c.Scrape(context.Background(), func(io.Reader, time.Time) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Errorf("the reader's error must reach the caller intact, got: %v", err)
	}
}
