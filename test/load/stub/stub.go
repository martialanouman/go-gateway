// Package stub serves a stand-in for the gateway's public REST surface, so a load script can be
// driven and validated without a running gateway (step-200, load harness).
//
// It answers POST /v1/messages the way api/openapi-public.yaml says the real API does — 202 with an
// AcceptedMessage on a valid submission, 401 without a usable bearer, 422 on an invalid or unknown
// field, always with the flat { code, message, errors[] } error model — and does nothing else: no
// queueing, no routing, no billing.
//
// Its reason to exist is Config.Delay. A load run against a local stub passes trivially and therefore
// proves nothing; the same script replayed against a stub slowed past the latency budget must exit
// non-zero. The delay is what gives the harness the ability to fail, so it is configurable from the
// outside (option here, -delay on cmd/load-stub) rather than baked in.
//
// The stub never logs, echoes or otherwise serializes the `text` field of a submission — the repo
// invariant that a message body leaks nowhere holds here too, including on the error paths.
package stub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
	"github.com/martialanouman/go-gateway/internal/platform/uuidx"
)

// APIKeyPrefix is the prefix every public API key carries. The stub authenticates on shape alone —
// it has no account store — so any well-formed bearer is accepted.
const APIKeyPrefix = "sgw_"

// DefaultAddr is the listen address used when Config.Addr is empty.
const DefaultAddr = ":8099"

// Config configures a stub instance. The zero value is a valid, instantaneous stub.
type Config struct {
	// Addr is the TCP address to listen on, e.g. ":8099" or "127.0.0.1:0" for an ephemeral port.
	// Empty means DefaultAddr. Unused by NewHandler.
	Addr string

	// Delay is an artificial latency added before every response, rejections included. Zero (the
	// default) answers as fast as the runtime allows. Raising it past the load script's latency
	// budget is how a run is made to fail on purpose.
	Delay time.Duration
}

// e164 mirrors the `to` pattern of SubmitMessageRequest in api/openapi-public.yaml.
var e164 = regexp.MustCompile(`^\+?[1-9][0-9]{6,14}$`)

// Server is a running stub listening on a TCP port. Create one with Listen and release it with
// Shutdown; the goroutine serving it exits when Shutdown returns.
type Server struct {
	http *http.Server
	lis  net.Listener
	done chan struct{}
}

// Listen binds cfg.Addr and serves the stub in the background. ctx governs the bind only — the
// server's lifetime is Shutdown's business, so a caller may bind under a startup deadline without
// arming a hidden shutdown.
func Listen(ctx context.Context, cfg Config) (*Server, error) {
	addr := cfg.Addr
	if addr == "" {
		addr = DefaultAddr
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("stub: listen on %s: %w", addr, err)
	}

	srv := &Server{
		http: &http.Server{
			Handler:           NewHandler(cfg),
			ReadHeaderTimeout: 5 * time.Second,
		},
		lis:  lis,
		done: make(chan struct{}),
	}
	go func() {
		defer close(srv.done)
		// Serve returns ErrServerClosed on Shutdown; any other error is the listener dying, which
		// the caller observes as failing requests. Nothing here outlives Shutdown.
		_ = srv.http.Serve(lis)
	}()

	return srv, nil
}

// Addr reports the address actually bound, which is how a caller learns the port when it asked for
// an ephemeral one.
func (s *Server) Addr() string {
	return s.lis.Addr().String()
}

// Shutdown stops accepting, drains in-flight requests and waits for the serving goroutine to exit.
// It is safe to call more than once.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.http.Shutdown(ctx)
	select {
	case <-s.done:
	case <-ctx.Done():
		return errors.Join(err, ctx.Err())
	}

	return err
}

// NewHandler returns the stub's HTTP handler, for use with httptest or a server of the caller's own.
// Config.Addr is ignored.
func NewHandler(cfg Config) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		if !sleep(r.Context(), cfg.Delay) {
			return // client gone: there is no one left to answer
		}
		submit(w, r)
	})

	return mux
}

// sleep waits d, reporting false if the client disconnected first. A load client that gives up must
// not pin a goroutine to a timer.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// submitRequest mirrors SubmitMessageRequest. Optional fields are pointers so that "absent" and
// "zero" stay distinguishable, as additionalProperties: false makes every present field meaningful.
type submitRequest struct {
	To                 string  `json:"to"`
	From               string  `json:"from"`
	Text               string  `json:"text"`
	Encoding           *string `json:"encoding,omitempty"`
	RegisteredDelivery *bool   `json:"registered_delivery,omitempty"`
	ValidityPeriod     *string `json:"validity_period,omitempty"`
	Priority           *int    `json:"priority,omitempty"`
	ClientRef          *string `json:"client_ref,omitempty"`
	DataCoding         *int    `json:"data_coding,omitempty"`
}

// acceptedMessage mirrors the AcceptedMessage schema.
type acceptedMessage struct {
	ID         string  `json:"id"`
	TraceID    string  `json:"trace_id"`
	Status     string  `json:"status"`
	ClientRef  *string `json:"client_ref"`
	AcceptedAt string  `json:"accepted_at"`
}

// fieldError is one entry of the flat error model's errors[].
type fieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// errorBody is the repo's flat error model: { code, message, errors[] }.
type errorBody struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Errors  []fieldError `json:"errors,omitempty"`
}

func submit(w http.ResponseWriter, r *http.Request) {
	if !authenticated(r) {
		writeJSON(w, http.StatusUnauthorized, errorBody{
			Code:    string(errs.ErrUnauthenticated),
			Message: "missing or malformed bearer API key",
		})

		return
	}

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req submitRequest
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{
			Code:    string(errs.ErrValidation),
			Message: "request body is not a valid submission",
			Errors:  []fieldError{decodeError(err)},
		})

		return
	}

	if problems := validate(req); len(problems) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity, errorBody{
			Code:    string(errs.ErrValidation),
			Message: "request body failed validation",
			Errors:  problems,
		})

		return
	}

	writeJSON(w, http.StatusAccepted, acceptedMessage{
		ID:         uuidx.New().String(),
		TraceID:    uuidx.New().String(),
		Status:     "accepted",
		ClientRef:  req.ClientRef,
		AcceptedAt: time.Now().UTC().Format(time.RFC3339),
	})
}

// authenticated reports whether the request carries a bearer that looks like a public API key. The
// stub has no account store, so shape is all it can check — and all a load client needs.
func authenticated(r *http.Request) bool {
	const scheme = "Bearer "

	h := r.Header.Get("Authorization")
	if len(h) <= len(scheme) || !strings.EqualFold(h[:len(scheme)], scheme) {
		return false
	}

	return strings.HasPrefix(strings.TrimSpace(h[len(scheme):]), APIKeyPrefix)
}

// decodeError turns a decode failure into a field problem. Only the unknown-field case names a
// field, and only from the decoder's own message, which quotes the key and never a value — no part
// of the submitted payload is ever reflected back.
func decodeError(err error) fieldError {
	const unknown = "json: unknown field "
	if msg := err.Error(); strings.HasPrefix(msg, unknown) {
		return fieldError{
			Field:   strings.Trim(strings.TrimPrefix(msg, unknown), `"`),
			Message: "unknown field",
		}
	}

	return fieldError{Field: "body", Message: "malformed JSON body"}
}

// validate applies the constraints SubmitMessageRequest declares, reporting every problem at once so
// a load script's failure output names all of them.
func validate(req submitRequest) []fieldError {
	var problems []fieldError

	switch {
	case req.To == "":
		problems = append(problems, fieldError{Field: "to", Message: "required"})
	case !e164.MatchString(req.To):
		problems = append(problems, fieldError{Field: "to", Message: "must be an E.164 MSISDN"})
	}

	switch {
	case req.From == "":
		problems = append(problems, fieldError{Field: "from", Message: "required"})
	case utf8.RuneCountInString(req.From) > 20:
		problems = append(problems, fieldError{Field: "from", Message: "must be at most 20 characters"})
	}

	// Length only — the text itself is never quoted back, here or anywhere else.
	switch n := utf8.RuneCountInString(req.Text); {
	case n == 0:
		problems = append(problems, fieldError{Field: "text", Message: "required"})
	case n > 2000:
		problems = append(problems, fieldError{Field: "text", Message: "must be at most 2000 characters"})
	}

	if req.Encoding != nil {
		switch *req.Encoding {
		case "auto", "gsm7", "ucs2", "binary":
		default:
			problems = append(problems, fieldError{Field: "encoding", Message: "must be one of auto, gsm7, ucs2, binary"})
		}
	}
	if req.Priority != nil && (*req.Priority < 0 || *req.Priority > 3) {
		problems = append(problems, fieldError{Field: "priority", Message: "must be between 0 and 3"})
	}
	if req.ValidityPeriod != nil && utf8.RuneCountInString(*req.ValidityPeriod) > 16 {
		problems = append(problems, fieldError{Field: "validity_period", Message: "must be at most 16 characters"})
	}
	if req.ClientRef != nil && utf8.RuneCountInString(*req.ClientRef) > 128 {
		problems = append(problems, fieldError{Field: "client_ref", Message: "must be at most 128 characters"})
	}
	if req.DataCoding != nil && (*req.DataCoding < 0 || *req.DataCoding > 255) {
		problems = append(problems, fieldError{Field: "data_coding", Message: "must be between 0 and 255"})
	}

	return problems
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The client is a load generator: a write failure means it hung up mid-response, which is its
	// own business and nothing this stub can report to anyone.
	_ = json.NewEncoder(w).Encode(body)
}
