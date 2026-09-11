package restapi_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	"github.com/martialanouman/go-gateway/internal/credential"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
)

// TestAPIKeyAuthFailsClosedWhenPostgresIsCut is the step-260c acceptance test for the REST-auth row of
// the failure-policy matrix (guide de codage §16): a PostgreSQL outage answers 500 internal_error, and
// there is no principal cache — every authenticated request reads the control plane.
//
// The branch it covers (auth.go:39-41) had NO test at all, not even a unit one: no fakePrincipals in
// this package ever fills err, and the 500s that are tested belong to the CDR and account readers.
// That is the gap §16's [MUST] exists to close.
//
// The load-bearing half is "not 401". An invalid key and an unreachable database are one line apart in
// the middleware and worlds apart in what they tell the caller: a 401 says the key itself is wrong,
// which sends an integrator rotating credentials in the middle of an outage that has nothing to do
// with them. The control below establishes what a genuine 401 looks like, so the outage assertion can
// tell the two apart rather than merely asserting a number.
func TestAPIKeyAuthFailsClosedWhenPostgresIsCut(t *testing.T) {
	seed := pgtest.Pool(t) // uncut: seeds the key, and stays alive to prove the cut one was the only fault
	key := seedAPIKey(t, seed)

	// Built while the link is up — postgres.NewPool pings eagerly, so the cut comes after.
	cutPool, proxy := pgtest.Cuttable(t)
	h := newHarness(t, postgres.NewAPIKeyRepo(cutPool), &fakeCDRReader{})

	// Control A, link up: the seeded key authenticates. Without it, a 500 under the cut could equally
	// be a harness that never wired a working store.
	if got := statusOf(t, h, key); got != http.StatusOK {
		t.Fatalf("with postgres up the seeded key = %d, want 200 — the control failed", got)
	}

	// Control B, link up: an unknown key is 401. This is the camp the outage must not land in.
	if got := statusOf(t, h, "sgw_definitely_not_a_real_key"); got != http.StatusUnauthorized {
		t.Fatalf("with postgres up an unknown key = %d, want 401 — without this control the outage "+
			"assertion below cannot tell an invalid key from an unreachable database", got)
	}

	proxy.Cut()

	// The outage, on a key that is valid and active: anything but a fault here is wrong.
	status, code := statusAndCode(t, h, key)
	if status == http.StatusOK {
		t.Fatal("with postgres cut a request was AUTHENTICATED: the middleware holds no principal " +
			"cache and must not acquire one — serving a caller whose channel state and account status " +
			"could not be read serves a suspended account as readily as a live one")
	}
	if status == http.StatusUnauthorized {
		t.Fatal("with postgres cut the request got 401: that tells the caller its API key is invalid " +
			"and sends it rotating credentials during an outage it did not cause — the fault is ours " +
			"and must read as one")
	}
	if status != http.StatusInternalServerError || code != "internal_error" {
		t.Errorf("outage response = %d %q, want 500 internal_error", status, code)
	}

	// The same outage, on a key that does not exist. With the link up this is 401; under the cut it
	// must be 500, because the middleware cannot know the key is unknown — it never reached the table.
	if got := statusOf(t, h, "sgw_definitely_not_a_real_key"); got != http.StatusInternalServerError {
		t.Errorf("outage response for an unknown key = %d, want 500: answering 401 would assert the "+
			"key is invalid, which a middleware that could not read the table cannot know", got)
	}

	proxy.Resume()

	// No latch. The retry loop is infrastructure recovery, not the behaviour under test: pgx only
	// discovers the connections the cut killed by failing on one, so the first request back can still
	// answer 500 off a corpse.
	deadline := time.Now().Add(15 * time.Second)
	for {
		got := statusOf(t, h, key)
		if got == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after postgres came back the seeded key = %d, want 200: the middleware latched "+
				"on the outage", got)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// seedAPIKey creates a customer, an account and an active API-key credential through the UNCUT pool,
// returning the clear-text key an ESME presents. REST is enabled by default on a fresh account.
func seedAPIKey(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()

	customer, err := postgres.NewCustomerRepo(pool).Create(ctx, cp.NewCustomer{
		Name: "ChaosCo-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("create customer: %v", err)
	}
	account, err := postgres.NewAccountRepo(pool).Create(ctx, cp.NewAccount{
		CustomerID: customer.ID, Name: "rest-" + uuid.NewString(),
	})
	if err != nil {
		t.Fatalf("create account: %v", err)
	}
	key, hash, err := credential.GenerateAPIKey()
	if err != nil {
		t.Fatalf("generate api key: %v", err)
	}
	if _, err := postgres.NewCredentialRepo(pool).Create(ctx, cp.NewCredential{
		AccountID:  account.ID,
		Type:       cp.CredentialAPIKey,
		APIKeyHash: &hash,
	}); err != nil {
		t.Fatalf("create credential: %v", err)
	}
	return key
}

// statusOf presents key on an authenticated operation and reports the status. list-messages is the
// cheapest one that requires the scheme: its reader is a fake, so the cut pool is reached by the
// middleware and by nothing else.
func statusOf(t *testing.T, h *harness, key string) int {
	t.Helper()
	status, _ := statusAndCode(t, h, key)
	return status
}

// statusAndCode is statusOf plus the flat error model's code, which is what distinguishes an outage
// (internal_error) from every other 500 the surface can produce.
func statusAndCode(t *testing.T, h *harness, key string) (int, string) {
	t.Helper()
	resp := h.do(t, http.MethodGet, "/v1/messages", key, nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusOK {
		return resp.StatusCode, ""
	}
	var body struct {
		Code string `json:"code"`
	}
	decode(t, resp, &body)
	return resp.StatusCode, body.Code
}
