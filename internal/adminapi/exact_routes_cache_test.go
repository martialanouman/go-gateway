package adminapi_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/adminapi"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
)

// opLog records durable writes and cache invalidations on one timeline, which is what makes the
// ordering assertions below possible: "commit first, invalidate after" is the whole safety argument of
// step-250e, and two separate counters could not tell the two orders apart.
type opLog struct {
	mu  sync.Mutex
	ops []string
}

func (l *opLog) add(op string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ops = append(l.ops, op)
}

func (l *opLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ops...)
}

// loggingStore decorates the in-memory store so every durable write lands on the shared timeline.
type loggingStore struct {
	adminapi.ExactRouteAdminStore
	log *opLog
}

func (s loggingStore) Upsert(ctx context.Context, r exact.Route) (exact.Route, error) {
	s.log.add("upsert:" + r.MSISDN)
	return s.ExactRouteAdminStore.Upsert(ctx, r)
}

func (s loggingStore) Delete(ctx context.Context, msisdn string) (bool, error) {
	s.log.add("delete:" + msisdn)
	return s.ExactRouteAdminStore.Delete(ctx, msisdn)
}

func (s loggingStore) BulkUpsert(ctx context.Context, routes []exact.Route) error {
	s.log.add(fmt.Sprintf("bulk:%d", len(routes)))
	return s.ExactRouteAdminStore.BulkUpsert(ctx, routes)
}

// fakeRouteCache records what the handlers ask to forget, on the same timeline.
type fakeRouteCache struct {
	log *opLog
	err error
}

func (c *fakeRouteCache) Invalidate(_ context.Context, msisdns ...string) error {
	c.log.add("invalidate:" + strings.Join(msisdns, ","))
	return c.err
}

// cacheTestAPI wires the exact-route handlers over a logged store and a recording cache.
func cacheTestAPI(t *testing.T, cacheErr error) (http.Handler, *opLog) {
	t.Helper()
	log := &opLog{}
	store := loggingStore{ExactRouteAdminStore: newFakeExactRouteStore(), log: log}
	api := newTestAPIWith(t, adminapi.Deps{
		ExactRoutes:     store,
		ExactRouteCache: &fakeRouteCache{log: log, err: cacheErr},
		Imports:         syncRunner{},
	})
	return api, log
}

func createRoute(t *testing.T, api http.Handler, msisdn string) {
	t.Helper()
	w := httptest.NewRecorder()
	body := fmt.Sprintf(`{"msisdn":%q,"target_type":"connector","target_id":%q}`, msisdn, uuid.NewString())
	api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes", body))
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", w.Code, w.Body)
	}
}

// TestExactRouteMutationsInvalidateAfterTheCommit is the ordering rule of step-250e, one case per
// mutation: the durable write lands FIRST, the cache is told to forget SECOND.
//
// The reverse order is not merely untidy — it is unsafe. A concurrent reader that invalidates before
// the commit can repopulate the cache from the pre-commit row, and since nothing else ever writes that
// key, the stale value would then be pinned for a full TTL.
func TestExactRouteMutationsInvalidateAfterTheCommit(t *testing.T) {
	const msisdn = "2250700000001"

	t.Run("create", func(t *testing.T) {
		api, log := cacheTestAPI(t, nil)
		createRoute(t, api, msisdn)
		wantOps(t, log, []string{"upsert:" + msisdn, "invalidate:" + msisdn})
	})

	t.Run("update", func(t *testing.T) {
		api, log := cacheTestAPI(t, nil)
		createRoute(t, api, msisdn)

		w := httptest.NewRecorder()
		body := fmt.Sprintf(`{"target_type":"connector","target_id":%q}`, uuid.NewString())
		api.ServeHTTP(w, authed(t, http.MethodPatch, "/v1/admin/exact-routes/"+msisdn, body))
		if w.Code != http.StatusOK {
			t.Fatalf("update status = %d, want 200; body=%s", w.Code, w.Body)
		}
		wantOps(t, log, []string{
			"upsert:" + msisdn, "invalidate:" + msisdn, // the create
			"upsert:" + msisdn, "invalidate:" + msisdn, // the update
		})
	})

	t.Run("delete", func(t *testing.T) {
		api, log := cacheTestAPI(t, nil)
		createRoute(t, api, msisdn)

		w := httptest.NewRecorder()
		api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/exact-routes/"+msisdn, ""))
		if w.Code != http.StatusNoContent {
			t.Fatalf("delete status = %d, want 204; body=%s", w.Code, w.Body)
		}
		wantOps(t, log, []string{
			"upsert:" + msisdn, "invalidate:" + msisdn,
			"delete:" + msisdn, "invalidate:" + msisdn,
		})
	})

	t.Run("import", func(t *testing.T) {
		api, log := cacheTestAPI(t, nil)
		w := httptest.NewRecorder()
		body := fmt.Sprintf(`{"source":"mnp_import","rows":[
			{"msisdn":"2250700000001","target_type":"connector","target_id":%q},
			{"msisdn":"2250700000002","target_type":"connector","target_id":%q}]}`,
			uuid.NewString(), uuid.NewString())
		api.ServeHTTP(w, authed(t, http.MethodPost, "/v1/admin/exact-routes/import", body))
		if w.Code != http.StatusAccepted {
			t.Fatalf("import status = %d, want 202; body=%s", w.Code, w.Body)
		}
		// Every imported number must be forgotten, or a re-ported number keeps its old carrier until
		// the TTL — the exact failure this step exists to remove.
		wantOps(t, log, []string{"bulk:2", "invalidate:2250700000001,2250700000002"})
	})
}

// TestExactRouteInvalidationFailureDoesNotFailTheRequest: the durable commit already happened, so a
// Redis blip must not turn a successful admin mutation into an error the operator will retry against a
// row that is already written. The TTL bounds the staleness, and both the upsert and the DEL are
// idempotent — the same trade-off BalanceCacheInvalidator makes (step-148).
func TestExactRouteInvalidationFailureDoesNotFailTheRequest(t *testing.T) {
	api, log := cacheTestAPI(t, errors.New("redis down"))
	createRoute(t, api, "2250700000001") // asserts 201 internally

	if got := log.snapshot(); len(got) != 2 {
		t.Errorf("ops = %v, want the commit followed by an attempted invalidation", got)
	}
}

// TestExactRouteDeleteOfUnknownNumberInvalidatesNothing: a 404 changed nothing durably, so there is
// nothing to forget. Invalidating anyway would be harmless but dishonest — it would suggest a write
// happened.
func TestExactRouteDeleteOfUnknownNumberInvalidatesNothing(t *testing.T) {
	api, log := cacheTestAPI(t, nil)

	w := httptest.NewRecorder()
	api.ServeHTTP(w, authed(t, http.MethodDelete, "/v1/admin/exact-routes/2250799999999", ""))
	if w.Code != http.StatusNotFound {
		t.Fatalf("delete unknown = %d, want 404", w.Code)
	}
	for _, op := range log.snapshot() {
		if strings.HasPrefix(op, "invalidate:") {
			t.Errorf("a 404 delete invalidated the cache (%q); nothing was committed", op)
		}
	}
}

func wantOps(t *testing.T, log *opLog, want []string) {
	t.Helper()
	got := log.snapshot()
	if len(got) != len(want) {
		t.Fatalf("ops = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ops = %v, want %v (differs at %d)", got, want, i)
		}
	}
}
