//go:build loadref

package e2e_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/martialanouman/go-gateway/internal/config"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/routing"
	"github.com/martialanouman/go-gateway/internal/routing/exact"
	"github.com/martialanouman/go-gateway/internal/storage/postgres"
	"github.com/martialanouman/go-gateway/internal/testutil/kafkatest"
	"github.com/martialanouman/go-gateway/internal/testutil/pgtest"
	"github.com/martialanouman/go-gateway/internal/testutil/redistest"
)

const (
	// envPortedShare is the fraction of prefilled records addressed to a ported number. Its default is
	// the TOP of the 10-30% band step-280 calls representative of a mature MNP market, so the reading is
	// the pessimistic end of the dimensioning rather than the comfortable one.
	envPortedShare = "REF_PORTED_SHARE"

	// envPortedPool is how many DISTINCT ported numbers those records touch. It is the locality dial
	// this bench substitutes for a TTL — see l0Dest — and therefore the single lever that moves the
	// reading between the Redis arm and the Postgres arm.
	envPortedPool = "REF_PORTED_POOL"

	// envPgMaxConns sizes the pool the window measures. Its default is READ from config.Defaults()
	// rather than written here, so the bench follows production's POSTGRES_MAX_CONNS instead of pinning
	// a copy that would drift: the whole question is that default (10) against the 12 lanes a router pod
	// can own.
	envPgMaxConns = "REF_PG_MAX_CONNS"
)

const (
	// l0Lanes is fixed, not swept, on the pattern of poolFidelityBinds: this bench measures where
	// step-270 operates. 12 is KAFKA_TOPIC_PARTITIONS as deployed, and it is the number step-280 sets
	// against MaxConns=10 — sweeping it would answer a question nobody has yet asked.
	l0Lanes = 12

	// l0SeedChunk is how many exact routes go in one BatchUpsert. The repo batches internally; this only
	// bounds the slice built in memory.
	l0SeedChunk = 1000
)

// TestRouterL0Fidelity prices the L0 stage — the one stage of the MT pipeline no bench has ever run.
//
// # What it measures, and what it deliberately does not
//
// Every absolute number here belongs to this laptop. What transposes is the RATIOS: lookups per message,
// the outcome mix, bytes per cached key, acquisitions per message against MaxConns. step-280 carries
// those to the representative environment and multiplies; it cannot carry a throughput.
//
// # Why the "without" side is the declarative resolver, not the sweep's stub
//
// TestRouterConsumeCeiling runs ceilResolver, which returns a connector id and touches nothing. Pairing
// L0 against THAT would price the L0 stage plus the whole declarative resolution, and file the sum under
// L0. Both sides here share one real routing.SnapshotResolver, so the delta is what wiring L0 in front
// of it costs — and the rows of this bench are therefore NOT comparable to the sweep's.
//
// # What the delta contains at share > 0
//
// It is a NET figure, and that is the production-relevant one: L0 adds a Bloom probe to every message
// and a store lookup to the ported ones, and it SKIPS declarative resolution for the ported ones that
// hit (a connector target resolves without consulting the snapshot at all, snapshot.go routeForTarget).
// The share=0 palier is the clean one — there the delta is the Bloom gate and nothing else, which is
// exactly what the 70-90% of non-ported traffic pays in production.
//
// # Three paliers, and why each exists
//
//   - share=0 ...................... the Bloom gate alone.
//   - share>0, pool much below the window's ported lookups .... the Redis arm.
//   - share>0, pool at or above them .......................... the Postgres arm, and the pool pressure.
//
// Run them one `go test` at a time, on a rested host: a bench started behind another measures a host that
// has just worked (test/load/README.md, step-230).
func TestRouterL0Fidelity(t *testing.T) {
	brokers := kafkatest.Brokers(t)
	rdb := redistest.Client(t)

	hold := envDuration(t, envCalHold, routerCeilingHold)
	records := int(envFloat(t, envPrefill, routerCeilingPrefill))
	pairs := int(envFloat(t, envFidelityPairs, poolFidelityPairs))
	share := envFloat(t, envPortedShare, 0.30)
	portedPool := int(envFloat(t, envPortedPool, 100000))
	maxConns := int32(envFloat(t, envPgMaxConns, float64(config.Defaults().Postgres.MaxConns)))

	fix := newL0Fixture(t, rdb, share, portedPool, maxConns)

	bed := newRouterBed(t, brokers, l0Lanes, records, func(i int) string {
		return l0Dest(i, share, portedPool)
	})

	without := make([]float64, 0, pairs)
	with := make([]float64, 0, pairs)
	var last routerPalier
	var pressure, footprint string

	for i := range pairs {
		bare := func() float64 {
			return measureRouterPalier(t, bed, hold, refResolver{fix.snapshot}, nil, "declarative only").rate
		}
		wired := func() float64 {
			// Emptied before every wired palier, never during one: the cache TTL is six hours and the
			// window is thirty seconds, so without this the second palier reads a cache the first one
			// warmed and pg_hit collapses to zero while the pool looks idle. This is the freshStore
			// argument, and it empties the WHOLE database for the same reason and with the same safety:
			// a package's tests run sequentially and nothing else lives behind the loadref tag.
			if err := rdb.FlushDB(context.Background()).Err(); err != nil {
				t.Fatalf("emptying the exactroute cache between paliers: %v", err)
			}
			// The baseline is read AFTER the flush, and the order is the whole measurement: read before
			// it and the count the palier "added" is its own keys minus the previous palier's identical
			// set, which is zero. The first run of this bench did exactly that, and cacheFootprint
			// refused to price it rather than print a plausible number.
			keysBefore, memBefore := redisFootprint(t, rdb)
			statBefore := fix.pool.Stat()
			probe := &l0Probe{lookups: fix.lookups, verify: func(mix map[string]uint64, messages uint64) error {
				return mixHolds(mix, messages, share, portedPool)
			}}
			last = measureRouterPalier(t, bed, hold, fix.l0, probe, "L0 wired")
			statAfter := fix.pool.Stat()
			keysAfter, memAfter := redisFootprint(t, rdb)
			pressure = poolPressure(
				statAfter.AcquireCount()-statBefore.AcquireCount(),
				statAfter.EmptyAcquireCount()-statBefore.EmptyAcquireCount(),
				statAfter.AcquireDuration()-statBefore.AcquireDuration(),
				last.messages, maxConns, l0Lanes)
			footprint = cacheFootprint(keysBefore, keysAfter, memBefore, memAfter)
			return last.rate
		}
		// Alternate, so neither side is always the one that ran second.
		if i%2 == 0 {
			without = append(without, bare())
			with = append(with, wired())
			continue
		}
		with = append(with, wired())
		without = append(without, bare())
	}

	verdict, err := fidelityDelta(without, with, "the L0 stage")
	if err != nil {
		t.Fatalf("share=%.3f pool=%d, %d pairs: %v", share, portedPool, pairs, err)
	}

	t.Logf("l0 fidelity: share=%.3f · ported pool %d · %d lanes · MaxConns=%d · %s",
		share, portedPool, l0Lanes, maxConns, verdict)
	t.Logf("             mix over the last wired window: %s", renderMix(last.mix, last.messages))
	t.Logf("             pgx: %s", pressure)
	t.Logf("             redis: %s", footprint)
}

// l0Fixture is everything the paliers measure THROUGH: the seeded control plane, the dedicated pgx pool,
// the filter, the snapshot and the resolver chain.
//
// It exists as a type so the test body holds no context of its own. Every setup call needs one and every
// palier must not: threading a single ctx through both would hand the measurement a cancellation channel
// it has no use for, and the linter is right to refuse it.
type l0Fixture struct {
	pool     *pgxpool.Pool
	snapshot *routing.SnapshotResolver
	l0       *routing.L0Resolver
	lookups  *countingLookups
}

func newL0Fixture(t *testing.T, rdb *redis.Client, share float64, portedPool int, maxConns int32) *l0Fixture {
	t.Helper()
	ctx := context.Background()

	// The control plane is seeded through the SHARED pool: the seed is not part of any window, and
	// running it through the dedicated pool would leave its connections warm in a way production's would
	// not be at the first message.
	shared := pgtest.Pool(t)
	_, connectorID := seedRefControlPlane(t, shared, 1)
	seedExactRoutes(t, postgres.NewExactRouteRepo(shared), connectorID, share, portedPool)

	// The dedicated pool is the instrument. pgtest.Pool hands out a pool shared by the whole package,
	// opened by pgxpool.New with no config at all, so its Stat() describes every test that ran before —
	// and its MaxConns is the driver's default, not production's.
	cfg := pgtest.Config(t)
	cfg.MaxConns = maxConns
	cfg.MinConns = config.Defaults().Postgres.MinConns
	pool, err := postgres.NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("dedicated pool at MaxConns=%d: %v", maxConns, err)
	}
	t.Cleanup(pool.Close)

	repo := postgres.NewExactRouteRepo(pool)
	// The filter is built AFTER the seed, and that order is the bench's most dangerous invariant: built
	// before, MightContain answers false for every ported number, the store is never reached, and the
	// palier prices an L0 stage that did nothing while looking like a measurement. The preflight below
	// catches it in a second; mixHolds is the net behind it.
	bloom, err := exact.LoadBloom(ctx, repo)
	if err != nil {
		t.Fatalf("load exact-route bloom: %v", err)
	}
	snapshot, err := routing.LoadSnapshot(ctx, postgres.NewRouteRepo(pool))
	if err != nil {
		t.Fatalf("load route snapshot: %v", err)
	}

	lookups := newCountingLookups()
	l0 := routing.NewL0Resolver(
		exact.NewResolver(bloom, rdb, repo, exact.DefaultCacheTTL, exact.WithLookupMeter(lookups)),
		nil, // no script stage: L1 is another milestone's question
		snapshot,
	)
	preflightL0(t, l0, share, portedPool)

	return &l0Fixture{pool: pool, snapshot: snapshot, l0: l0, lookups: lookups}
}

// seedExactRoutes writes the ported working set into exact_routes, every row pointing at the connector
// the catch-all route already targets.
//
// Pointing them at the SAME connector is deliberate: the routing OUTCOME is then identical on both sides
// of every pair, so the delta is the cost of the lookup and not the cost of a different route. It is
// also the shape a real MNP import has (spec §6.1: MSISDN -> carrier).
func seedExactRoutes(t *testing.T, repo *postgres.ExactRouteRepo, connector uuid.UUID, share float64, pool int) {
	t.Helper()
	if share <= 0 {
		return
	}
	ctx := context.Background()
	target := exact.Target{Type: exact.TargetConnector, ID: connector}

	seen := make(map[string]bool, pool)
	batch := make([]exact.Route, 0, l0SeedChunk)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := repo.BulkUpsert(ctx, batch); err != nil {
			t.Fatalf("seeding %d exact routes: %v", len(batch), err)
		}
		batch = batch[:0]
	}
	// Drawn from l0Dest itself rather than from a parallel formula: a seed that computes its numbers a
	// second way is a seed that can disagree with the lookup, and the disagreement reads as a clean
	// 100% bloom_miss.
	for i := 0; len(seen) < pool; i++ {
		dest := l0Dest(i, share, pool)
		if dest == nonPortedDest || seen[dest] {
			continue
		}
		seen[dest] = true
		batch = append(batch, exact.Route{MSISDN: dest, Target: target, Source: exact.SourceMNPImport})
		if len(batch) == l0SeedChunk {
			flush()
		}
	}
	flush()
	t.Logf("seeded %d exact routes on connector %s", len(seen), connector)
}

// preflightL0 resolves one ported and one non-ported number before the window opens, so a seed that
// never bound fails in a second instead of after thirty.
func preflightL0(t *testing.T, l0 *routing.L0Resolver, share float64, pool int) {
	t.Helper()
	if share <= 0 {
		return
	}
	ported := l0Dest(0, share, pool)
	if ported == nonPortedDest {
		t.Fatalf("share=%.3f puts no ported number at index 0: the bench would measure a share it did not seed", share)
	}
	route, err := l0.Resolve(context.Background(), pipeline.RouteRequest{Dest: ported, Segments: 1})
	if err != nil {
		t.Fatalf("preflight on ported %s: %v", ported, err)
	}
	if route.ConnectorID == uuid.Nil {
		t.Fatalf("preflight on ported %s resolved to no connector: the seed, the bloom or the canonical form disagree", ported)
	}
}

// redisFootprint reads the cache size and the instance's memory, to be bracketed around a palier.
//
// Both readings are needed because only their DIFFERENCE prices this palier: the instance holds whatever
// the run left behind, and used_memory alone would divide all of it by the keys one window added.
func redisFootprint(t *testing.T, rdb *redis.Client) (keys, used int64) {
	t.Helper()
	ctx := context.Background()

	keys, err := rdb.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("reading the cache size: %v", err)
	}
	info, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		t.Fatalf("reading redis memory: %v", err)
	}
	for line := range strings.SplitSeq(info, "\n") {
		raw, ok := strings.CutPrefix(strings.TrimSpace(line), "used_memory:")
		if !ok {
			continue
		}
		if used, err = strconv.ParseInt(raw, 10, 64); err != nil {
			t.Fatalf("parsing used_memory %q: %v", raw, err)
		}
		return keys, used
	}
	t.Fatal("redis INFO memory carried no used_memory line: the footprint cannot be priced")
	return 0, 0
}

// renderMix prints the outcome mix as counts and as lookups per message.
func renderMix(mix map[string]uint64, messages uint64) string {
	if len(mix) == 0 || messages == 0 {
		return "no observation"
	}
	var total uint64
	out := ""
	for _, name := range l0Outcomes {
		n := mix[name]
		total += n
		if n == 0 {
			continue
		}
		out += fmt.Sprintf("%s %d (%.1f%%) · ", name, n, 100*float64(n)/float64(messages))
	}
	return fmt.Sprintf("%s%d lookups over %d messages (%.2f/message)",
		out, total, messages, float64(total)/float64(messages))
}
