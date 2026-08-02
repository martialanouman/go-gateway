// Load script for the gateway's ingestion path: it hammers POST /v1/messages and encodes the
// ingestion latency budget as a k6 threshold, so a run that misses the budget exits non-zero
// (k6 exits 99 on a crossed threshold) instead of merely printing a slow number.
//
// The point of a load harness is its ability to FAIL. That is verified against test/load/stub
// (`go run ./cmd/load-stub`): with `-delay 0` this script must exit 0, with `-delay 300ms` — above
// the 250 ms budget — it must exit non-zero. If both runs pass, the thresholds are not wired.
//
// WHAT IS AND IS NOT ENCODED HERE
//
// Encoded: the INGESTION budget, p99 < 250 ms. k6 measures submission → HTTP response, which is
// exactly the span that budget covers.
//
// NOT encoded, deliberately: the end-to-end budget (< 2 s, submission → SMSC delivery). k6 never
// observes the SMSC leg — it sees a 202 and nothing after it — so a threshold claiming to cover
// end-to-end from here would measure the wrong thing while looking correct. Do not add one.
// End-to-end latency belongs to a probe that watches the outbound side.
//
// Two guards sit beside the latency threshold, because a fast failure is still a failure: a run
// where every request is rejected in 2 ms would sail through a latency-only threshold. So the HTTP
// error rate is bounded, and every response is checked for 202 under its own threshold.
//
// USAGE
//
//   k6 run test/load/k6/messages.js                      # smoke (default), CI-sized
//   PROFILE=sustained k6 run test/load/k6/messages.js     # 8 000 req/s — never in CI
//   PROFILE=peak      k6 run test/load/k6/messages.js     # 15 000 req/s — never in CI
//   IDEMPOTENCY=on    k6 run test/load/k6/messages.js     # exercise the Idempotency-Key path
//   BASE_URL=https://api.example.com API_KEY=sgw_… k6 run test/load/k6/messages.js
//
// Only `smoke` is meant to run on a developer machine or in CI. `sustained` and `peak` state the
// production targets and need a load generator sized for them; run locally they measure the laptop,
// not the gateway.
//
// IDEMPOTENCY selects WHICH server path is measured, not how hard. The gateway switches to
// submitIdempotent as soon as the header is present, adding two Redis round-trips (Reserve before
// the Kafka publish, Finalize after). Tuning capacity with the header off would optimise a path that
// retrying clients never take, and the NFR verdict would be pronounced on the favourable case. It
// defaults to off because the header also changes what a run costs the target's Redis — see
// test/load/README.md before pointing an `IDEMPOTENCY=on` run at anything shared.

import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

// INGESTION_BUDGET_MS is the ingestion latency budget from the technical specification. It is the
// number this whole harness exists to defend; changing it changes what a green run certifies.
const INGESTION_BUDGET_MS = 250;

// ERROR_BUDGET is the tolerated share of failed responses. It also floors the accepted-202 rate, so
// both guards agree on what "healthy" means.
const ERROR_BUDGET = 0.01;

// CHECK_ACCEPTED names the 202 check. It is a constant because the threshold below selects the
// check's submetric by that exact name — a rename in one place only would silently unwire the guard.
const CHECK_ACCEPTED = 'status is 202';

// PROFILES holds the three load levels. The rates are the capacity targets: 8 000 req/s sustained,
// 15 000 req/s at peak. `smoke` is not a scaled-down target — it is just enough traffic to prove
// the harness and the endpoint work, over enough samples for a p99 to mean something.
const PROFILES = {
  smoke: { rate: 50, duration: '10s', preAllocatedVUs: 25, maxVUs: 100 },
  sustained: { rate: 8000, duration: '60s', preAllocatedVUs: 800, maxVUs: 4000 },
  peak: { rate: 15000, duration: '60s', preAllocatedVUs: 1500, maxVUs: 8000 },
};

const PROFILE = __ENV.PROFILE || 'smoke';

const profile = PROFILES[PROFILE];
if (!profile) {
  // Init-time failure: a typo must stop the run, not silently downgrade it to smoke.
  throw new Error(
    `unknown PROFILE "${PROFILE}"; expected one of ${Object.keys(PROFILES).join(', ')}`
  );
}

const IDEMPOTENCY = __ENV.IDEMPOTENCY || 'off';
if (IDEMPOTENCY !== 'on' && IDEMPOTENCY !== 'off') {
  // Same doctrine as PROFILE above: a typo must stop the run. Falling back to 'off' would hand back
  // a green run of the path the option exists to leave.
  throw new Error(`unknown IDEMPOTENCY "${IDEMPOTENCY}"; expected on or off`);
}
const IDEMPOTENT = IDEMPOTENCY === 'on';

// RUN_SEED separates this run's keys from every other run's. The gateway remembers a key for 24 h,
// so without a per-run component two runs on the same day would replay each other's keys and the
// second would measure the idempotency cache — the very defect the option exists to avoid, at the
// scale of the whole run. Milliseconds in base 36, then six random base-36 characters against two
// runs starting inside the same millisecond.
const RUN_SEED = `${Date.now().toString(36)}${randomBase36(6)}`;

function randomBase36(n) {
  const alphabet = '0123456789abcdefghijklmnopqrstuvwxyz';
  let out = '';
  for (let i = 0; i < n; i++) {
    out += alphabet[Math.floor(Math.random() * alphabet.length)];
  }

  return out;
}

const BASE_URL = __ENV.BASE_URL || 'http://127.0.0.1:8099';
// The stub authenticates on shape alone; a real gateway needs a real key passed in.
const API_KEY = __ENV.API_KEY || 'sgw_loadtest';
const SENDER_ID = __ENV.SENDER_ID || 'ACME';

export const options = {
  // k6's default summary prints p(90)/p(95) but not p(99) — the exact statistic this harness is
  // built around. Ask for it explicitly, or the number carrying the verdict stays invisible.
  summaryTrendStats: ['avg', 'min', 'med', 'p(95)', 'p(99)', 'max'],

  scenarios: {
    [PROFILE]: {
      executor: 'constant-arrival-rate',
      rate: profile.rate,
      timeUnit: '1s',
      duration: profile.duration,
      preAllocatedVUs: profile.preAllocatedVUs,
      maxVUs: profile.maxVUs,
      // A slow server must show up as latency, not as a truncated run.
      gracefulStop: '30s',
    },
  },

  thresholds: {
    // THE budget. Crossing it exits 99 — this line is the acceptance criterion.
    http_req_duration: [`p(99)<${INGESTION_BUDGET_MS}`],

    // Guard 1: fast failures are still failures. Without this, a run answering 401 in 2 ms would
    // clear the latency threshold in style.
    http_req_failed: [`rate<${ERROR_BUDGET}`],

    // Guard 2: the response must actually be an acceptance. Selected by check name so that adding
    // another check later cannot dilute this one.
    [`checks{check:${CHECK_ACCEPTED}}`]: [`rate>${1 - ERROR_BUDGET}`],
  },
};

// submission builds one valid body. Every field is one the public contract declares: the API is
// additionalProperties:false, so an extra key here turns the whole run into a wall of 422s.
function submission() {
  return JSON.stringify({
    to: msisdn(),
    from: SENDER_ID,
    text: 'go-gateway load harness',
  });
}

// msisdn spreads destinations across VUs and iterations so the run does not hammer a single number
// — routing, rate limiting and opt-out all key on the destination.
//
// The spread stays inside +22507000xxxx, the placeholder block this repo already uses for fixtures
// (`+2250700000000`). An earlier version varied seven digits, which produced live, assignable Orange
// CI subscriber numbers: pointed at a real gateway — which README.md explicitly shows how to do —
// a smoke run would have delivered 500 real SMS to 500 real people, and a sustained run 480 000.
// Widen this block only against a stub.
function msisdn() {
  // 977 is prime, so VUs congruent modulo a small number do not walk the same series.
  const n = (__VU * 977 + __ITER) % 10000;

  return `+225070000${String(n).padStart(4, '0')}`; // n=0 gives the repo placeholder exactly
}

const params = {
  headers: {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${API_KEY}`,
  },
  // No assertion reads the body; parsing it at 15 000 req/s would measure the load generator's JSON
  // throughput instead of the gateway's latency.
  responseType: 'text',
};

// requestParams returns the params for one request. With IDEMPOTENCY off it returns `params`
// untouched, so the header is ABSENT — never present and empty, which the gateway would read as "no
// idempotency" and answer 202 to without a word, leaving the harness measuring the plain path while
// looking instrumented.
//
// The key is `k6-<run seed>-<iteration>`, ~25 characters against the contract's limit of 128.
// `iterationInTest` comes from k6/execution and is unique by construction over the whole run,
// including distributed runs — unlike any arithmetic fold of (__VU, __ITER), such as the `% 10000`
// in msisdn() below, which can collide. It is unique per SCENARIO, and this script declares exactly
// one; adding a second scenario would need the scenario name folded in too. The `k6-` prefix is not
// decoration: it makes the harness's keys sweepable in the target's Redis.
//
// Object.assign rather than object spread: it is ES2015 and cannot depend on the compatibility mode
// the run was started with.
function requestParams() {
  if (!IDEMPOTENT) {
    return params;
  }

  return Object.assign({}, params, {
    headers: Object.assign({}, params.headers, {
      'Idempotency-Key': `k6-${RUN_SEED}-${exec.scenario.iterationInTest}`,
    }),
  });
}

export default function () {
  const res = http.post(`${BASE_URL}/v1/messages`, submission(), requestParams());

  check(res, {
    [CHECK_ACCEPTED]: (r) => r.status === 202,
  });
}
