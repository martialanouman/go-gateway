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
//   BASE_URL=https://api.example.com API_KEY=sgw_… k6 run test/load/k6/messages.js
//
// Only `smoke` is meant to run on a developer machine or in CI. `sustained` and `peak` state the
// production targets and need a load generator sized for them; run locally they measure the laptop,
// not the gateway.

import http from 'k6/http';
import { check } from 'k6';

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
function msisdn() {
  const n = (__VU * 1000000 + (__ITER % 1000000)) % 10000000;

  return `+225070${String(n).padStart(7, '0')}`;
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

export default function () {
  const res = http.post(`${BASE_URL}/v1/messages`, submission(), params);

  check(res, {
    [CHECK_ACCEPTED]: (r) => r.status === 202,
  });
}
