package restapi

import (
	"context"
	"time"
)

// Health is the public health body (api/openapi-public.yaml Health).
type Health struct {
	Status  string    `json:"status" enum:"ok,degraded"`
	Version string    `json:"version,omitempty"`
	Time    time.Time `json:"time" format:"date-time"`
}

type healthOutput struct {
	Body Health
}

// health is the public, unauthenticated health check on the business port (§1.5). It is distinct
// from the ops /healthz on the ops port. M2 always reports ok; degraded/503 is a later refinement.
func (s *server) health(_ context.Context, _ *struct{}) (*healthOutput, error) {
	return &healthOutput{Body: Health{
		Status:  "ok",
		Version: s.deps.Version,
		Time:    s.now(),
	}}, nil
}
