package clickhouse

import (
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"

	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

func TestAMetricsReadOverItsBudgetIsUnavailableNotInternal(t *testing.T) {
	for code, name := range map[int32]string{159: "TIMEOUT_EXCEEDED", 160: "TOO_SLOW", 202: "TOO_MANY_SIMULTANEOUS_QUERIES", 241: "MEMORY_LIMIT_EXCEEDED"} {
		if err := metricsErr("summary", &clickhouse.Exception{Code: code}); !errors.Is(err, errs.ErrServiceUnavailable) {
			t.Errorf("%s: err = %v, want ErrServiceUnavailable (503), a retry may pass", name, err)
		}
	}
	if err := metricsErr("summary", &clickhouse.Exception{Code: 62}); errors.Is(err, errs.ErrServiceUnavailable) {
		t.Errorf("SYNTAX_ERROR: err = %v, want an internal error, a retry cannot pass", err)
	}
}
