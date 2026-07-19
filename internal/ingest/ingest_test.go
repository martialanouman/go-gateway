package ingest_test

import (
	"testing"

	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

// TestAcceptedRowEncoding: the accepted row keys on the envelope's resolved Encoding string — the same
// value the connector's enroute row uses — so the two lifecycle rows agree. data_coding is the wire
// override the connector honours, not the CDR encoding label.
func TestAcceptedRowEncoding(t *testing.T) {
	dc := 0 // present but not consulted for the CDR encoding label
	tests := []struct {
		name     string
		encoding string
		want     clickhouse.Encoding
	}{
		{"ucs2", "ucs2", clickhouse.EncodingUCS2},
		{"binary", "binary", clickhouse.EncodingBinary},
		{"gsm7", "gsm7", clickhouse.EncodingGSM7},
		{"auto resolves to gsm7", "auto", clickhouse.EncodingGSM7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := ingest.AcceptedRow(pipeline.InboundMT{Encoding: tc.encoding, DataCoding: &dc})
			if row.Encoding != tc.want {
				t.Errorf("encoding = %q, want %q", row.Encoding, tc.want)
			}
		})
	}
}
