package ingest_test

import (
	"testing"

	"github.com/martialanouman/go-gateway/internal/ingest"
	"github.com/martialanouman/go-gateway/internal/pipeline"
	"github.com/martialanouman/go-gateway/internal/smpp"
	"github.com/martialanouman/go-gateway/internal/storage/clickhouse"
)

func TestAcceptedRowEncoding(t *testing.T) {
	ucs2 := int(smpp.DataCodingUCS2)
	binary := int(smpp.DataCodingBinary)
	gsm7 := int(smpp.DataCodingGSM7)

	tests := []struct {
		name     string
		encoding string
		dataCode *int
		want     clickhouse.Encoding
	}{
		// SMPP sets Encoding "auto" and carries the real coding in data_coding: the accepted row must
		// reflect data_coding, not fall back to gsm7.
		{"data_coding ucs2 overrides auto", "auto", &ucs2, clickhouse.EncodingUCS2},
		{"data_coding binary overrides auto", "auto", &binary, clickhouse.EncodingBinary},
		{"data_coding gsm7", "auto", &gsm7, clickhouse.EncodingGSM7},
		// REST usually leaves data_coding nil and relies on the encoding enum (rétro-compat).
		{"no data_coding falls back to encoding", "ucs2", nil, clickhouse.EncodingUCS2},
		{"no data_coding auto is gsm7", "auto", nil, clickhouse.EncodingGSM7},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := ingest.AcceptedRow(pipeline.InboundMT{Encoding: tc.encoding, DataCoding: tc.dataCode})
			if row.Encoding != tc.want {
				t.Errorf("encoding = %q, want %q", row.Encoding, tc.want)
			}
		})
	}
}
