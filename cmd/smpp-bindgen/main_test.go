package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/test/load/bindgen"
)

// The exit status is the whole point of running this from a script: nobody greps the log lines, they
// read the code. So the rule "what makes a run a failure" is asserted on directly, one report per
// case, rather than left to a peer and a shell.
//
// The case that matters most is the healthy one. A saturating injection normally ends with its
// writers torn out of a blocked WritePDU by the closing window, and treating that as a failure would
// hand a non-zero exit to every full-length run of the tool's own default mode.
func TestVerdict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rep          bindgen.Report
		submit       bool
		wantContains string // empty means the run must be a success
	}{
		{
			name: "a clean bind probe",
			rep:  bindgen.Report{Requested: 10, Bound: 10},
		},
		{
			name:         "a bind that never bound",
			rep:          bindgen.Report{Requested: 10, Bound: 9, Failed: 1},
			wantContains: "binds failed",
		},
		{
			name:         "a session the peer let go",
			rep:          bindgen.Report{Requested: 10, Bound: 10, Dropped: 2},
			wantContains: "dropped",
		},
		{
			name:         "an injector that pushed nothing",
			rep:          bindgen.Report{Requested: 4, Bound: 4},
			submit:       true,
			wantContains: "no submit_sm",
		},
		{
			// The end of any full-length saturating run: every writer was mid-write when its window
			// closed. Nothing failed, and the exit status must say so.
			name:   "writers the closing window cut short",
			rep:    bindgen.Report{Requested: 8, Bound: 8, Submitted: 240000, Accepted: 239000, SubmitCutShort: 8},
			submit: true,
		},
		{
			name:         "a write that failed on the wire",
			rep:          bindgen.Report{Requested: 8, Bound: 8, Submitted: 1200, SubmitErrors: 3, SubmitErr: errors.New("broken pipe")},
			submit:       true,
			wantContains: "failed on the wire",
		},
		{
			// Cut-short writers must not launder a real failure sitting next to them.
			name: "a real failure next to cut-short writers",
			rep: bindgen.Report{Requested: 8, Bound: 8, Submitted: 1200, SubmitCutShort: 7,
				SubmitErrors: 1, SubmitErr: errors.New("broken pipe")},
			submit:       true,
			wantContains: "failed on the wire",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := verdict(tc.rep, tc.submit)
			if tc.wantContains == "" {
				if err != nil {
					t.Fatalf("verdict = %v, want a successful run", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("verdict = nil, want a failure naming %q", tc.wantContains)
			}
			if !strings.Contains(err.Error(), tc.wantContains) {
				t.Errorf("verdict = %q, want it to name %q", err, tc.wantContains)
			}
		})
	}
}
