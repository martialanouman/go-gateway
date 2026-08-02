package ceiling

import (
	"context"

	"github.com/martialanouman/go-gateway/test/load/bindgen"
)

// BindgenLoad runs a tier with test/load/bindgen: it opens the tier's binds against one peer and
// injects submit_sm on all of them for the whole hold window.
//
// It is the only place the sweep touches an SMPP peer, which is what keeps the sweep itself testable
// without one.
type BindgenLoad struct {
	// Base is the configuration shared by every tier: peer address, credentials, timeouts, and the
	// submit_sm shape. Binds, Hold and OnAllBound are set per tier and anything put in them here is
	// ignored — they belong to the sweep, not to the peer.
	Base bindgen.Config
}

// Inject runs one tier and returns the injector's report.
//
// A nil Base.Submit is filled in with the package defaults rather than honoured: bindgen reads it as
// "bind and stay idle", which would hold the sessions open, submit nothing, and hand the sweep a peer
// reading of zero that looks exactly like a peer with no throughput at all.
func (l BindgenLoad) Inject(ctx context.Context, p LoadParams) (bindgen.Report, error) {
	cfg := l.Base
	cfg.Binds = p.Binds
	cfg.Hold = p.Hold
	// bindgen calls OnAllBound once every bind has settled and just before the injection window opens:
	// exactly the instant the sweep starts counting from.
	cfg.OnAllBound = p.OnStart
	if cfg.Submit == nil {
		cfg.Submit = &bindgen.SubmitConfig{}
	}
	return bindgen.Run(ctx, cfg)
}
