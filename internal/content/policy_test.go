package content_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

func TestEffectiveStorageResolvesInheritToOff(t *testing.T) {
	cases := map[cp.ContentStorage]cp.ContentStorage{
		cp.ContentInherit:          cp.ContentOff, // conservative default
		cp.ContentOff:              cp.ContentOff,
		cp.ContentStoredPlaintext:  cp.ContentStoredPlaintext,
		cp.ContentStoredEncrypted:  cp.ContentStoredEncrypted,
		cp.ContentStorage("bogus"): cp.ContentOff, // unknown → off
	}
	for in, want := range cases {
		if got := content.EffectiveStorage(in); got != want {
			t.Errorf("EffectiveStorage(%q) = %q, want %q", in, got, want)
		}
	}
}

type fakePolicyLister struct {
	rows []cp.CustomerContentPolicy
	err  error
}

func (f fakePolicyLister) ListContentStorage(context.Context) ([]cp.CustomerContentPolicy, error) {
	return f.rows, f.err
}

func TestPolicySnapshotForResolvesAndDefaultsOff(t *testing.T) {
	enc, plain, inh := uuid.New(), uuid.New(), uuid.New()
	snap, err := content.LoadPolicySnapshot(context.Background(), fakePolicyLister{rows: []cp.CustomerContentPolicy{
		{CustomerID: enc, ContentStorage: cp.ContentStoredEncrypted},
		{CustomerID: plain, ContentStorage: cp.ContentStoredPlaintext},
		{CustomerID: inh, ContentStorage: cp.ContentInherit},
	}})
	if err != nil {
		t.Fatalf("LoadPolicySnapshot: %v", err)
	}
	if got := snap.For(enc); got != cp.ContentStoredEncrypted {
		t.Errorf("enc customer = %q, want stored_encrypted", got)
	}
	if got := snap.For(plain); got != cp.ContentStoredPlaintext {
		t.Errorf("plain customer = %q, want stored_plaintext", got)
	}
	if got := snap.For(inh); got != cp.ContentOff {
		t.Errorf("inherit customer = %q, want off (resolved)", got)
	}
	if got := snap.For(uuid.New()); got != cp.ContentOff {
		t.Errorf("unknown customer = %q, want off", got)
	}
}

func TestLoadPolicySnapshotPropagatesError(t *testing.T) {
	_, err := content.LoadPolicySnapshot(context.Background(), fakePolicyLister{err: errors.New("db down")})
	if err == nil {
		t.Fatal("LoadPolicySnapshot = nil, want the lister error")
	}
}

func TestPolicyHolderStoreSwapsAndDefaultsOff(t *testing.T) {
	cust := uuid.New()
	var h content.PolicyHolder

	// Before any Store, everyone resolves to off.
	if got := h.For(cust); got != cp.ContentOff {
		t.Fatalf("empty holder For = %q, want off", got)
	}

	enc, _ := content.LoadPolicySnapshot(context.Background(), fakePolicyLister{rows: []cp.CustomerContentPolicy{
		{CustomerID: cust, ContentStorage: cp.ContentStoredEncrypted},
	}})
	h.Store(enc)
	if got := h.For(cust); got != cp.ContentStoredEncrypted {
		t.Fatalf("after Store(encrypted) For = %q, want stored_encrypted", got)
	}

	// An opt-out swap must take effect immediately (the hot-reload guarantee).
	off, _ := content.LoadPolicySnapshot(context.Background(), fakePolicyLister{rows: []cp.CustomerContentPolicy{
		{CustomerID: cust, ContentStorage: cp.ContentOff},
	}})
	h.Store(off)
	if got := h.For(cust); got != cp.ContentOff {
		t.Errorf("after opt-out swap For = %q, want off", got)
	}
}
