package content_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
)

func TestEffectiveStorageResolvesInheritToThePlatformDefault(t *testing.T) {
	for _, platform := range []cp.ContentStorage{cp.ContentOff, cp.ContentStoredEncrypted} {
		cases := map[cp.ContentStorage]cp.ContentStorage{
			cp.ContentInherit:          platform,
			cp.ContentOff:              cp.ContentOff,
			cp.ContentStoredPlaintext:  cp.ContentStoredPlaintext,
			cp.ContentStoredEncrypted:  cp.ContentStoredEncrypted,
			cp.ContentStorage("bogus"): cp.ContentOff, // unknown → off, never the platform default
		}
		for in, want := range cases {
			if got := content.EffectiveStorage(in, platform); got != want {
				t.Errorf("EffectiveStorage(%q, platform %q) = %q, want %q", in, platform, got, want)
			}
		}
	}
}

// TestAPlatformDefaultNeverResolvesToPlaintext: the database refuses a plaintext platform default, and the
// resolution must not trust that alone — an inherit customer only stores in clear under its own contract.
func TestAPlatformDefaultNeverResolvesToPlaintext(t *testing.T) {
	for _, platform := range []cp.ContentStorage{cp.ContentStoredPlaintext, cp.ContentInherit, "bogus"} {
		if got := content.EffectiveStorage(cp.ContentInherit, platform); got != cp.ContentOff {
			t.Errorf("inherit under platform %q = %q, want off", platform, got)
		}
	}
}

type fakePolicyLister struct {
	rows        []cp.CustomerContentPolicy
	err         error
	platform    cp.ContentStorage
	platformErr error
}

func (f fakePolicyLister) ListContentStorage(context.Context) ([]cp.CustomerContentPolicy, error) {
	return f.rows, f.err
}

func (f fakePolicyLister) PlatformContentStorage(context.Context) (cp.ContentStorage, error) {
	if f.platform == "" {
		return cp.ContentOff, f.platformErr
	}
	return f.platform, f.platformErr
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

func TestPolicySnapshotResolvesInheritAgainstThePlatformDefault(t *testing.T) {
	inh, off := uuid.New(), uuid.New()
	snap, err := content.LoadPolicySnapshot(context.Background(), fakePolicyLister{
		platform: cp.ContentStoredEncrypted,
		rows: []cp.CustomerContentPolicy{
			{CustomerID: inh, ContentStorage: cp.ContentInherit},
			{CustomerID: off, ContentStorage: cp.ContentOff},
		},
	})
	if err != nil {
		t.Fatalf("LoadPolicySnapshot: %v", err)
	}
	if got := snap.For(inh); got != cp.ContentStoredEncrypted {
		t.Errorf("inherit customer = %q, want the platform's stored_encrypted", got)
	}
	if got := snap.For(off); got != cp.ContentOff {
		t.Errorf("explicit off customer = %q, want off: an opt-out beats the platform", got)
	}
}

func TestPolicySnapshotFailsWhenThePlatformDefaultIsUnreadable(t *testing.T) {
	_, err := content.LoadPolicySnapshot(context.Background(), fakePolicyLister{platformErr: errors.New("no table")})
	if err == nil {
		t.Fatal("LoadPolicySnapshot succeeded without a platform default; want an error, never a silent off")
	}
}
