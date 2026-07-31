package billing_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/martialanouman/go-gateway/internal/billing"
	"github.com/martialanouman/go-gateway/internal/billing/pb"
	"github.com/martialanouman/go-gateway/internal/content"
	cp "github.com/martialanouman/go-gateway/internal/controlplane"
	errs "github.com/martialanouman/go-gateway/internal/platform/errors"
)

type fakeContentKeyStore struct {
	active       *cp.ContentKey
	createCalls  int
	rotateCalls  int
	lastWrapped  []byte
	lastKeyRef   string
	createErr    error
	rotateErr    error
	byID         map[uuid.UUID]cp.ContentKey
	destroyCount int
	destroyErr   error
	destroyCalls int
}

func (s *fakeContentKeyStore) GetActive(_ context.Context, customerID uuid.UUID) (cp.ContentKey, error) {
	if s.active == nil {
		return cp.ContentKey{}, errs.ErrNotFound
	}
	return *s.active, nil
}

func (s *fakeContentKeyStore) GetByID(_ context.Context, id uuid.UUID) (cp.ContentKey, error) {
	if s.byID == nil {
		return cp.ContentKey{}, errs.ErrNotFound
	}
	k, ok := s.byID[id]
	if !ok {
		return cp.ContentKey{}, errs.ErrNotFound
	}
	return k, nil
}

func (s *fakeContentKeyStore) CreateIfAbsent(_ context.Context, customerID uuid.UUID, wrapped []byte, keyRef string) (cp.ContentKey, bool, error) {
	s.createCalls++
	s.lastWrapped, s.lastKeyRef = wrapped, keyRef
	if s.createErr != nil {
		return cp.ContentKey{}, false, s.createErr
	}
	key := cp.ContentKey{ID: uuid.New(), CustomerID: customerID, WrappedKey: wrapped, KMSKeyRef: keyRef, Status: cp.ContentKeyActive}
	s.active = &key
	return key, true, nil
}

func (s *fakeContentKeyStore) DestroyByCustomer(_ context.Context, _ uuid.UUID) (int, error) {
	s.destroyCalls++
	return s.destroyCount, s.destroyErr
}

func (s *fakeContentKeyStore) Rotate(_ context.Context, customerID uuid.UUID, wrapped []byte, keyRef string) (cp.ContentKey, error) {
	s.rotateCalls++
	s.lastWrapped, s.lastKeyRef = wrapped, keyRef
	if s.rotateErr != nil {
		return cp.ContentKey{}, s.rotateErr
	}
	key := cp.ContentKey{ID: uuid.New(), CustomerID: customerID, WrappedKey: wrapped, KMSKeyRef: keyRef, Status: cp.ContentKeyActive}
	s.active = &key
	return key, nil
}

// TestGetOrCreateCreatesWhenAbsent: with no active key, the server generates+wraps a DEK and creates the key,
// and the response carries metadata only — never the wrapped bytes or any plaintext.
func TestGetOrCreateCreatesWhenAbsent(t *testing.T) {
	store := &fakeContentKeyStore{}
	kms := content.NewDevKMS()
	srv := billing.NewContentKeyServer(kms, store)
	cust := uuid.New()

	resp, err := srv.GetOrCreateContentKey(context.Background(), &pb.GetOrCreateContentKeyRequest{CustomerId: cust.String()})
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	if store.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", store.createCalls)
	}
	if resp.GetStatus() != string(cp.ContentKeyActive) || resp.GetKmsKeyRef() != kms.KeyRef() {
		t.Errorf("resp = %+v, want active + keyRef %q", resp, kms.KeyRef())
	}
	// The store received a non-empty wrapped DEK, and it is NOT the plaintext (unwrap must yield 32 bytes).
	if len(store.lastWrapped) == 0 {
		t.Fatal("store got an empty wrapped key")
	}
	dek, err := kms.UnwrapDataKey(context.Background(), store.lastWrapped)
	if err != nil || len(dek) != 32 {
		t.Errorf("wrapped key does not unwrap to a 32-byte DEK: (%d bytes, %v)", len(dek), err)
	}
}

// TestGetOrCreateReturnsExisting: an existing active key is returned without creating a new one.
func TestGetOrCreateReturnsExisting(t *testing.T) {
	cust := uuid.New()
	existing := cp.ContentKey{ID: uuid.New(), CustomerID: cust, KMSKeyRef: "kek/1", Status: cp.ContentKeyActive}
	store := &fakeContentKeyStore{active: &existing}
	srv := billing.NewContentKeyServer(content.NewDevKMS(), store)

	resp, err := srv.GetOrCreateContentKey(context.Background(), &pb.GetOrCreateContentKeyRequest{CustomerId: cust.String()})
	if err != nil {
		t.Fatalf("GetOrCreate: %v", err)
	}
	if store.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (existing key reused)", store.createCalls)
	}
	if resp.GetKeyId() != existing.ID.String() {
		t.Errorf("key_id = %s, want existing %s", resp.GetKeyId(), existing.ID)
	}
}

// TestRotateWrapsAndRotates: rotate seals a fresh DEK and calls the store's Rotate.
func TestRotateWrapsAndRotates(t *testing.T) {
	store := &fakeContentKeyStore{}
	kms := content.NewDevKMS()
	srv := billing.NewContentKeyServer(kms, store)
	cust := uuid.New()

	resp, err := srv.RotateContentKey(context.Background(), &pb.RotateContentKeyRequest{CustomerId: cust.String()})
	if err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if store.rotateCalls != 1 {
		t.Fatalf("rotateCalls = %d, want 1", store.rotateCalls)
	}
	if resp.GetStatus() != string(cp.ContentKeyActive) {
		t.Errorf("status = %q, want active", resp.GetStatus())
	}
	if _, err := kms.UnwrapDataKey(context.Background(), store.lastWrapped); err != nil {
		t.Errorf("rotated wrapped key does not unwrap: %v", err)
	}
}

// TestGetContentEncryptionKeyUnwrapsDEK: the guarded encrypt path returns the active key id plus its
// plaintext 32-byte DEK (the wrapped bytes unwrapped by the KMS), creating the key when absent.
func TestGetContentEncryptionKeyUnwrapsDEK(t *testing.T) {
	store := &fakeContentKeyStore{}
	kms := content.NewDevKMS()
	srv := billing.NewContentKeyServer(kms, store)
	cust := uuid.New()

	resp, err := srv.GetContentEncryptionKey(context.Background(), &pb.GetContentEncryptionKeyRequest{CustomerId: cust.String()})
	if err != nil {
		t.Fatalf("GetContentEncryptionKey: %v", err)
	}
	if len(resp.GetDek()) != 32 {
		t.Fatalf("dek length = %d, want 32", len(resp.GetDek()))
	}
	if resp.GetKeyId() == "" {
		t.Fatal("key_id empty")
	}
	// The returned DEK must be exactly what the stored wrapped bytes unwrap to.
	wantDEK, err := kms.UnwrapDataKey(context.Background(), store.lastWrapped)
	if err != nil {
		t.Fatalf("unwrap stored: %v", err)
	}
	if !bytes.Equal(resp.GetDek(), wantDEK) {
		t.Error("returned DEK differs from the stored wrapped key's plaintext")
	}
}

// TestGetContentEncryptionKeyReusesExisting: an existing active key is unwrapped, not recreated.
func TestGetContentEncryptionKeyReusesExisting(t *testing.T) {
	kms := content.NewDevKMS()
	cust := uuid.New()
	dek, _ := content.GenerateDataKey()
	wrapped, _ := kms.WrapDataKey(context.Background(), dek)
	existing := cp.ContentKey{ID: uuid.New(), CustomerID: cust, WrappedKey: wrapped, KMSKeyRef: kms.KeyRef(), Status: cp.ContentKeyActive}
	store := &fakeContentKeyStore{active: &existing}
	srv := billing.NewContentKeyServer(kms, store)

	resp, err := srv.GetContentEncryptionKey(context.Background(), &pb.GetContentEncryptionKeyRequest{CustomerId: cust.String()})
	if err != nil {
		t.Fatalf("GetContentEncryptionKey: %v", err)
	}
	if store.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0", store.createCalls)
	}
	if !bytes.Equal(resp.GetDek(), dek) || resp.GetKeyId() != existing.ID.String() {
		t.Error("did not return the existing key's DEK/id")
	}
}

// TestGetContentKeyUnwrapsSpecificKey: the decrypt path unwraps a specific (possibly retired) key by id.
func TestGetContentKeyUnwrapsSpecificKey(t *testing.T) {
	kms := content.NewDevKMS()
	dek, _ := content.GenerateDataKey()
	wrapped, _ := kms.WrapDataKey(context.Background(), dek)
	keyID := uuid.New()
	store := &fakeContentKeyStore{byID: map[uuid.UUID]cp.ContentKey{
		keyID: {ID: keyID, WrappedKey: wrapped, Status: cp.ContentKeyRetired}, // a retired key still decrypts old CDRs
	}}
	srv := billing.NewContentKeyServer(kms, store)

	resp, err := srv.GetContentKey(context.Background(), &pb.GetContentKeyRequest{KeyId: keyID.String()})
	if err != nil {
		t.Fatalf("GetContentKey: %v", err)
	}
	if resp.GetDestroyed() || !bytes.Equal(resp.GetDek(), dek) {
		t.Errorf("resp = {destroyed:%v dek==orig:%v}, want the retired key's DEK", resp.GetDestroyed(), bytes.Equal(resp.GetDek(), dek))
	}
}

// TestGetContentKeyDestroyedReturnsNoKey: a crypto-shredded key reports destroyed and returns no DEK.
func TestGetContentKeyDestroyedReturnsNoKey(t *testing.T) {
	keyID := uuid.New()
	store := &fakeContentKeyStore{byID: map[uuid.UUID]cp.ContentKey{
		keyID: {ID: keyID, WrappedKey: []byte("irrelevant"), Status: cp.ContentKeyDestroyed},
	}}
	srv := billing.NewContentKeyServer(content.NewDevKMS(), store)

	resp, err := srv.GetContentKey(context.Background(), &pb.GetContentKeyRequest{KeyId: keyID.String()})
	if err != nil {
		t.Fatalf("GetContentKey: %v", err)
	}
	if !resp.GetDestroyed() || len(resp.GetDek()) != 0 {
		t.Errorf("resp = {destroyed:%v dek_len:%d}, want destroyed + empty dek", resp.GetDestroyed(), len(resp.GetDek()))
	}
}

// TestDestroyContentKeysShreds: the crypto-shred RPC delegates to the store and returns the destroyed count.
func TestDestroyContentKeysShreds(t *testing.T) {
	store := &fakeContentKeyStore{destroyCount: 3}
	srv := billing.NewContentKeyServer(content.NewDevKMS(), store)

	resp, err := srv.DestroyContentKeys(context.Background(), &pb.DestroyContentKeysRequest{CustomerId: uuid.NewString()})
	if err != nil {
		t.Fatalf("DestroyContentKeys: %v", err)
	}
	if store.destroyCalls != 1 || resp.GetDestroyedCount() != 3 {
		t.Errorf("calls=%d count=%d, want 1/3", store.destroyCalls, resp.GetDestroyedCount())
	}
}

// TestDestroyContentKeysInvalidID: a non-UUID customer id is InvalidArgument, no store call.
func TestDestroyContentKeysInvalidID(t *testing.T) {
	store := &fakeContentKeyStore{}
	srv := billing.NewContentKeyServer(content.NewDevKMS(), store)
	_, err := srv.DestroyContentKeys(context.Background(), &pb.DestroyContentKeysRequest{CustomerId: "nope"})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
	if store.destroyCalls != 0 {
		t.Errorf("store touched on invalid id")
	}
}

// TestGetContentKeyUnknownIsNotFound: an unknown key id is NotFound.
func TestGetContentKeyUnknownIsNotFound(t *testing.T) {
	srv := billing.NewContentKeyServer(content.NewDevKMS(), &fakeContentKeyStore{})
	_, err := srv.GetContentKey(context.Background(), &pb.GetContentKeyRequest{KeyId: uuid.NewString()})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
}

// TestContentKeyInvalidCustomerID: a non-UUID customer id is InvalidArgument, no crypto or store call.
func TestContentKeyInvalidCustomerID(t *testing.T) {
	store := &fakeContentKeyStore{}
	srv := billing.NewContentKeyServer(content.NewDevKMS(), store)

	_, err := srv.GetOrCreateContentKey(context.Background(), &pb.GetOrCreateContentKeyRequest{CustomerId: "not-a-uuid"})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", status.Code(err))
	}
	if store.createCalls != 0 {
		t.Errorf("store touched on invalid id (createCalls=%d)", store.createCalls)
	}
}

// TestContentKeyStoreErrorPropagates: an unknown customer (store ErrNotFound on create) surfaces as NotFound.
func TestContentKeyStoreErrorPropagates(t *testing.T) {
	store := &fakeContentKeyStore{createErr: errs.ErrNotFound}
	srv := billing.NewContentKeyServer(content.NewDevKMS(), store)

	_, err := srv.GetOrCreateContentKey(context.Background(), &pb.GetOrCreateContentKeyRequest{CustomerId: uuid.NewString()})
	if status.Code(err) != codes.NotFound {
		t.Errorf("code = %v, want NotFound", status.Code(err))
	}
	if !strings.Contains(status.Convert(err).Message(), string(errs.ErrNotFound)) {
		t.Errorf("message = %q, want it to carry %q", status.Convert(err).Message(), errs.ErrNotFound)
	}
}
