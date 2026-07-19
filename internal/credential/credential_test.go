package credential_test

import (
	"strings"
	"testing"

	"github.com/martialanouman/go-gateway/internal/credential"
)

// TestAnAPIKeyHashIsDeterministicSoTheLookupIndexWorks is the load-bearing property of §1.9: the
// same key must hash to the same value every time, or the indexed lookup on api_key_hash cannot
// find the row.
func TestAnAPIKeyHashIsDeterministicSoTheLookupIndexWorks(t *testing.T) {
	const key = "sgw_abcdefghijklmnopqrstuvwxyz0123456789ABCD"
	first := credential.HashAPIKey(key)
	second := credential.HashAPIKey(key)
	if first != second {
		t.Fatal("HashAPIKey is not deterministic; the lookup index would never match")
	}
}

// TestAPIKeyCarriesTheSgwPrefix: the prefix is what makes a leaked key recognisable on sight.
func TestAPIKeyCarriesTheSgwPrefix(t *testing.T) {
	key, hash, err := credential.GenerateAPIKey()
	if err != nil {
		t.Fatalf("GenerateAPIKey() error = %v", err)
	}
	if !strings.HasPrefix(key, credential.APIKeyPrefix) {
		t.Errorf("key %q does not carry the %q prefix", key, credential.APIKeyPrefix)
	}
	if hash != credential.HashAPIKey(key) {
		t.Error("returned hash does not match HashAPIKey(key)")
	}
	if !credential.VerifyAPIKey(key, hash) {
		t.Error("VerifyAPIKey rejects the key it was generated with")
	}
}

// TestVerifyAPIKeyRejectsAWrongKey: a different key must not verify.
func TestVerifyAPIKeyRejectsAWrongKey(t *testing.T) {
	_, hash, _ := credential.GenerateAPIKey()
	if credential.VerifyAPIKey("sgw_not-the-right-key", hash) {
		t.Error("VerifyAPIKey accepted a wrong key")
	}
}

// TestGeneratedAPIKeysDoNotRepeat: 10k draws must all be distinct, or the 256-bit entropy claim is
// false.
func TestGeneratedAPIKeysDoNotRepeat(t *testing.T) {
	seen := make(map[string]struct{}, 10000)
	for i := 0; i < 10000; i++ {
		key, _, err := credential.GenerateAPIKey()
		if err != nil {
			t.Fatalf("GenerateAPIKey() error = %v", err)
		}
		if _, dup := seen[key]; dup {
			t.Fatalf("duplicate API key generated at draw %d", i)
		}
		seen[key] = struct{}{}
	}
}

// TestTwoBindPasswordHashesOfTheSameInputDifferBecauseTheSaltIsRandom: unlike the API key, a bind
// password is salted, so hashing the same password twice yields different encodings.
func TestTwoBindPasswordHashesOfTheSameInputDifferBecauseTheSaltIsRandom(t *testing.T) {
	const pw = "correct horse battery staple"
	h1, err := credential.HashBindPassword(pw)
	if err != nil {
		t.Fatalf("HashBindPassword() error = %v", err)
	}
	h2, err := credential.HashBindPassword(pw)
	if err != nil {
		t.Fatalf("HashBindPassword() error = %v", err)
	}
	if h1 == h2 {
		t.Error("two hashes of the same password are identical; the salt is not random")
	}
	if !strings.HasPrefix(h1, "$argon2id$") {
		t.Errorf("hash %q is not a PHC argon2id string", h1)
	}
}

// TestVerifyBindPasswordAcceptsTheRightPasswordAndRejectsWrong exercises the whole PHC round-trip.
func TestVerifyBindPasswordAcceptsTheRightPasswordAndRejectsWrong(t *testing.T) {
	password, hash, err := credential.GenerateBindPassword()
	if err != nil {
		t.Fatalf("GenerateBindPassword() error = %v", err)
	}

	ok, err := credential.VerifyBindPassword(password, hash)
	if err != nil {
		t.Fatalf("VerifyBindPassword() error = %v", err)
	}
	if !ok {
		t.Error("VerifyBindPassword rejects the password it was generated with")
	}

	ok, err = credential.VerifyBindPassword("wrong password", hash)
	if err != nil {
		t.Fatalf("VerifyBindPassword() error = %v", err)
	}
	if ok {
		t.Error("VerifyBindPassword accepted a wrong password")
	}
}

// TestVerifyBindPasswordRejectsAMalformedHash: a corrupt encoding is an error — never a panic and
// never a silent accept. Covers the argon2-panic parameters (t=0 / p=0) and the empty-hash segment
// that would otherwise make a compare of two empty slices accept ANY password.
// TestBindPasswordFitsTheSMPPField: a generated bind password must fit the SMPP v3.4 §4.1.1 password
// field — a C-Octet String of at most 9 octets (8 usable chars + NUL) — or an ESME could never send
// it in a bind PDU. This is a regression guard for the 32-char default that made generated passwords
// unbindable.
func TestBindPasswordFitsTheSMPPField(t *testing.T) {
	const smppPasswordMaxChars = 8
	for range 100 {
		password, _, err := credential.GenerateBindPassword()
		if err != nil {
			t.Fatalf("GenerateBindPassword() error = %v", err)
		}
		if len(password) > smppPasswordMaxChars {
			t.Fatalf("bind password %q is %d chars, want <= %d (SMPP field limit)",
				password, len(password), smppPasswordMaxChars)
		}
	}
}

func TestVerifyBindPasswordRejectsAMalformedHash(t *testing.T) {
	bad := []string{
		"", "not-a-hash", "$argon2id$broken", "$bcrypt$v=19$m=1,t=1,p=1$x$y",
		"$argon2id$v=19$m=65536,t=0,p=4$c2FsdHNhbHQ$aGFzaGhhc2g", // t=0 -> argon2 panics without the guard
		"$argon2id$v=19$m=65536,t=1,p=0$c2FsdHNhbHQ$aGFzaGhhc2g", // p=0 -> argon2 panics without the guard
		"$argon2id$v=19$m=65536,t=1,p=4$c2FsdHNhbHQ$",            // empty hash -> would false-accept
		"$argon2id$v=19$m=99999999999,t=1,p=4$c2FsdA$aGFzaA",     // memory bomb
	}
	for _, enc := range bad {
		ok, err := credential.VerifyBindPassword("x", enc)
		if err == nil {
			t.Errorf("VerifyBindPassword(%q) = nil error, want a decode/range error", enc)
		}
		if ok {
			t.Errorf("VerifyBindPassword(%q) accepted a password against a malformed hash", enc)
		}
	}
}
