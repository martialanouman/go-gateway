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

// TestVerifyBindPasswordAcceptsTheReferenceImplementationVectors pins argon2id against hashes this
// repository did not produce. Every other test here hashes and verifies with the same code, so a
// future golang.org/x/crypto bump that changed the derivation would leave them ALL green while every
// bind password and API key already in the database became unverifiable — a total authentication
// outage, announced by nothing. Verified at the v0.53→v0.56 bump: the argon2 code was identical, so
// the risk had not materialised. It was not guarded either.
//
// The vectors come from the reference C implementation's own test suite, P-H-C/phc-winner-argon2,
// src/test.c at commit f57e61e19229e23c4445b85494dbf7c07de721cb (the hashtest lines for Argon2_id).
// Two of the eight, chosen for what they exercise rather than for coverage: the first carries the
// production parameters (t=1, m=64 MiB), the second is the only Argon2id vector upstream with p > 1,
// which is the lane-parallel derivation the production p=4 uses. The 256 MiB vector is deliberately
// left out: it would make every test run allocate a quarter of a gigabyte.
//
// They go through VerifyBindPassword — the path the stored hashes take — and not through argon2.IDKey
// directly, so the PHC parsing is pinned along with the derivation.
func TestVerifyBindPasswordAcceptsTheReferenceImplementationVectors(t *testing.T) {
	vectors := []struct {
		name     string
		password string
		encoded  string
	}{
		{
			name:     "t=1,m=65536,p=1 (the production time and memory)",
			password: "password",
			encoded:  "$argon2id$v=19$m=65536,t=1,p=1$c29tZXNhbHQ$9qWtwbpyPd3vm1rB1GThgPzZ3/ydHL92zKL+15XZypg",
		},
		{
			name:     "t=2,m=256,p=2 (the lane-parallel derivation, as production p=4)",
			password: "password",
			encoded:  "$argon2id$v=19$m=256,t=2,p=2$c29tZXNhbHQ$bQk8UB/VmZZF4Oo79iDXuL5/0ttZwg2f/5U52iv1cDc",
		},
	}

	for _, v := range vectors {
		t.Run(v.name, func(t *testing.T) {
			ok, err := credential.VerifyBindPassword(v.password, v.encoded)
			if err != nil {
				t.Fatalf("VerifyBindPassword() error = %v", err)
			}
			if !ok {
				t.Errorf("VerifyBindPassword rejects the reference vector %q: this Go argon2id no longer "+
					"agrees with phc-winner-argon2, so every hash already stored is unverifiable", v.encoded)
			}
		})
	}
}
