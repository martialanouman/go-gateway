// Package credential generates and hashes the two kinds of secret an SMPP account holds: the SMPP
// bind password and the REST API key. The two use deliberately different schemes (plan §1.9).
//
//   - Bind password: argon2id with a per-hash random salt, encoded in the PHC string. It is verified
//     rarely (once per bind), so it can and must be slow; the row is found by system_id first and the
//     hash checked second.
//   - API key: format "sgw_<32 random bytes>", hashed with a DETERMINISTIC, UNSALTED SHA-256. It is
//     verified on every REST request (target 8000/s) and looked up BY the hash through an index, so a
//     per-row salt would turn every lookup into a full scan. The key carries 256 bits of entropy from
//     crypto/rand, so there is no low-entropy secret for a salt to protect and no rainbow table to
//     build.
//
// Comparisons are constant time in both cases. The Verify* functions are not called at M1 (no bind,
// no REST yet); they ship now, with tests, so the hash formats are settled and M2/M3 cannot reopen
// them.
package credential

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// APIKeyPrefix marks a gateway API key on sight — in a log an operator pastes into a ticket, or in
// a secret scanner's ruleset (plan §1.9).
const APIKeyPrefix = "sgw_"

// apiKeyBytes is the random payload length of an API key: 256 bits of entropy.
const apiKeyBytes = 32

// argon2id parameters. They target roughly 50-100ms on the deployment class; because they are
// encoded in the PHC string with each hash, they can be raised later without a migration — old
// hashes still verify against their own recorded parameters.
const (
	argonTime    = 1
	argonMemory  = 64 * 1024 // 64 MiB
	argonThreads = 4
	argonKeyLen  = 32
	argonSaltLen = 16

	// maxArgonMemoryKiB caps the memory a hash may request on verify (1 GiB), so a crafted PHC
	// string cannot make argon2.IDKey allocate an unbounded amount. Well above the 64 MiB default.
	maxArgonMemoryKiB = 1 << 20
)

// GenerateAPIKey returns a new API key "sgw_<43 base64url chars>" carrying 256 bits of entropy, and
// its storable deterministic hash.
func GenerateAPIKey() (key, hash string, err error) {
	buf := make([]byte, apiKeyBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("generate api key: %w", err)
	}
	key = APIKeyPrefix + base64.RawURLEncoding.EncodeToString(buf)
	return key, HashAPIKey(key), nil
}

// HashAPIKey is a DETERMINISTIC, UNSALTED SHA-256 of the key, hex-encoded. It is deterministic on
// purpose: the key is looked up by this hash through an index, and it is verified on every REST
// request. See the package doc for why salting is neither needed nor possible here.
func HashAPIKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// VerifyAPIKey reports whether key matches hash, in constant time.
func VerifyAPIKey(key, hash string) bool {
	return subtle.ConstantTimeCompare([]byte(HashAPIKey(key)), []byte(hash)) == 1
}

// GenerateBindPassword returns a new SMPP bind password and its argon2id hash.
func GenerateBindPassword() (password, hash string, err error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", "", fmt.Errorf("generate bind password: %w", err)
	}
	password = base64.RawURLEncoding.EncodeToString(buf)
	hash, err = HashBindPassword(password)
	if err != nil {
		return "", "", err
	}
	return password, hash, nil
}

// HashBindPassword applies argon2id with a fresh random salt and returns the standard PHC-encoded
// string ($argon2id$v=19$m=...,t=...,p=...$<salt>$<hash>), so the parameters travel with the hash.
func HashBindPassword(password string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("hash bind password: %w", err)
	}
	sum := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(sum),
	), nil
}

// VerifyBindPassword reports whether password matches the PHC-encoded hash. The comparison is
// constant time. It re-derives with the parameters recorded in the encoding, so a hash made with
// stronger parameters still verifies after the defaults are raised.
func VerifyBindPassword(password, encoded string) (bool, error) {
	params, salt, want, err := parsePHC(encoded)
	if err != nil {
		return false, err
	}
	//nolint:gosec // G115: the derived key length is argonKeyLen (32), well within uint32.
	got := argon2.IDKey([]byte(password), salt, params.time, params.memory, params.threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}

type argonParams struct {
	memory  uint32
	time    uint32
	threads uint8
}

// parsePHC decodes an argon2id PHC string into its parameters, salt and hash.
func parsePHC(encoded string) (argonParams, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	// ["", "argon2id", "v=19", "m=..,t=..,p=..", "<salt>", "<hash>"]
	if len(parts) != 6 || parts[1] != "argon2id" {
		return argonParams{}, nil, nil, errors.New("verify bind password: not an argon2id hash")
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil || version != argon2.Version {
		return argonParams{}, nil, nil, errors.New("verify bind password: unsupported argon2 version")
	}

	var p argonParams
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &p.memory, &p.time, &p.threads); err != nil {
		return argonParams{}, nil, nil, errors.New("verify bind password: malformed parameters")
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return argonParams{}, nil, nil, errors.New("verify bind password: malformed salt")
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return argonParams{}, nil, nil, errors.New("verify bind password: malformed hash")
	}

	// Bound the parameters decoded from an untrusted/corrupt hash. argon2.IDKey PANICS on time<1 or
	// threads<1, and an empty hash segment would make the constant-time compare of two empty slices
	// return equal — accepting ANY password. A crafted memory value would also let a hash trigger a
	// gigabyte allocation. Reject all of these before deriving.
	if p.time < 1 || p.threads < 1 || p.memory < 1 || p.memory > maxArgonMemoryKiB {
		return argonParams{}, nil, nil, errors.New("verify bind password: parameters out of range")
	}
	if len(salt) == 0 || len(want) == 0 {
		return argonParams{}, nil, nil, errors.New("verify bind password: empty salt or hash")
	}
	return p, salt, want, nil
}
