// Package box seals heain-database's stored values and blinds its lookup
// keys under one data key from heain-core's KMS (heain-sdk App.DataKey).
//
// Values are AES-256-GCM (heain-sdk Sealer), bound to their place by the
// additional data. Lookup keys -- an account id, a citizen id in an
// imported registry -- are stored only as HMAC-SHA256 under a key derived
// from the data key, so no plaintext identifier rests on disk or in the
// SQL database either. Destroying the data key in core makes both the
// values and the index useless (crypto-shred).
package box

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/heainframework/heain-sdk/heain"
)

// Box is one data key's sealer and index.
type Box struct {
	s   *heain.Sealer
	mac []byte
}

// New builds a Box from a 32-byte data key.
func New(key []byte) (*Box, error) {
	s, err := heain.NewSealer(key)
	if err != nil {
		return nil, err
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte("heain-database/index/v1"))
	return &Box{s: s, mac: m.Sum(nil)}, nil
}

// Index blinds a lookup key made of parts (hex HMAC-SHA256).
func (b *Box) Index(parts ...string) string {
	m := hmac.New(sha256.New, b.mac)
	m.Write([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(m.Sum(nil))
}

// Seal encrypts v bound to aad.
func (b *Box) Seal(v []byte, aad string) []byte { return b.s.Seal(v, []byte(aad)) }

// Open decrypts what Seal returned for the same aad.
func (b *Box) Open(sealed []byte, aad string) ([]byte, error) { return b.s.Open(sealed, []byte(aad)) }
