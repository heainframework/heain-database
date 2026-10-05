package box

import (
	"bytes"
	"testing"
)

func key(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func TestBox(t *testing.T) {
	a, err := New(key(1))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := New(key(2))
	if a.Index("account", "1234") != a.Index("account", "1234") || a.Index("account", "1234") == b.Index("account", "1234") {
		t.Fatal("index must be stable per key and differ across keys")
	}
	if a.Index("a", "bc") == a.Index("ab", "c") {
		t.Fatal("parts are separated")
	}
	s := a.Seal([]byte("secret"), "x")
	if bytes.Contains(s, []byte("secret")) {
		t.Fatal("plaintext in sealed value")
	}
	if v, err := a.Open(s, "x"); err != nil || string(v) != "secret" {
		t.Fatal("open")
	}
	if _, err := a.Open(s, "y"); err == nil {
		t.Fatal("aad")
	}
	if _, err := b.Open(s, "x"); err == nil {
		t.Fatal("other key")
	}
	if _, err := New([]byte("short")); err == nil {
		t.Fatal("short key")
	}
}
