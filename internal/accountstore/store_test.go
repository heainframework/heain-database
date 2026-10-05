package accountstore

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/heainframework/heain-database/internal/box"
)

func TestAccounts(t *testing.T) {
	b, _ := box.New(bytes.Repeat([]byte{7}, 32))
	p := filepath.Join(t.TempDir(), "accounts.db")
	s, err := Open(p, b)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.Put(Account{ID: "citizen-1234567890123", Role: "voter", MetadataKV: map[string]string{"district": "secret-district-9"}})
	if err != nil || a.CreatedAt.IsZero() {
		t.Fatal(err)
	}
	a2, _ := s.Put(Account{ID: "citizen-1234567890123", Role: "officer"})
	if !a2.CreatedAt.Equal(a.CreatedAt) {
		t.Fatal("created_at is kept on replace")
	}
	if g, err := s.Get("citizen-1234567890123"); err != nil || g.Role != "officer" {
		t.Fatalf("get: %+v %v", g, err)
	}
	if _, err := s.Get("nobody"); err != ErrNotFound {
		t.Fatal("not found")
	}
	_, _ = s.Put(Account{ID: "x2", Role: "r", MetadataKV: map[string]string{"district": "secret-district-9"}})
	if l, _ := s.List(); len(l) != 2 {
		t.Fatalf("list %d", len(l))
	}
	_ = s.Delete("x2")
	_ = s.Delete("x2")
	if l, _ := s.List(); len(l) != 1 {
		t.Fatal("delete")
	}
	_ = s.Close()
	raw, _ := os.ReadFile(p)
	for _, m := range []string{"citizen-1234567890123", "secret-district-9", "officer"} {
		if bytes.Contains(raw, []byte(m)) {
			t.Fatalf("plaintext %q at rest", m)
		}
	}
	other, _ := box.New(bytes.Repeat([]byte{8}, 32))
	s2, _ := Open(p, other)
	defer s2.Close()
	if _, err := s2.Get("citizen-1234567890123"); err == nil {
		t.Fatal("another key must not find or open the record")
	}
}
