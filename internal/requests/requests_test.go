package requests

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/heainframework/heain-database/internal/box"
)

func TestRequests(t *testing.T) {
	b, _ := box.New(bytes.Repeat([]byte{5}, 32))
	p := filepath.Join(t.TempDir(), "r.db")
	s, _ := Open(p, b)
	_ = s.Put(Request{ActionID: "appp5-a", Kind: KindPolicyWrite, State: StateWaiting, Namespace: "n", Key: "k", Value: "pending-secret-value"})
	_ = s.Put(Request{ActionID: "appp5-b", Kind: KindDatasetPurge, State: StateWaiting, Dataset: "voters", ScopeKey: "d9"})
	w, _ := s.Waiting()
	if len(w) != 2 {
		t.Fatal("waiting")
	}
	r, _ := s.Get("appp5-a")
	_ = s.Decide(r, StateApplied, "")
	if r, _ = s.Get("appp5-a"); r.State != StateApplied || r.Value != "" || r.DecidedAt == nil {
		t.Fatalf("%+v", r)
	}
	if w, _ = s.Waiting(); len(w) != 1 || w[0].ActionID != "appp5-b" {
		t.Fatal("waiting after decide")
	}
	if _, err := s.Get("nope"); err != ErrNotFound {
		t.Fatal("not found")
	}
	_ = s.Close()
	raw, _ := os.ReadFile(p)
	if bytes.Contains(raw, []byte("pending-secret")) || bytes.Contains(raw, []byte("voters")) {
		t.Fatal("plaintext at rest")
	}
}
