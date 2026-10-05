package knowledgestore

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/heainframework/heain-database/internal/box"
)

func TestKnowledge(t *testing.T) {
	b, _ := box.New(bytes.Repeat([]byte{3}, 32))
	p := filepath.Join(t.TempDir(), "k.db")
	s, err := Open(p, b)
	if err != nil {
		t.Fatal(err)
	}
	e, err := s.PutLearned("asr", "model-th", "whisper-secret-v3")
	if err != nil || e.Version != 1 || e.Kind != KindLearned {
		t.Fatalf("%+v %v", e, err)
	}
	if e, _ = s.PutLearned("asr", "model-th", "v4"); e.Version != 2 {
		t.Fatal("version")
	}
	if _, err := s.PutPolicy("access", "threshold", "0.9", ""); err == nil {
		t.Fatal("policy needs an action id")
	}
	if e, err = s.PutPolicy("access", "threshold", "0.9", "appp5-1"); err != nil || e.Kind != KindPolicy {
		t.Fatal(err)
	}
	if _, err := s.PutLearned("access", "threshold", "0.1"); err == nil {
		t.Fatal("a policy entry cannot be overwritten as learned")
	}
	if g, _ := s.Get("access", "threshold"); g.Value != "0.9" || g.ActionID != "appp5-1" {
		t.Fatalf("%+v", g)
	}
	if _, err := s.Get("x", "y"); err != ErrNotFound {
		t.Fatal("not found")
	}
	_ = s.Close()
	raw, _ := os.ReadFile(p)
	for _, m := range []string{"whisper-secret", "model-th", "threshold"} {
		if bytes.Contains(raw, []byte(m)) {
			t.Fatalf("plaintext %q at rest", m)
		}
	}
}
