package knowledgestore

import (
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "knowledge.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPutLearnedIsImmediate(t *testing.T) {
	s := openTestStore(t)
	e, err := s.PutLearned("models", "video-restoration", "v3")
	if err != nil {
		t.Fatalf("PutLearned: %v", err)
	}
	if e.Kind != KindLearned || e.Version != 1 {
		t.Fatalf("unexpected entry: %+v", e)
	}

	got, err := s.Get("models", "video-restoration")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Value != "v3" {
		t.Fatalf("expected value v3, got %q", got.Value)
	}
}

func TestLearnedVersionIncrements(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.PutLearned("ns", "k", "v1"); err != nil {
		t.Fatalf("PutLearned: %v", err)
	}
	e2, err := s.PutLearned("ns", "k", "v2")
	if err != nil {
		t.Fatalf("PutLearned: %v", err)
	}
	if e2.Version != 2 {
		t.Fatalf("expected version 2, got %d", e2.Version)
	}
}

func TestPolicyWriteRequiresApproval(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.ProposePolicyWrite("req-1", "policy", "max-retries", "5"); err != nil {
		t.Fatalf("ProposePolicyWrite: %v", err)
	}

	// Not yet in effect.
	if _, err := s.Get("policy", "max-retries"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound before approval, got %v", err)
	}

	pending, err := s.ListPending()
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(pending) != 1 || pending[0].RequestID != "req-1" {
		t.Fatalf("unexpected pending list: %+v", pending)
	}

	entry, err := s.ApprovePolicyWrite("req-1")
	if err != nil {
		t.Fatalf("ApprovePolicyWrite: %v", err)
	}
	if entry.Kind != KindPolicy || entry.Value != "5" {
		t.Fatalf("unexpected approved entry: %+v", entry)
	}

	got, err := s.Get("policy", "max-retries")
	if err != nil {
		t.Fatalf("Get after approval: %v", err)
	}
	if got.Value != "5" {
		t.Fatalf("expected value 5 after approval, got %q", got.Value)
	}

	pending, err = s.ListPending()
	if err != nil {
		t.Fatalf("ListPending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected empty pending list after approval, got %+v", pending)
	}
}

func TestPolicyWriteRejection(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.ProposePolicyWrite("req-2", "policy", "k", "v"); err != nil {
		t.Fatalf("ProposePolicyWrite: %v", err)
	}
	if err := s.RejectPolicyWrite("req-2"); err != nil {
		t.Fatalf("RejectPolicyWrite: %v", err)
	}
	if _, err := s.Get("policy", "k"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, rejected write must never take effect, got %v", err)
	}
	if _, err := s.ApprovePolicyWrite("req-2"); err != ErrPendingNotFound {
		t.Fatalf("expected ErrPendingNotFound for already-rejected request, got %v", err)
	}
}

func TestApproveUnknownRequest(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.ApprovePolicyWrite("does-not-exist"); err != ErrPendingNotFound {
		t.Fatalf("expected ErrPendingNotFound, got %v", err)
	}
}
