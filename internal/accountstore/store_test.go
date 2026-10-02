package accountstore

import (
	"path/filepath"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "accounts.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPutGet(t *testing.T) {
	s := openTestStore(t)
	acc, err := s.Put(Account{ID: "acc-1", Role: "operator", MetadataKV: map[string]string{"zone": "th-01"}})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if acc.CreatedAt.IsZero() || acc.UpdatedAt.IsZero() {
		t.Fatalf("expected timestamps to be set, got %+v", acc)
	}

	got, err := s.Get("acc-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Role != "operator" || got.MetadataKV["zone"] != "th-01" {
		t.Fatalf("unexpected account: %+v", got)
	}
}

func TestPutPreservesCreatedAt(t *testing.T) {
	s := openTestStore(t)
	first, err := s.Put(Account{ID: "acc-1", Role: "operator"})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	second, err := s.Put(Account{ID: "acc-1", Role: "admin"})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("expected CreatedAt to be preserved across update: first=%v second=%v", first.CreatedAt, second.CreatedAt)
	}
	if second.Role != "admin" {
		t.Fatalf("expected role to update, got %q", second.Role)
	}
}

func TestGetNotFound(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Get("missing"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestDeleteIdempotent(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Put(Account{ID: "acc-1"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete("acc-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Delete("acc-1"); err != nil {
		t.Fatalf("second Delete should be a no-op, got: %v", err)
	}
	if _, err := s.Get("acc-1"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestList(t *testing.T) {
	s := openTestStore(t)
	for _, id := range []string{"a", "b", "c"} {
		if _, err := s.Put(Account{ID: id}); err != nil {
			t.Fatalf("Put(%s): %v", id, err)
		}
	}
	list, err := s.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected 3 accounts, got %d", len(list))
	}
}

func TestPutRejectsEmptyID(t *testing.T) {
	s := openTestStore(t)
	if _, err := s.Put(Account{}); err == nil {
		t.Fatalf("expected error for empty id")
	}
}
