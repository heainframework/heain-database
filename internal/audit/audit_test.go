package audit

import (
	"path/filepath"
	"testing"
)

func TestRecordAndAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	if err := l.Record("dataset.import", "admin-cn", map[string]string{"name": "voter-eligibility"}); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := l.Record("dataset.purge", "admin-cn", map[string]string{"name": "voter-eligibility"}); err != nil {
		t.Fatalf("Record: %v", err)
	}

	events, err := l.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Action != "dataset.import" || events[1].Action != "dataset.purge" {
		t.Fatalf("unexpected event order/content: %+v", events)
	}
	if events[0].Fields["name"] != "voter-eligibility" {
		t.Fatalf("expected fields to round-trip, got %+v", events[0].Fields)
	}
}

func TestRecordPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Record("account.put", "x", nil); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	l2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	if err := l2.Record("account.put", "y", nil); err != nil {
		t.Fatalf("Record after reopen: %v", err)
	}
	events, err := l2.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected the log to have survived reopen with 2 total events, got %d", len(events))
	}
}

func TestRecordRejectsEmptyAction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()
	if err := l.Record("", "actor", nil); err == nil {
		t.Fatalf("expected error for empty action")
	}
}
