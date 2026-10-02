package externalstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// openTestStore backs externalstore with SQLite -- the real, documented
// non-Postgres engine deployments are free to redirect to (see the package
// doc comment). Single-connection in-memory SQLite is used so the schema
// persists for the lifetime of the test without needing a real file.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	s, err := Open(context.Background(), db, DialectSQLite)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func rec(key, value string) Record {
	return Record{RecordKey: key, Value: json.RawMessage(value)}
}

func TestImportAndGetRecordScoped(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	ds, err := s.Import(ctx, "voter-eligibility", "th-zone-01", "2026 Q4 snapshot",
		LifecyclePolicy{OnComplete: LifecycleFullPurge},
		[]Record{rec("citizen-001", `{"eligible":true}`), rec("citizen-002", `{"eligible":false}`)})
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if ds.Version != 1 || ds.ScopeKey != "th-zone-01" {
		t.Fatalf("unexpected dataset: %+v", ds)
	}

	got, err := s.GetRecord(ctx, "voter-eligibility", "th-zone-01", "citizen-001")
	if err != nil {
		t.Fatalf("GetRecord: %v", err)
	}
	if string(got.Value) != `{"eligible":true}` {
		t.Fatalf("unexpected record value: %s", got.Value)
	}
}

func TestGetRecordIsScopeIsolated(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.Import(ctx, "voter-eligibility", "th-zone-01", "src", LifecyclePolicy{}, []Record{rec("citizen-001", `{"eligible":true}`)}); err != nil {
		t.Fatalf("Import zone-01: %v", err)
	}
	if _, err := s.Import(ctx, "voter-eligibility", "th-zone-02", "src", LifecyclePolicy{}, []Record{rec("citizen-999", `{"eligible":true}`)}); err != nil {
		t.Fatalf("Import zone-02: %v", err)
	}

	// citizen-001 exists in zone-01's own import, never in zone-02's --
	// this is the "never synced/queried cross-scope in Stage A" guarantee.
	if _, err := s.GetRecord(ctx, "voter-eligibility", "th-zone-02", "citizen-001"); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound across scopes, got %v", err)
	}
	if _, err := s.GetRecord(ctx, "voter-eligibility", "th-zone-01", "citizen-001"); err != nil {
		t.Fatalf("expected citizen-001 to be found within its own scope: %v", err)
	}
}

func TestReimportReplacesScopeAndBumpsVersion(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.Import(ctx, "voter-eligibility", "th-zone-01", "v1 src", LifecyclePolicy{}, []Record{rec("citizen-001", `{"eligible":true}`)}); err != nil {
		t.Fatalf("first Import: %v", err)
	}
	ds2, err := s.Import(ctx, "voter-eligibility", "th-zone-01", "v2 src", LifecyclePolicy{}, []Record{rec("citizen-002", `{"eligible":true}`)})
	if err != nil {
		t.Fatalf("second Import: %v", err)
	}
	if ds2.Version != 2 {
		t.Fatalf("expected version 2 after reimport, got %d", ds2.Version)
	}

	// The first import's record must be gone -- reimport replaces the
	// scope's snapshot wholesale, it does not merge.
	if _, err := s.GetRecord(ctx, "voter-eligibility", "th-zone-01", "citizen-001"); err != ErrNotFound {
		t.Fatalf("expected citizen-001 to be gone after reimport, got %v", err)
	}
	if _, err := s.GetRecord(ctx, "voter-eligibility", "th-zone-01", "citizen-002"); err != nil {
		t.Fatalf("expected citizen-002 from the new import to be present: %v", err)
	}
}

func TestPurgeRemovesDatasetAndRecordsForScopeOnly(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.Import(ctx, "voter-eligibility", "th-zone-01", "src", LifecyclePolicy{OnComplete: LifecycleFullPurge}, []Record{rec("citizen-001", `{}`)}); err != nil {
		t.Fatalf("Import zone-01: %v", err)
	}
	if _, err := s.Import(ctx, "voter-eligibility", "th-zone-02", "src", LifecyclePolicy{OnComplete: LifecycleRetain}, []Record{rec("citizen-999", `{}`)}); err != nil {
		t.Fatalf("Import zone-02: %v", err)
	}

	if err := s.Purge(ctx, "voter-eligibility", "th-zone-01"); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	if _, err := s.GetDataset(ctx, "voter-eligibility", "th-zone-01"); err != ErrNotFound {
		t.Fatalf("expected dataset metadata gone after purge, got %v", err)
	}
	if _, err := s.GetRecord(ctx, "voter-eligibility", "th-zone-01", "citizen-001"); err != ErrNotFound {
		t.Fatalf("expected record gone after purge, got %v", err)
	}

	// zone-02, a distinct scope with LifecycleRetain, must be untouched.
	if _, err := s.GetRecord(ctx, "voter-eligibility", "th-zone-02", "citizen-999"); err != nil {
		t.Fatalf("expected zone-02 to be unaffected by zone-01's purge: %v", err)
	}
}

func TestPurgeIsIdempotent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.Purge(ctx, "never-imported", "no-such-scope"); err != nil {
		t.Fatalf("expected purging a never-imported dataset/scope to be a no-op, got: %v", err)
	}
}

func TestImportDefaultsLifecycleToRetain(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	ds, err := s.Import(ctx, "ds", "scope-a", "src", LifecyclePolicy{}, nil)
	if err != nil {
		t.Fatalf("Import: %v", err)
	}
	if ds.LifecyclePolicy.OnComplete != LifecycleRetain {
		t.Fatalf("expected default lifecycle policy to be retain, got %q", ds.LifecyclePolicy.OnComplete)
	}
}

func TestImportRejectsEmptyNameOrScope(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if _, err := s.Import(ctx, "", "scope", "src", LifecyclePolicy{}, nil); err == nil {
		t.Fatalf("expected error for empty name")
	}
	if _, err := s.Import(ctx, "ds", "", "src", LifecyclePolicy{}, nil); err == nil {
		t.Fatalf("expected error for empty scope key")
	}
}
