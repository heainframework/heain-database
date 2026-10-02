package httpapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/heainframework/heain-database/internal/accountstore"
	"github.com/heainframework/heain-database/internal/audit"
	"github.com/heainframework/heain-database/internal/externalstore"
	"github.com/heainframework/heain-database/internal/knowledgestore"
)

func newTestServer(t *testing.T, admin AdminAuthorize) *Server {
	t.Helper()
	dir := t.TempDir()

	accounts, err := accountstore.Open(filepath.Join(dir, "accounts.db"))
	if err != nil {
		t.Fatalf("accountstore.Open: %v", err)
	}
	t.Cleanup(func() { _ = accounts.Close() })

	knowledge, err := knowledgestore.Open(filepath.Join(dir, "knowledge.db"))
	if err != nil {
		t.Fatalf("knowledgestore.Open: %v", err)
	}
	t.Cleanup(func() { _ = knowledge.Close() })

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	external, err := externalstore.Open(context.Background(), db, externalstore.DialectSQLite)
	if err != nil {
		t.Fatalf("externalstore.Open: %v", err)
	}

	auditLog, err := audit.Open(filepath.Join(dir, "audit.log"))
	if err != nil {
		t.Fatalf("audit.Open: %v", err)
	}
	t.Cleanup(func() { _ = auditLog.Close() })

	return NewServer(accounts, knowledge, external, auditLog, admin)
}

func doJSON(t *testing.T, s *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestAccountsRoundTrip(t *testing.T) {
	s := newTestServer(t, nil)

	rec := doJSON(t, s, http.MethodPost, "/accounts", accountstore.Account{ID: "acc-1", Role: "operator"})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(t, s, http.MethodGet, "/accounts/acc-1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var acc accountstore.Account
	if err := json.Unmarshal(rec.Body.Bytes(), &acc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if acc.Role != "operator" {
		t.Fatalf("unexpected role: %q", acc.Role)
	}
}

func TestKnowledgeLearnedDirectWrite(t *testing.T) {
	s := newTestServer(t, nil)

	rec := doJSON(t, s, http.MethodPut, "/knowledge/models/video-restoration", map[string]string{"value": "v3"})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(t, s, http.MethodGet, "/knowledge/models/video-restoration", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPolicyWriteRequiresApprovalOverHTTP(t *testing.T) {
	s := newTestServer(t, func(r *http.Request) bool { return true })

	rec := doJSON(t, s, http.MethodPut, "/policy/policy/max-retries", map[string]string{"request_id": "req-1", "value": "5"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	// Not yet in effect.
	rec = doJSON(t, s, http.MethodGet, "/knowledge/policy/max-retries", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 before approval, got %d", rec.Code)
	}

	rec = doJSON(t, s, http.MethodPost, "/policy/approve/req-1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on approve, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(t, s, http.MethodGet, "/knowledge/policy/max-retries", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 after approval, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestPolicyApproveDeniedWithoutAdmin(t *testing.T) {
	s := newTestServer(t, func(r *http.Request) bool { return false })

	doJSON(t, s, http.MethodPut, "/policy/policy/k", map[string]string{"request_id": "req-1", "value": "v"})

	rec := doJSON(t, s, http.MethodPost, "/policy/approve/req-1", nil)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 without admin authorization, got %d", rec.Code)
	}
}

func TestDatasetImportLookupPurgeOverHTTP(t *testing.T) {
	s := newTestServer(t, func(r *http.Request) bool { return true })

	importBody := map[string]any{
		"scope_key":          "th-zone-01",
		"source_description": "2026 Q4 snapshot",
		"lifecycle_policy":   map[string]string{"on_complete": "full_purge"},
		"records": []map[string]any{
			{"record_key": "citizen-001", "value": json.RawMessage(`{"eligible":true}`)},
		},
	}
	rec := doJSON(t, s, http.MethodPost, "/datasets/voter-eligibility/import", importBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on import, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(t, s, http.MethodGet, "/datasets/voter-eligibility/records/citizen-001?scope_key=th-zone-01", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on record lookup, got %d: %s", rec.Code, rec.Body.String())
	}

	// A different scope must not see this scope's record.
	rec = doJSON(t, s, http.MethodGet, "/datasets/voter-eligibility/records/citizen-001?scope_key=th-zone-99", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 across scopes, got %d", rec.Code)
	}

	rec = doJSON(t, s, http.MethodPost, "/datasets/voter-eligibility/purge", map[string]string{"scope_key": "th-zone-01"})
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on purge, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(t, s, http.MethodGet, "/datasets/voter-eligibility/records/citizen-001?scope_key=th-zone-01", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after purge, got %d", rec.Code)
	}
}

func TestDatasetPurgeDeniedWithoutAdmin(t *testing.T) {
	s := newTestServer(t, func(r *http.Request) bool { return false })
	rec := doJSON(t, s, http.MethodPost, "/datasets/voter-eligibility/purge", map[string]string{"scope_key": "th-zone-01"})
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 without admin authorization, got %d", rec.Code)
	}
}
