// Package httpapi ties heain-database's stores together behind one HTTP
// surface: /accounts, /knowledge/{namespace}/{key}, /policy/{namespace}/{key}
// + /policy/approve/{request_id} + /policy/reject/{request_id} +
// /policy/pending, and /datasets/{name}/import + /datasets/{name}/records/{key}
// + /datasets/{name}/purge.
//
// Destructive/administrative calls (/datasets/{name}/purge, policy
// approve/reject) require AdminAuthorize to return true for the request --
// the same mTLS client-certificate CN/SAN admin-authorization pattern
// already used elsewhere in this project (/demote, /duty-profile). This
// package does not hardcode how that check is performed; it is injected,
// so a deployment can wire it to its own PKI however it needs to.
package httpapi

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/heainframework/heain-database/internal/accountstore"
	"github.com/heainframework/heain-database/internal/audit"
	"github.com/heainframework/heain-database/internal/externalstore"
	"github.com/heainframework/heain-database/internal/knowledgestore"
)

// AdminAuthorize reports whether r carries credentials authorized for a
// destructive/administrative call. The zero value (nil) in Server means
// "allow everything" -- intended only for local testing; cmd/node wires a
// real mTLS CN/SAN check for any real deployment.
type AdminAuthorize func(r *http.Request) bool

// Server implements http.Handler over heain-database's three stores plus
// its interim audit log.
type Server struct {
	Accounts  *accountstore.Store
	Knowledge *knowledgestore.Store
	External  *externalstore.Store
	Audit     *audit.Log
	Admin     AdminAuthorize

	mux *http.ServeMux
}

// NewServer builds a Server and registers all routes.
func NewServer(accounts *accountstore.Store, knowledge *knowledgestore.Store, external *externalstore.Store, auditLog *audit.Log, admin AdminAuthorize) *Server {
	s := &Server{Accounts: accounts, Knowledge: knowledge, External: external, Audit: auditLog, Admin: admin}
	s.mux = http.NewServeMux()
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/accounts", s.handleAccounts)
	s.mux.HandleFunc("/accounts/", s.handleAccountByID)
	s.mux.HandleFunc("/knowledge/", s.handleKnowledge)
	s.mux.HandleFunc("/policy/pending", s.handlePolicyPending)
	s.mux.HandleFunc("/policy/approve/", s.handlePolicyApprove)
	s.mux.HandleFunc("/policy/reject/", s.handlePolicyReject)
	s.mux.HandleFunc("/policy/", s.handlePolicyPropose)
	s.mux.HandleFunc("/datasets/", s.handleDatasets)
}

func (s *Server) isAdmin(r *http.Request) bool {
	if s.Admin == nil {
		return true
	}
	return s.Admin(r)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) record(action, actor string, fields map[string]string) {
	if s.Audit == nil {
		return
	}
	_ = s.Audit.Record(action, actor, fields)
}

func actorOf(r *http.Request) string {
	if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
		return r.TLS.PeerCertificates[0].Subject.CommonName
	}
	return "unknown"
}

// --- accounts ---

func (s *Server) handleAccounts(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost, http.MethodPut:
		var acc accountstore.Account
		if err := json.NewDecoder(r.Body).Decode(&acc); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		saved, err := s.Accounts.Put(acc)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		s.record("account.put", actorOf(r), map[string]string{"id": saved.ID})
		writeJSON(w, http.StatusOK, saved)
	case http.MethodGet:
		list, err := s.Accounts.List()
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, list)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) handleAccountByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/accounts/")
	if id == "" {
		writeErr(w, http.StatusBadRequest, "missing account id")
		return
	}
	switch r.Method {
	case http.MethodGet:
		acc, err := s.Accounts.Get(id)
		if err != nil {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, acc)
	case http.MethodDelete:
		if err := s.Accounts.Delete(id); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		s.record("account.delete", actorOf(r), map[string]string{"id": id})
		writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// --- knowledge (learned, direct write) ---

func (s *Server) handleKnowledge(w http.ResponseWriter, r *http.Request) {
	namespace, key, ok := splitTwo(strings.TrimPrefix(r.URL.Path, "/knowledge/"))
	if !ok {
		writeErr(w, http.StatusBadRequest, "expected /knowledge/{namespace}/{key}")
		return
	}
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		entry, err := s.Knowledge.PutLearned(namespace, key, body.Value)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		s.record("knowledge.learned.put", actorOf(r), map[string]string{"namespace": namespace, "key": key})
		writeJSON(w, http.StatusOK, entry)
	case http.MethodGet:
		entry, err := s.Knowledge.Get(namespace, key)
		if err != nil {
			writeErr(w, http.StatusNotFound, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, entry)
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// --- policy (queued write, requires approval) ---

func (s *Server) handlePolicyPropose(w http.ResponseWriter, r *http.Request) {
	namespace, key, ok := splitTwo(strings.TrimPrefix(r.URL.Path, "/policy/"))
	if !ok {
		writeErr(w, http.StatusBadRequest, "expected /policy/{namespace}/{key}")
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		RequestID string `json:"request_id"`
		Value     string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.RequestID == "" {
		writeErr(w, http.StatusBadRequest, "request_id is required")
		return
	}
	pw, err := s.Knowledge.ProposePolicyWrite(body.RequestID, namespace, key, body.Value)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.record("knowledge.policy.propose", actorOf(r), map[string]string{"namespace": namespace, "key": key, "request_id": body.RequestID})
	writeJSON(w, http.StatusAccepted, pw)
}

func (s *Server) handlePolicyPending(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	list, err := s.Knowledge.ListPending()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handlePolicyApprove(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimPrefix(r.URL.Path, "/policy/approve/")
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.isAdmin(r) {
		writeErr(w, http.StatusForbidden, "admin authorization required")
		return
	}
	entry, err := s.Knowledge.ApprovePolicyWrite(requestID)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	s.record("knowledge.policy.approve", actorOf(r), map[string]string{"request_id": requestID})
	writeJSON(w, http.StatusOK, entry)
}

func (s *Server) handlePolicyReject(w http.ResponseWriter, r *http.Request) {
	requestID := strings.TrimPrefix(r.URL.Path, "/policy/reject/")
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.isAdmin(r) {
		writeErr(w, http.StatusForbidden, "admin authorization required")
		return
	}
	if err := s.Knowledge.RejectPolicyWrite(requestID); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	s.record("knowledge.policy.reject", actorOf(r), map[string]string{"request_id": requestID})
	writeJSON(w, http.StatusOK, map[string]string{"status": "rejected"})
}

// --- datasets (external/"outside" store) ---

func (s *Server) handleDatasets(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/datasets/")
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	name, action := parts[0], parts[1]
	switch action {
	case "import":
		s.handleDatasetImport(w, r, name)
	case "purge":
		s.handleDatasetPurge(w, r, name)
	default:
		if strings.HasPrefix(action, "records/") {
			recordKey := strings.TrimPrefix(action, "records/")
			s.handleDatasetRecord(w, r, name, recordKey)
			return
		}
		writeErr(w, http.StatusNotFound, "not found")
	}
}

func (s *Server) handleDatasetImport(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var body struct {
		ScopeKey          string                        `json:"scope_key"`
		SourceDescription string                        `json:"source_description"`
		LifecyclePolicy   externalstore.LifecyclePolicy `json:"lifecycle_policy"`
		Records           []externalstore.Record        `json:"records"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	ds, err := s.External.Import(r.Context(), name, body.ScopeKey, body.SourceDescription, body.LifecyclePolicy, body.Records)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	s.record("dataset.import", actorOf(r), map[string]string{
		"name": name, "scope_key": body.ScopeKey, "version": itoa(ds.Version), "record_count": itoa(len(body.Records)),
	})
	writeJSON(w, http.StatusOK, ds)
}

func (s *Server) handleDatasetRecord(w http.ResponseWriter, r *http.Request, name, recordKey string) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	scopeKey := r.URL.Query().Get("scope_key")
	if scopeKey == "" {
		writeErr(w, http.StatusBadRequest, "scope_key query parameter is required")
		return
	}
	rec, err := s.External.GetRecord(r.Context(), name, scopeKey, recordKey)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Server) handleDatasetPurge(w http.ResponseWriter, r *http.Request, name string) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if !s.isAdmin(r) {
		writeErr(w, http.StatusForbidden, "admin authorization required")
		return
	}
	var body struct {
		ScopeKey string `json:"scope_key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.ScopeKey == "" {
		writeErr(w, http.StatusBadRequest, "scope_key is required")
		return
	}
	if err := s.External.Purge(r.Context(), name, body.ScopeKey); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.record("dataset.purge", actorOf(r), map[string]string{"name": name, "scope_key": body.ScopeKey})
	writeJSON(w, http.StatusOK, map[string]string{"status": "purged"})
}

// --- helpers ---

func splitTwo(path string) (a, b string, ok bool) {
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
