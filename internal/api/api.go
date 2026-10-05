// Package api serves heain-database's capabilities over the heain-sdk
// direct-endpoint server (mTLS, app certificates, formal audit in
// heain-core). Changes that need an Approver -- policy writes and dataset
// purges -- go through heain-core's P5 gate and are applied by Resolve once
// approved.
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/heainframework/heain-sdk/heain"

	"github.com/heainframework/heain-database/internal/accountstore"
	"github.com/heainframework/heain-database/internal/externalstore"
	"github.com/heainframework/heain-database/internal/knowledgestore"
	"github.com/heainframework/heain-database/internal/requests"
)

// P5 action types heain-database proposes.
const (
	TypePolicyWrite  = "db.policy_write"
	TypeDatasetPurge = "db.dataset_purge"
)

// API ties the stores to the endpoints.
type API struct {
	App       *heain.App
	Accounts  *accountstore.Store
	Knowledge *knowledgestore.Store
	Requests  *requests.Store
	External  *externalstore.Store
	// MaxImportBytes bounds one import request body.
	MaxImportBytes int64
	Logf           func(string, ...any)
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, code, msg string) {
	reply(w, status, map[string]any{"error": map[string]any{"code": code, "message": msg, "retryable": status >= 500}})
}

func decode(w http.ResponseWriter, r *http.Request, max int64, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, max)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		fail(w, http.StatusBadRequest, "bad_request", err.Error())
		return false
	}
	return true
}

// Register adds every endpoint of the manifest to s.
func (a *API) Register(s *heain.Server) error {
	h := map[string]http.HandlerFunc{
		"PUT /v1/accounts/{id}":                 a.putAccount,
		"GET /v1/accounts/{id}":                 a.getAccount,
		"DELETE /v1/accounts/{id}":              a.deleteAccount,
		"GET /v1/accounts":                      a.listAccounts,
		"PUT /v1/knowledge/{ns}/{key}":          a.putLearned,
		"GET /v1/knowledge/{ns}/{key}":          a.getKnowledge,
		"PUT /v1/policy/{ns}/{key}":             a.proposePolicy,
		"GET /v1/requests/{action_id}":          a.requestStatus,
		"POST /v1/datasets/{name}/import":       a.importDataset,
		"GET /v1/datasets/{name}":               a.getDataset,
		"GET /v1/datasets/{name}/records/{key}": a.getRecord,
		"POST /v1/datasets/{name}/purge":        a.proposePurge,
	}
	for p, f := range h {
		if err := s.HandleFunc(p, f); err != nil {
			return err
		}
	}
	return nil
}

// ---- accounts (inside)

func (a *API) putAccount(w http.ResponseWriter, r *http.Request) {
	var acc accountstore.Account
	if !decode(w, r, 1<<20, &acc) {
		return
	}
	acc.ID = r.PathValue("id")
	saved, err := a.Accounts.Put(acc)
	if err != nil {
		fail(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	reply(w, http.StatusOK, saved)
}

func (a *API) getAccount(w http.ResponseWriter, r *http.Request) {
	acc, err := a.Accounts.Get(r.PathValue("id"))
	if errors.Is(err, accountstore.ErrNotFound) {
		fail(w, http.StatusNotFound, "not_found", "no such account")
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	reply(w, http.StatusOK, acc)
}

func (a *API) deleteAccount(w http.ResponseWriter, r *http.Request) {
	if err := a.Accounts.Delete(r.PathValue("id")); err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	reply(w, http.StatusOK, map[string]any{"status": "deleted"})
}

func (a *API) listAccounts(w http.ResponseWriter, r *http.Request) {
	l, err := a.Accounts.List()
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	reply(w, http.StatusOK, map[string]any{"accounts": l})
}

// ---- knowledge (inside): learned directly, policy through P5

func (a *API) putLearned(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Value string `json:"value"`
	}
	if !decode(w, r, 1<<20, &b) {
		return
	}
	e, err := a.Knowledge.PutLearned(r.PathValue("ns"), r.PathValue("key"), b.Value)
	if err != nil {
		fail(w, http.StatusConflict, "not_allowed", err.Error())
		return
	}
	reply(w, http.StatusOK, e)
}

func (a *API) getKnowledge(w http.ResponseWriter, r *http.Request) {
	e, err := a.Knowledge.Get(r.PathValue("ns"), r.PathValue("key"))
	if errors.Is(err, knowledgestore.ErrNotFound) {
		fail(w, http.StatusNotFound, "not_found", "no such entry")
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	reply(w, http.StatusOK, e)
}

func (a *API) proposePolicy(w http.ResponseWriter, r *http.Request) {
	var b struct {
		Value string `json:"value"`
	}
	if !decode(w, r, 1<<20, &b) {
		return
	}
	ns, key := r.PathValue("ns"), r.PathValue("key")
	sum := sha256.Sum256([]byte(b.Value))
	a.propose(w, r, heain.Proposal{Type: TypePolicyWrite, Category: heain.CategoryKnowledgeUpdate,
		Data: map[string]any{"namespace": ns, "key": key, "value_sha256": hex.EncodeToString(sum[:])}},
		requests.Request{Kind: requests.KindPolicyWrite, Namespace: ns, Key: key, Value: b.Value})
}

// propose sends p to P5, files req under its action id and applies it at
// once if P5 already allows it.
func (a *API) propose(w http.ResponseWriter, r *http.Request, p heain.Proposal, req requests.Request) {
	res, err := a.App.Propose(r.Context(), p)
	if err != nil {
		fail(w, http.StatusBadGateway, "p5_unavailable", err.Error())
		return
	}
	req.ActionID, req.State, req.CreatedAt = res.ActionID, requests.StateWaiting, time.Now().UTC()
	if err := a.Requests.Put(req); err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	if !res.Waiting() {
		a.settle(r.Context(), req, res)
	}
	got, _ := a.Requests.Get(req.ActionID)
	reply(w, http.StatusAccepted, map[string]any{"action_id": res.ActionID, "result": res.Result, "state": got.State})
}

func (a *API) requestStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("action_id")
	req, err := a.Requests.Get(id)
	if errors.Is(err, requests.ErrNotFound) {
		fail(w, http.StatusNotFound, "not_found", "no such request")
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	res, err := a.App.PolicyStatus(r.Context(), id)
	if err != nil {
		fail(w, http.StatusBadGateway, "p5_unavailable", err.Error())
		return
	}
	if req.State == requests.StateWaiting && !res.Waiting() {
		a.settle(r.Context(), req, res)
		req, _ = a.Requests.Get(id)
	}
	out := map[string]any{"action_id": id, "kind": req.Kind, "state": req.State, "result": res.Result, "created_at": req.CreatedAt}
	if req.Error != "" {
		out["error"] = req.Error
	}
	reply(w, http.StatusOK, out)
}

// ---- datasets (outside)

func (a *API) importDataset(w http.ResponseWriter, r *http.Request) {
	var b struct {
		ScopeKey          string                        `json:"scope_key"`
		SourceDescription string                        `json:"source_description"`
		LifecyclePolicy   externalstore.LifecyclePolicy `json:"lifecycle_policy"`
		Records           []externalstore.Record        `json:"records"`
	}
	if !decode(w, r, a.MaxImportBytes, &b) {
		return
	}
	ds, err := a.External.Import(r.Context(), r.PathValue("name"), b.ScopeKey, b.SourceDescription, b.LifecyclePolicy, b.Records)
	if err != nil {
		fail(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	reply(w, http.StatusOK, ds)
}

func (a *API) getDataset(w http.ResponseWriter, r *http.Request) {
	ds, err := a.External.GetDataset(r.Context(), r.PathValue("name"), r.URL.Query().Get("scope_key"))
	if errors.Is(err, externalstore.ErrNotFound) {
		fail(w, http.StatusNotFound, "not_found", "no such dataset in this scope")
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	reply(w, http.StatusOK, ds)
}

func (a *API) getRecord(w http.ResponseWriter, r *http.Request) {
	scope := r.URL.Query().Get("scope_key")
	if scope == "" {
		fail(w, http.StatusBadRequest, "bad_request", "scope_key query parameter is required")
		return
	}
	rec, err := a.External.GetRecord(r.Context(), r.PathValue("name"), scope, r.PathValue("key"))
	if errors.Is(err, externalstore.ErrNotFound) {
		fail(w, http.StatusNotFound, "not_found", "no such record in this dataset scope")
		return
	}
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	reply(w, http.StatusOK, rec)
}

func (a *API) proposePurge(w http.ResponseWriter, r *http.Request) {
	var b struct {
		ScopeKey string `json:"scope_key"`
	}
	if !decode(w, r, 1<<20, &b) {
		return
	}
	name := r.PathValue("name")
	if _, err := a.External.GetDataset(r.Context(), name, b.ScopeKey); err != nil {
		fail(w, http.StatusNotFound, "not_found", "no such dataset in this scope")
		return
	}
	a.propose(w, r, heain.Proposal{Type: TypeDatasetPurge, Category: heain.CategoryRetentionOverride,
		Data: map[string]any{"dataset": name, "scope_key": b.ScopeKey}},
		requests.Request{Kind: requests.KindDatasetPurge, Dataset: name, ScopeKey: b.ScopeKey})
}

// ---- P5 outcomes

// Resolve applies or drops the waiting requests as P5 decides them, every
// interval, until ctx ends.
func (a *API) Resolve(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		waiting, err := a.Requests.Waiting()
		if err != nil {
			a.logf("heain-database: reading waiting requests: %v", err)
			continue
		}
		for _, req := range waiting {
			res, err := a.App.PolicyStatus(ctx, req.ActionID)
			if err != nil || res.Waiting() {
				continue
			}
			a.settle(ctx, req, res)
		}
	}
}

func (a *API) settle(ctx context.Context, req requests.Request, res heain.PolicyResult) {
	if !res.Allowed() {
		_ = a.Requests.Decide(req, requests.StateDenied, "")
		a.logf("heain-database: %s %s denied by P5", req.Kind, req.ActionID)
		return
	}
	var err error
	capability, detail := "", map[string]any{"action_id": req.ActionID}
	switch req.Kind {
	case requests.KindPolicyWrite:
		capability, detail["namespace"], detail["key"] = "db.policy.propose", req.Namespace, req.Key
		_, err = a.Knowledge.PutPolicy(req.Namespace, req.Key, req.Value, req.ActionID)
	case requests.KindDatasetPurge:
		capability, detail["dataset"], detail["scope_key"] = "db.dataset.purge", req.Dataset, req.ScopeKey
		err = a.External.Purge(ctx, req.Dataset, req.ScopeKey)
	}
	if err != nil {
		_ = a.Requests.Decide(req, requests.StateFailed, err.Error())
		_ = a.App.Audit(ctx, capability, "error:apply_failed", detail)
		a.logf("heain-database: applying %s %s failed: %v", req.Kind, req.ActionID, err)
		return
	}
	_ = a.Requests.Decide(req, requests.StateApplied, "")
	_ = a.App.Audit(ctx, capability, "applied", detail)
	a.logf("heain-database: %s %s approved and applied", req.Kind, req.ActionID)
}

func (a *API) logf(f string, v ...any) {
	if a.Logf != nil {
		a.Logf(f, v...)
	}
}
