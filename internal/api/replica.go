package api

// Zone sync of inside data (Stage B, author decision 2026-10-08): every
// instance of heain-database in the zone serves its change log and pulls
// the others'. Records travel sealed under the zone key; nothing is opened
// to be copied, and peers are found with zone discovery only.

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/heainframework/heain-sdk/heain"

	"github.com/heainframework/heain-database/internal/replica"
)

// CapReplica is the capability of the change-log endpoint.
const CapReplica = "db.replica"

type changesPage struct {
	Epoch   string           `json:"epoch"`
	Head    uint64           `json:"head"`
	Changes []replica.Change `json:"changes"`
}

// changes serves GET /v1/replica/changes?since=&limit= to another
// heain-database instance only.
func (a *API) changes(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(heain.Caller(r.Context()), a.App.Manifest.App.ID+".") {
		fail(w, http.StatusForbidden, "forbidden", "only another instance of "+a.App.Manifest.App.ID+" may read the change log")
		return
	}
	if a.Replica == nil {
		fail(w, http.StatusServiceUnavailable, "zone_sync_off", "zone sync is off on this instance")
		return
	}
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	ep, head, cs, err := a.Replica.Changes(since, limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	reply(w, http.StatusOK, changesPage{Epoch: ep, Head: head, Changes: cs})
}

// PullZone pulls the change logs of the other instances in the zone every
// every, until ctx ends.
func (a *API) PullZone(ctx context.Context, every time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
		if n, peers, err := a.PullOnce(ctx); err != nil {
			a.logf("heain-database: zone sync: %v", err)
		} else if n > 0 {
			a.logf("heain-database: zone sync: %d change(s) applied from %d peer(s)", n, peers)
		}
	}
}

const pageSize = 500

// PullOnce reads every peer's new changes once; it returns how many were
// applied and how many peers were read.
func (a *API) PullOnce(ctx context.Context) (int, int, error) {
	insts, _, err := a.App.DiscoverZone(ctx, CapReplica, 1)
	if err != nil {
		return 0, 0, err
	}
	total, read := 0, 0
	for _, in := range insts {
		if in.AppID != a.App.Manifest.App.ID || in.EndpointBase == "" {
			continue
		}
		peer := in.Node + "/" + in.InstanceID
		ep, since := a.Replica.Last(peer)
		applied := 0
		for {
			var page changesPage
			_, err := a.App.Call(ctx, heain.CallSpec{App: in.AppID, Capability: CapReplica, Version: 1, Instance: in.InstanceID,
				Scope: heain.ScopeZone, Method: http.MethodGet,
				Path: "/v1/replica/changes?since=" + strconv.FormatUint(since, 10) + "&limit=" + strconv.Itoa(pageSize),
				Out:  &page, Timeout: 30 * time.Second})
			if err != nil {
				a.logf("heain-database: zone sync from %s: %v", peer, err)
				break
			}
			if page.Epoch == a.Replica.Epoch() {
				break // this very instance
			}
			if page.Epoch != ep && since != 0 {
				ep, since = page.Epoch, 0 // the peer's log was recreated: read it from the start
				continue
			}
			ep = page.Epoch
			n, err := a.Replica.Apply(peer, page.Epoch, page.Changes)
			applied += n
			if err != nil {
				a.logf("heain-database: zone sync from %s: %v", peer, err)
				break
			}
			if len(page.Changes) < pageSize {
				read++
				break
			}
			since = page.Changes[len(page.Changes)-1].Seq
		}
		if applied > 0 {
			_ = a.App.Audit(ctx, CapReplica, "applied", map[string]any{"peer": peer, "changes": applied})
		}
		total += applied
	}
	return total, read, nil
}
