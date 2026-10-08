package api

// Zone sync of inside data (Stage B, author decision 2026-10-08): every
// instance of heain-database in the zone serves its change log and pulls
// the others'. Records travel sealed under the zone key; nothing is opened
// to be copied, and peers are found with zone discovery only.

import (
	"context"
	"net/http"
	"time"

	"github.com/heainframework/heain-sdk/zonesync"
)

// CapReplica is the capability of the change-log endpoint.
const CapReplica = "db.replica"

// changes serves GET /v1/replica/changes to another heain-database
// instance only (heain-sdk zonesync).
func (a *API) changes(w http.ResponseWriter, r *http.Request) {
	if a.Replica == nil {
		if !zonesync.SameApp(a.App, r) {
			fail(w, http.StatusForbidden, "forbidden", "only another instance of "+a.App.Manifest.App.ID+" may read the change log")
			return
		}
		fail(w, http.StatusServiceUnavailable, "zone_sync_off", "zone sync is off on this instance")
		return
	}
	zonesync.Handler(a.App, a.Replica)(w, r)
}

// Puller is the zone sync of this instance.
func (a *API) Puller() *zonesync.Puller {
	return &zonesync.Puller{App: a.App, Log: a.Replica, Capability: CapReplica, Path: "/v1/replica/changes", Logf: a.Logf,
		Audit: func(ctx context.Context, peer string, n int) {
			_ = a.App.Audit(ctx, CapReplica, "applied", map[string]any{"peer": peer, "changes": n})
		}}
}

// PullZone pulls the other instances' logs every every and compacts this
// instance's log every hour, until ctx ends.
func (a *API) PullZone(ctx context.Context, every time.Duration) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Hour):
			}
			if n, err := a.Replica.Compact(); err == nil && n > 0 {
				a.logf("heain-database: zone sync: %d replaced change(s) compacted", n)
			}
		}
	}()
	a.Puller().Run(ctx, every)
}
