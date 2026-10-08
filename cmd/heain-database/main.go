// Command heain-database is the storage base app: "inside" data (accounts,
// AI/ML knowledge) in embedded BoltDB and "outside" reference datasets in a
// SQL database (PostgreSQL by default, SQLite by choice), everything sealed
// under data keys held by heain-core's KMS. It is configured through the
// heain-sdk HEAIN_* variables (heain.StartFromEnv) and runs however the
// operator likes: a plain process, a service unit, or a container.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // "pgx" database/sql driver
	_ "github.com/mattn/go-sqlite3"    // "sqlite3" database/sql driver

	"github.com/heainframework/heain-sdk/core"
	"github.com/heainframework/heain-sdk/heain"

	"github.com/heainframework/heain-database/internal/accountstore"
	"github.com/heainframework/heain-database/internal/api"
	"github.com/heainframework/heain-database/internal/box"
	"github.com/heainframework/heain-database/internal/externalstore"
	"github.com/heainframework/heain-database/internal/knowledgestore"
	"github.com/heainframework/heain-database/internal/replica"
	"github.com/heainframework/heain-database/internal/requests"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	dialect := flag.String("external-dialect", env("HEAIN_DB_DIALECT", "postgres"), "outside store SQL engine: postgres or sqlite (env HEAIN_DB_DIALECT)")
	dsn := flag.String("external-dsn", os.Getenv("HEAIN_DB_DSN"), "outside store DSN (env HEAIN_DB_DSN); sqlite default <state>/external.db")
	maxImportMB := flag.Int64("max-import-mb", 64, "largest dataset import request body, in MiB")
	zoneSync := flag.Bool("zone-sync", env("HEAIN_DB_ZONE_SYNC", "on") != "off", "keep accounts and knowledge the same on every node of the zone, sealed under the zone key (env HEAIN_DB_ZONE_SYNC=off to keep them on this node only)")
	zoneEvery := flag.Duration("zone-sync-every", 5*time.Second, "how often the other instances of the zone are read")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := heain.StartFromEnv(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REFUSED: %v\n", err)
		os.Exit(2)
	}
	log.Printf("heain-database: registered (%s), waiting for admission", app.Status())
	if err := app.WaitActive(ctx); err != nil {
		log.Fatal(err)
	}
	state := env("HEAIN_STATE_DIR", "/state")

	inside, err := app.DataKey(ctx, "inside")
	if err != nil {
		log.Fatalf("heain-database: data key from core: %v", err)
	}
	ib, err := box.New(inside)
	if err != nil {
		log.Fatal(err)
	}
	// Accounts and knowledge: under the zone key with zone sync (Stage B),
	// else under this node's key. Pending P5 requests stay on this node.
	sb := ib
	if *zoneSync {
		zk, err := zoneKey(ctx, app)
		if err != nil {
			log.Fatalf("heain-database: zone key from core: %v", err)
		}
		if sb, err = box.New(zk); err != nil {
			log.Fatal(err)
		}
	}
	accounts, err := accountstore.Open(filepath.Join(state, "accounts.db"), sb)
	if err != nil {
		log.Fatal(err)
	}
	defer accounts.Close()
	knowledge, err := knowledgestore.Open(filepath.Join(state, "knowledge.db"), sb)
	if err != nil {
		log.Fatal(err)
	}
	defer knowledge.Close()
	var rlog *replica.Log
	if *zoneSync {
		if rlog, err = startReplica(state, ib, accounts, knowledge); err != nil {
			log.Fatal(err)
		}
		defer rlog.Close()
	}
	reqs, err := requests.Open(filepath.Join(state, "requests.db"), ib)
	if err != nil {
		log.Fatal(err)
	}
	defer reqs.Close()

	driver, d := "pgx", externalstore.DialectPostgres
	switch *dialect {
	case "postgres":
	case "sqlite":
		driver, d = "sqlite3", externalstore.DialectSQLite
		if *dsn == "" {
			*dsn = filepath.Join(state, "external.db")
		}
	default:
		log.Fatalf("heain-database: unknown -external-dialect %q (postgres or sqlite)", *dialect)
	}
	if *dsn == "" {
		log.Fatal("heain-database: -external-dsn (HEAIN_DB_DSN) is required for postgres")
	}
	db, err := sql.Open(driver, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	keys := externalstore.Keys{
		Box: func(ctx context.Context, name string) (*box.Box, error) {
			k, err := app.DataKey(ctx, name)
			if err != nil {
				return nil, err
			}
			return box.New(k)
		},
		Destroy: app.DestroyDataKey,
	}
	octx, cancel := context.WithTimeout(ctx, 30*time.Second)
	external, err := externalstore.Open(octx, db, d, keys)
	cancel()
	if err != nil {
		log.Fatal(err)
	}

	a := &api.API{App: app, Accounts: accounts, Knowledge: knowledge, Requests: reqs, External: external,
		Replica: rlog, MaxImportBytes: *maxImportMB << 20, Logf: log.Printf}
	srv := app.NewServer()
	if err := a.Register(srv); err != nil {
		log.Fatal(err)
	}
	go a.Resolve(ctx, 2*time.Second)
	if rlog != nil {
		go a.PullZone(ctx, *zoneEvery)
	}
	l, err := net.Listen("tcp", heain.Listen(":19460"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("heain-database: active, serving on %s (outside store: %s)", l.Addr(), *dialect)
	if err := srv.Serve(ctx, l); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
	_ = app.Close(context.Background())
	log.Printf("heain-database: deregistered")
}

// zoneKey asks core for the zone key "inside", waiting while the zone's
// custodian cannot be reached and this node holds no copy yet.
func zoneKey(ctx context.Context, app *heain.App) ([]byte, error) {
	for {
		k, err := app.ZoneKey(ctx, "inside")
		if err == nil {
			return k, nil
		}
		var ce *core.Error
		if !errors.As(err, &ce) || !ce.Retryable {
			return nil, err
		}
		log.Printf("heain-database: zone key not available yet (%v); retrying", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// startReplica opens the zone change log. Records sealed under the node key
// (before zone sync) are sealed again under the zone key, and a new log
// starts with everything already held, so the other nodes get it.
func startReplica(state string, nodeBox *box.Box, accounts *accountstore.Store, knowledge *knowledgestore.Store) (*replica.Log, error) {
	ma, sa, err := accounts.Rekey(nodeBox)
	if err != nil {
		return nil, err
	}
	mk, sk, err := knowledge.Rekey(nodeBox)
	if err != nil {
		return nil, err
	}
	if ma+mk > 0 || sa+sk > 0 {
		log.Printf("heain-database: zone sync: %d account(s) and %d knowledge entr(ies) sealed again under the zone key (%d unreadable, left as they were)", ma, mk, sa+sk)
	}
	rlog, err := replica.Open(filepath.Join(state, "replica.db"))
	if err != nil {
		return nil, err
	}
	rlog.Register("account", accounts)
	rlog.Register("knowledge", knowledge)
	if _, head, _, err := rlog.Changes(0, 1); err == nil && head == 0 {
		for store, all := range map[string]func() (map[string][]byte, error){"account": accounts.All, "knowledge": knowledge.All} {
			m, err := all()
			if err != nil {
				return nil, err
			}
			for k, v := range m {
				if err := rlog.Record(store, k, v); err != nil {
					return nil, err
				}
			}
		}
	}
	accounts.OnWrite = func(k string, v []byte) error { return rlog.Record("account", k, v) }
	knowledge.OnWrite = func(k string, v []byte) error { return rlog.Record("knowledge", k, v) }
	log.Printf("heain-database: zone sync on (epoch %s): accounts and knowledge are kept on every node of the zone", rlog.Epoch())
	return rlog, nil
}
