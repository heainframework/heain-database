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

	"github.com/heainframework/heain-sdk/heain"

	"github.com/heainframework/heain-database/internal/accountstore"
	"github.com/heainframework/heain-database/internal/api"
	"github.com/heainframework/heain-database/internal/box"
	"github.com/heainframework/heain-database/internal/externalstore"
	"github.com/heainframework/heain-database/internal/knowledgestore"
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
	accounts, err := accountstore.Open(filepath.Join(state, "accounts.db"), ib)
	if err != nil {
		log.Fatal(err)
	}
	defer accounts.Close()
	knowledge, err := knowledgestore.Open(filepath.Join(state, "knowledge.db"), ib)
	if err != nil {
		log.Fatal(err)
	}
	defer knowledge.Close()
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
		MaxImportBytes: *maxImportMB << 20, Logf: log.Printf}
	srv := app.NewServer()
	if err := a.Register(srv); err != nil {
		log.Fatal(err)
	}
	go a.Resolve(ctx, 2*time.Second)
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
