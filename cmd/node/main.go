// Command node is heain-database's Stage A entry point: it wires
// accountstore + knowledgestore (both embedded BoltDB, the "inside"
// domain) and externalstore (PostgreSQL by default, the "outside" domain)
// behind one HTTP server, with a self-contained interim audit log pending
// heain-audit.
//
// heain-database is universal infrastructure (like heain-job/heain-access),
// not one of the 6 private CineNexus Pro content modules -- see
// design-notes/n-tier-generalization.md for the full design rationale.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	_ "github.com/mattn/go-sqlite3"    // registers the "sqlite3" database/sql driver (-external-dialect sqlite); previously only ever imported from test files, so the real binary could not actually start with this dialect until now

	"github.com/heainframework/heain-database/internal/accountstore"
	"github.com/heainframework/heain-database/internal/audit"
	"github.com/heainframework/heain-database/internal/externalstore"
	"github.com/heainframework/heain-database/internal/httpapi"
	"github.com/heainframework/heain-database/internal/knowledgestore"
)

func main() {
	var (
		listenAddr      = flag.String("listen-addr", ":8080", "HTTP listen address")
		accountsDBPath  = flag.String("accounts-db", "accounts.db", "path to the accountstore BoltDB file")
		knowledgeDBPath = flag.String("knowledge-db", "knowledge.db", "path to the knowledgestore BoltDB file")
		auditLogPath    = flag.String("audit-log", "audit.log", "path to the interim audit log file")
		externalDSN     = flag.String("external-dsn", "", "database/sql DSN for the externalstore (\"outside\") database")
		externalDialect = flag.String("external-dialect", "postgres", "externalstore SQL dialect: postgres or sqlite")

		tlsCertFile  = flag.String("tls-cert", "", "server TLS certificate file (enables HTTPS+mTLS when set)")
		tlsKeyFile   = flag.String("tls-key", "", "server TLS key file")
		tlsCAFile    = flag.String("tls-ca", "", "CA file used to verify client certificates for mTLS")
		adminCNsFlag = flag.String("admin-cns", "", "comma-separated list of client-certificate Common Names authorized for admin/destructive calls (purge, policy approve/reject)")
	)
	flag.Parse()

	accounts, err := accountstore.Open(*accountsDBPath)
	if err != nil {
		log.Fatalf("accountstore: %v", err)
	}
	defer accounts.Close()

	knowledge, err := knowledgestore.Open(*knowledgeDBPath)
	if err != nil {
		log.Fatalf("knowledgestore: %v", err)
	}
	defer knowledge.Close()

	auditLog, err := audit.Open(*auditLogPath)
	if err != nil {
		log.Fatalf("audit: %v", err)
	}
	defer auditLog.Close()

	driverName, dialect, err := resolveDialect(*externalDialect)
	if err != nil {
		log.Fatalf("external store: %v", err)
	}
	if *externalDSN == "" {
		log.Fatalf("external store: -external-dsn is required")
	}
	externalDB, err := sql.Open(driverName, *externalDSN)
	if err != nil {
		log.Fatalf("external store: open: %v", err)
	}
	defer externalDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	external, err := externalstore.Open(ctx, externalDB, dialect)
	cancel()
	if err != nil {
		log.Fatalf("external store: migrate: %v", err)
	}

	admin := adminAuthorizer(splitCSV(*adminCNsFlag))
	server := httpapi.NewServer(accounts, knowledge, external, auditLog, admin)

	httpServer := &http.Server{
		Addr:    *listenAddr,
		Handler: server,
	}

	useTLS := *tlsCertFile != "" && *tlsKeyFile != ""
	if useTLS {
		tlsConfig, err := buildServerTLSConfig(*tlsCertFile, *tlsKeyFile, *tlsCAFile)
		if err != nil {
			log.Fatalf("tls: %v", err)
		}
		httpServer.TLSConfig = tlsConfig
	}

	go func() {
		var err error
		if useTLS {
			log.Printf("heain-database listening (https+mtls) on %s", *listenAddr)
			err = httpServer.ListenAndServeTLS("", "") // certs come from TLSConfig
		} else {
			log.Printf("heain-database listening (plain http) on %s -- for local/dev use only", *listenAddr)
			err = httpServer.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

// resolveDialect maps the -external-dialect flag to the database/sql
// driver name to register under and externalstore's own Dialect value.
// This is the one place a new engine would be added.
func resolveDialect(name string) (driverName string, dialect externalstore.Dialect, err error) {
	switch name {
	case "postgres":
		return "pgx", externalstore.DialectPostgres, nil
	case "sqlite":
		return "sqlite3", externalstore.DialectSQLite, nil
	default:
		return "", "", fmt.Errorf("unknown dialect %q (expected \"postgres\" or \"sqlite\")", name)
	}
}

// adminAuthorizer builds an httpapi.AdminAuthorize that checks the calling
// client certificate's Common Name against allowedCNs -- the same mTLS
// CN/SAN admin-authorization pattern already used elsewhere in this
// project (/demote, /duty-profile). An empty allowedCNs list authorizes no
// one, which is the safe default: admin endpoints stay unreachable until
// an operator explicitly configures who may call them.
func adminAuthorizer(allowedCNs []string) httpapi.AdminAuthorize {
	allowed := make(map[string]bool, len(allowedCNs))
	for _, cn := range allowedCNs {
		allowed[cn] = true
	}
	return func(r *http.Request) bool {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			return false
		}
		cn := r.TLS.PeerCertificates[0].Subject.CommonName
		return allowed[cn]
	}
}

func buildServerTLSConfig(certFile, keyFile, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load server keypair: %w", err)
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("no certificates found in %s", caFile)
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
