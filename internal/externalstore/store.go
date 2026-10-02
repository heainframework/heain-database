// Package externalstore holds heain-database's "outside" domain: bulk
// reference data brought in from outside the system for verification/
// cross-checking by other modules at runtime (the author's own example:
// a population/voter-eligibility registry, queried to confirm a person's
// eligibility before allowing them to register). The system does not
// generate or own this data -- it only consults it.
//
// Storage: a real RDBMS, accessed through database/sql so the engine is a
// deployment choice, not a compile-time one. The default production engine
// is PostgreSQL (via a pgx-family driver, wired by cmd/node); this package
// itself imports no driver and speaks only database/sql + ANSI-portable
// SQL, so a deployment can redirect to SQLite or another engine with no
// schema-design cost -- confirmed explicitly for heain-database's "outside"
// domain (see design-notes/n-tier-generalization.md).
//
// External data enters only via bulk import, snapshot-style -- there is no
// continuous/incremental sync from the external source. Each import is one
// versioned snapshot, scoped to the operator-chosen ScopeKey it was
// imported for; verification lookups always query the currently-active
// snapshot for that scope.
package externalstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrNotFound is returned when a lookup finds no matching dataset/record.
var ErrNotFound = errors.New("externalstore: not found")

// LifecycleAction is the configured end-of-life behavior for a dataset,
// set per import -- never a module-wide constant. See decision 6 in
// design-notes/n-tier-generalization.md: "outside" datasets' end-of-life
// action is operator-configurable per import, not a single mandated
// behavior.
type LifecycleAction string

const (
	// LifecycleFullPurge is the recommended default for sensitive,
	// job/event-scoped reference data (e.g. the voter-eligibility case):
	// a full, unconditional, irreversible delete via Purge, invoked
	// explicitly once the job/event ends. It is never automatic.
	LifecycleFullPurge LifecycleAction = "full_purge"
	// LifecycleRetain leaves the dataset in place after the job/event
	// completes, to be queried again later or explicitly re-imported.
	LifecycleRetain LifecycleAction = "retain"
	// LifecycleArchive is carried as a distinct, named policy value for
	// Stage A's schema even though its own concrete behavior (e.g. moving
	// to colder storage vs. merely flagging as closed-but-kept) is not
	// decided yet -- deferred until a real need for it arises.
	LifecycleArchive LifecycleAction = "archive"
)

// LifecyclePolicy is a dataset's configured end-of-life behavior.
type LifecyclePolicy struct {
	OnComplete LifecycleAction `json:"on_complete"`
}

// Dataset is one imported external-data snapshot, scoped to ScopeKey.
//
// ScopeKey is a free-form, operator-chosen partition identifier -- a zone
// id, a province/district code, a customer id, a time window, or any other
// granularity that fits the deployment -- never a protocol-fixed "zone"
// enum. An import pulls in only the records matching its own ScopeKey, so
// a lookup stays fast against a small, relevant table rather than a large
// blob pulled in whole and filtered afterward.
type Dataset struct {
	Name              string          `json:"name"`
	Version           int             `json:"version"`
	ScopeKey          string          `json:"scope_key"`
	ImportedAt        time.Time       `json:"imported_at"`
	SourceDescription string          `json:"source_description"`
	LifecyclePolicy   LifecyclePolicy `json:"lifecycle_policy"`
}

// Record is one imported reference record, identified by its RecordKey
// within a dataset+scope (e.g. a citizen id within a voter-eligibility
// dataset for one electoral area). Value is an opaque JSON document --
// externalstore never interprets its shape; that is the calling module's
// (and the source dataset's) business.
type Record struct {
	RecordKey string          `json:"record_key"`
	Value     json.RawMessage `json:"value"`
}

// Store is a database/sql-backed external-reference store. It speaks only
// ANSI-portable SQL (ON CONFLICT upserts, no engine-specific types), so it
// runs unchanged against PostgreSQL or SQLite -- the engine and its driver
// are wired by the caller (cmd/node), never imported here.
type Store struct {
	db      *sql.DB
	dialect Dialect
}

// Dialect names the one portability difference externalstore must account
// for itself: parameter placeholder syntax ("?" vs "$1, $2, ...").
// Everything else (upsert via ON CONFLICT, column types) is written to
// work identically on both.
type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

// Open wires a Store on top of an already-opened *sql.DB (opened by the
// caller with whichever driver the deployment chose) and ensures the
// schema exists.
func Open(ctx context.Context, db *sql.DB, dialect Dialect) (*Store, error) {
	s := &Store{db: db, dialect: dialect}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("externalstore: migrate: %w", err)
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS datasets (
			name TEXT NOT NULL,
			scope_key TEXT NOT NULL,
			version INTEGER NOT NULL,
			imported_at TIMESTAMP NOT NULL,
			source_description TEXT NOT NULL,
			lifecycle_on_complete TEXT NOT NULL,
			PRIMARY KEY (name, scope_key)
		)`,
		`CREATE TABLE IF NOT EXISTS external_records (
			dataset_name TEXT NOT NULL,
			scope_key TEXT NOT NULL,
			record_key TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (dataset_name, scope_key, record_key)
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt, err)
		}
	}
	return nil
}

// rebind rewrites a "?"-placeholder query into the active dialect's own
// placeholder syntax. Writing every query with "?" internally, then
// rebinding once here, is what keeps the rest of this file dialect-free.
func (s *Store) rebind(query string) string {
	if s.dialect != DialectPostgres {
		return query
	}
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (s *Store) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.rebind(query), args...)
}

func (s *Store) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, s.rebind(query), args...)
}

func (s *Store) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, s.rebind(query), args...)
}

// Import replaces the currently-active snapshot for (name, scopeKey) with
// a new versioned one, in a single transaction: the dataset's own prior
// records for that scope are deleted, the new metadata row is upserted,
// and every record in the import is inserted. This is the only update
// path for Stage A -- there is no incremental/partial update, deliberately,
// so every import is one clear, auditable event.
func (s *Store) Import(ctx context.Context, name, scopeKey, sourceDescription string, policy LifecyclePolicy, records []Record) (Dataset, error) {
	if name == "" || scopeKey == "" {
		return Dataset{}, errors.New("externalstore: name and scope key must not be empty")
	}
	if policy.OnComplete == "" {
		policy.OnComplete = LifecycleRetain
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Dataset{}, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op if committed

	var prevVersion int
	row := tx.QueryRowContext(ctx, s.rebind(`SELECT version FROM datasets WHERE name = ? AND scope_key = ?`), name, scopeKey)
	switch err := row.Scan(&prevVersion); {
	case errors.Is(err, sql.ErrNoRows):
		prevVersion = 0
	case err != nil:
		return Dataset{}, err
	}
	ds := Dataset{
		Name:              name,
		Version:           prevVersion + 1,
		ScopeKey:          scopeKey,
		ImportedAt:        time.Now().UTC(),
		SourceDescription: sourceDescription,
		LifecyclePolicy:   policy,
	}

	if _, err := tx.ExecContext(ctx, s.rebind(`DELETE FROM external_records WHERE dataset_name = ? AND scope_key = ?`), name, scopeKey); err != nil {
		return Dataset{}, err
	}

	upsertDataset := s.rebind(`
		INSERT INTO datasets (name, scope_key, version, imported_at, source_description, lifecycle_on_complete)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (name, scope_key) DO UPDATE SET
			version = excluded.version,
			imported_at = excluded.imported_at,
			source_description = excluded.source_description,
			lifecycle_on_complete = excluded.lifecycle_on_complete`)
	if _, err := tx.ExecContext(ctx, upsertDataset, ds.Name, ds.ScopeKey, ds.Version, ds.ImportedAt, ds.SourceDescription, string(ds.LifecyclePolicy.OnComplete)); err != nil {
		return Dataset{}, err
	}

	insertRecord := s.rebind(`INSERT INTO external_records (dataset_name, scope_key, record_key, value) VALUES (?, ?, ?, ?)`)
	for _, rec := range records {
		if rec.RecordKey == "" {
			return Dataset{}, fmt.Errorf("externalstore: record key must not be empty")
		}
		if _, err := tx.ExecContext(ctx, insertRecord, name, scopeKey, rec.RecordKey, string(rec.Value)); err != nil {
			return Dataset{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return Dataset{}, err
	}
	return ds, nil
}

// GetDataset returns the currently-active dataset metadata for
// (name, scopeKey), or ErrNotFound.
func (s *Store) GetDataset(ctx context.Context, name, scopeKey string) (Dataset, error) {
	row := s.queryRow(ctx, `SELECT name, scope_key, version, imported_at, source_description, lifecycle_on_complete
		FROM datasets WHERE name = ? AND scope_key = ?`, name, scopeKey)
	var ds Dataset
	var onComplete string
	err := row.Scan(&ds.Name, &ds.ScopeKey, &ds.Version, &ds.ImportedAt, &ds.SourceDescription, &onComplete)
	if errors.Is(err, sql.ErrNoRows) {
		return Dataset{}, ErrNotFound
	}
	if err != nil {
		return Dataset{}, err
	}
	ds.LifecyclePolicy = LifecyclePolicy{OnComplete: LifecycleAction(onComplete)}
	return ds, nil
}

// GetRecord looks up one record within (name, scopeKey)'s currently-active
// snapshot. The lookup is always scoped -- there is no cross-scope query
// in Stage A, by design (deferred, not forgotten; see the design note).
func (s *Store) GetRecord(ctx context.Context, name, scopeKey, recordKey string) (Record, error) {
	row := s.queryRow(ctx, `SELECT record_key, value FROM external_records
		WHERE dataset_name = ? AND scope_key = ? AND record_key = ?`, name, scopeKey, recordKey)
	var rec Record
	var value string
	err := row.Scan(&rec.RecordKey, &value)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	rec.Value = json.RawMessage(value)
	return rec, nil
}

// Purge performs the full, unconditional, irreversible deletion of a
// dataset's scope: its metadata row and every one of its records. It is
// available regardless of the dataset's configured LifecyclePolicy -- an
// operator can always force-purge manually -- and it is the action Stage A
// actually performs when a full_purge-policy dataset's trigger fires.
// Purging a dataset/scope that does not exist is not an error: deletion is
// idempotent.
func (s *Store) Purge(ctx context.Context, name, scopeKey string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op if committed

	if _, err := tx.ExecContext(ctx, s.rebind(`DELETE FROM external_records WHERE dataset_name = ? AND scope_key = ?`), name, scopeKey); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, s.rebind(`DELETE FROM datasets WHERE name = ? AND scope_key = ?`), name, scopeKey); err != nil {
		return err
	}
	return tx.Commit()
}
