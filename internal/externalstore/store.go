// Package externalstore holds heain-database's "outside" domain: bulk
// reference data brought in from outside the system for verification by
// other apps at runtime (the author's example: a voter-eligibility registry,
// queried to confirm a person may register). The system does not own this
// data; it only consults it.
//
// Storage: a real RDBMS through database/sql -- PostgreSQL by default,
// SQLite or another engine by deployment choice (decision 1). Data enters
// only by bulk import, one versioned snapshot per (dataset, scope_key)
// (decisions 2 and 5); its end-of-life action is set per import (decision
// 6). See design-notes/n-tier-generalization.md in heain-core.
//
// v2 (Step 4b-2, 2026-10-06): every (dataset, scope_key) has its own data
// key in heain-core's KMS. Values are sealed and record keys (e.g. citizen
// ids) are stored only blinded (HMAC), so the SQL database holds no
// plaintext reference data. Purge deletes the rows and destroys the key in
// core: copies in database backups are unreadable too (crypto-shred).
package externalstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/heainframework/heain-database/internal/box"
)

// ErrNotFound is returned when a lookup finds no matching dataset/record.
var ErrNotFound = errors.New("externalstore: not found")

// LifecycleAction is a dataset's configured end-of-life behavior, set per
// import (decision 6).
type LifecycleAction string

const (
	// LifecycleFullPurge: full, irreversible deletion (rows and key) once
	// the job/event ends, requested explicitly and approved through P5.
	LifecycleFullPurge LifecycleAction = "full_purge"
	// LifecycleRetain keeps the dataset after the job/event (the default).
	LifecycleRetain LifecycleAction = "retain"
	// LifecycleArchive is a named value whose behavior is not decided yet.
	LifecycleArchive LifecycleAction = "archive"
)

// LifecyclePolicy is a dataset's configured end-of-life behavior.
type LifecyclePolicy struct {
	OnComplete LifecycleAction `json:"on_complete"`
}

// Dataset is one imported external-data snapshot, scoped to ScopeKey (a
// free-form, operator-chosen partition: a zone, a district code, a
// customer id, a time window).
type Dataset struct {
	Name              string          `json:"name"`
	Version           int             `json:"version"`
	ScopeKey          string          `json:"scope_key"`
	ImportedAt        time.Time       `json:"imported_at"`
	SourceDescription string          `json:"source_description"`
	LifecyclePolicy   LifecyclePolicy `json:"lifecycle_policy"`
	RecordCount       int             `json:"record_count"`
}

// Record is one imported reference record; Value is opaque JSON.
type Record struct {
	RecordKey string          `json:"record_key"`
	Value     json.RawMessage `json:"value"`
}

// Dialect is the one portability difference: placeholder syntax.
type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

// Keys gives the Box of one (dataset, scope) data key, by its key name;
// Destroy crypto-shreds that key in heain-core.
type Keys struct {
	Box     func(ctx context.Context, keyName string) (*box.Box, error)
	Destroy func(ctx context.Context, keyName string) error
}

// KeyName is the data key name of (name, scopeKey) in heain-core's KMS.
func KeyName(name, scopeKey string) string {
	h := sha256.Sum256([]byte(name + "\x00" + scopeKey))
	return "ds-" + hex.EncodeToString(h[:20])
}

// Store is a database/sql-backed external-reference store.
type Store struct {
	db      *sql.DB
	dialect Dialect
	keys    Keys
}

// Open wires a Store on an opened *sql.DB and ensures the schema.
func Open(ctx context.Context, db *sql.DB, dialect Dialect, keys Keys) (*Store, error) {
	s := &Store{db: db, dialect: dialect, keys: keys}
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS hdb_datasets (
			name TEXT NOT NULL,
			scope_key TEXT NOT NULL,
			version INTEGER NOT NULL,
			imported_at TEXT NOT NULL,
			source_description TEXT NOT NULL,
			lifecycle_on_complete TEXT NOT NULL,
			record_count INTEGER NOT NULL,
			PRIMARY KEY (name, scope_key)
		)`,
		`CREATE TABLE IF NOT EXISTS hdb_records (
			dataset_name TEXT NOT NULL,
			scope_key TEXT NOT NULL,
			record_ix TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY (dataset_name, scope_key, record_ix)
		)`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("externalstore: migrate: %w", err)
		}
	}
	return s, nil
}

func (s *Store) rebind(q string) string {
	if s.dialect != DialectPostgres {
		return q
	}
	var b strings.Builder
	n := 0
	for _, r := range q {
		if r == '?' {
			n++
			b.WriteString("$" + strconv.Itoa(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func aad(name, scope, ix string) string { return "record/" + name + "\x00" + scope + "\x00" + ix }

// Import replaces the active snapshot for (name, scopeKey) with a new
// version, in one transaction (the only update path, deliberately).
func (s *Store) Import(ctx context.Context, name, scopeKey, sourceDescription string, policy LifecyclePolicy, records []Record) (Dataset, error) {
	if name == "" || scopeKey == "" {
		return Dataset{}, errors.New("externalstore: name and scope key must not be empty")
	}
	switch policy.OnComplete {
	case "":
		policy.OnComplete = LifecycleRetain
	case LifecycleRetain, LifecycleFullPurge, LifecycleArchive:
	default:
		return Dataset{}, fmt.Errorf("externalstore: lifecycle on_complete must be retain, full_purge or archive")
	}
	b, err := s.keys.Box(ctx, KeyName(name, scopeKey))
	if err != nil {
		return Dataset{}, fmt.Errorf("externalstore: data key: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Dataset{}, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed
	prev := 0
	switch err := tx.QueryRowContext(ctx, s.rebind(`SELECT version FROM hdb_datasets WHERE name = ? AND scope_key = ?`), name, scopeKey).Scan(&prev); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return Dataset{}, err
	}
	ds := Dataset{Name: name, Version: prev + 1, ScopeKey: scopeKey, ImportedAt: time.Now().UTC(), SourceDescription: sourceDescription,
		LifecyclePolicy: policy, RecordCount: len(records)}
	if _, err := tx.ExecContext(ctx, s.rebind(`DELETE FROM hdb_records WHERE dataset_name = ? AND scope_key = ?`), name, scopeKey); err != nil {
		return Dataset{}, err
	}
	if _, err := tx.ExecContext(ctx, s.rebind(`
		INSERT INTO hdb_datasets (name, scope_key, version, imported_at, source_description, lifecycle_on_complete, record_count)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (name, scope_key) DO UPDATE SET
			version = excluded.version, imported_at = excluded.imported_at, source_description = excluded.source_description,
			lifecycle_on_complete = excluded.lifecycle_on_complete, record_count = excluded.record_count`),
		name, scopeKey, ds.Version, ds.ImportedAt.Format(time.RFC3339Nano), sourceDescription, string(policy.OnComplete), ds.RecordCount); err != nil {
		return Dataset{}, err
	}
	ins := s.rebind(`INSERT INTO hdb_records (dataset_name, scope_key, record_ix, value) VALUES (?, ?, ?, ?)`)
	seen := map[string]bool{}
	for _, rec := range records {
		if rec.RecordKey == "" {
			return Dataset{}, errors.New("externalstore: record key must not be empty")
		}
		if seen[rec.RecordKey] {
			return Dataset{}, errors.New("externalstore: duplicate record key in one import")
		}
		seen[rec.RecordKey] = true
		ix := b.Index("record", rec.RecordKey)
		plain, _ := json.Marshal(rec)
		if _, err := tx.ExecContext(ctx, ins, name, scopeKey, ix, base64.StdEncoding.EncodeToString(b.Seal(plain, aad(name, scopeKey, ix)))); err != nil {
			return Dataset{}, err
		}
	}
	return ds, tx.Commit()
}

// GetDataset returns the active dataset metadata, or ErrNotFound.
func (s *Store) GetDataset(ctx context.Context, name, scopeKey string) (Dataset, error) {
	var ds Dataset
	var at, oc string
	err := s.db.QueryRowContext(ctx, s.rebind(`SELECT name, scope_key, version, imported_at, source_description, lifecycle_on_complete, record_count
		FROM hdb_datasets WHERE name = ? AND scope_key = ?`), name, scopeKey).Scan(&ds.Name, &ds.ScopeKey, &ds.Version, &at, &ds.SourceDescription, &oc, &ds.RecordCount)
	if errors.Is(err, sql.ErrNoRows) {
		return Dataset{}, ErrNotFound
	}
	if err != nil {
		return Dataset{}, err
	}
	ds.ImportedAt, _ = time.Parse(time.RFC3339Nano, at)
	ds.LifecyclePolicy = LifecyclePolicy{OnComplete: LifecycleAction(oc)}
	return ds, nil
}

// GetRecord looks up one record in (name, scopeKey)'s active snapshot.
// There is no cross-scope query, by design.
func (s *Store) GetRecord(ctx context.Context, name, scopeKey, recordKey string) (Record, error) {
	if _, err := s.GetDataset(ctx, name, scopeKey); err != nil {
		return Record{}, err // never create a key for a dataset that does not exist
	}
	b, err := s.keys.Box(ctx, KeyName(name, scopeKey))
	if err != nil {
		return Record{}, err
	}
	ix := b.Index("record", recordKey)
	var v string
	err = s.db.QueryRowContext(ctx, s.rebind(`SELECT value FROM hdb_records WHERE dataset_name = ? AND scope_key = ? AND record_ix = ?`),
		name, scopeKey, ix).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, err
	}
	sealed, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		return Record{}, err
	}
	p, err := b.Open(sealed, aad(name, scopeKey, ix))
	if err != nil {
		return Record{}, err
	}
	var rec Record
	return rec, json.Unmarshal(p, &rec)
}

// Purge deletes a dataset scope -- its metadata and every record -- and
// destroys its data key in heain-core, so any copy left in a backup cannot
// be read. Idempotent. The api package calls it only after P5 approval.
func (s *Store) Purge(ctx context.Context, name, scopeKey string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed
	if _, err := tx.ExecContext(ctx, s.rebind(`DELETE FROM hdb_records WHERE dataset_name = ? AND scope_key = ?`), name, scopeKey); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, s.rebind(`DELETE FROM hdb_datasets WHERE name = ? AND scope_key = ?`), name, scopeKey); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.keys.Destroy(ctx, KeyName(name, scopeKey))
}
