// Package accountstore holds heain-database's "inside" account records: a
// reference record of who/what an account is (role + free-form metadata),
// never credential material. heain-access remains the system of record for
// verification; an Account here is looked up, not authenticated against.
//
// Storage: embedded BoltDB, local to this node -- the same convention used
// throughout heain-core/heain-job for the system's own operational data.
// Cross-zone sync of this data is Stage B, deliberately deferred (see
// design-notes/n-tier-generalization.md).
package accountstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrNotFound is returned when a lookup finds no matching record.
var ErrNotFound = errors.New("accountstore: not found")

var bucketAccounts = []byte("accounts")

// Account is a reference record for an account known to the system.
// MetadataKV is intentionally free-form (map[string]string) rather than a
// fixed schema, so this record shape fits many industries' own notion of
// "account" without a protocol change.
type Account struct {
	ID         string            `json:"id"`
	Role       string            `json:"role"`
	MetadataKV map[string]string `json:"metadata_kv,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// Store is a BoltDB-backed account store.
type Store struct {
	db *bolt.DB
}

// Open opens (creating if necessary) the BoltDB file at path and ensures
// the accounts bucket exists.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("accountstore: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketAccounts)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("accountstore: init bucket: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying BoltDB handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// Put creates or replaces the account identified by acc.ID.
func (s *Store) Put(acc Account) (Account, error) {
	if acc.ID == "" {
		return Account{}, errors.New("accountstore: id must not be empty")
	}
	now := time.Now().UTC()
	return acc, s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketAccounts)
		existing := b.Get([]byte(acc.ID))
		if existing != nil {
			var prev Account
			if err := json.Unmarshal(existing, &prev); err == nil {
				acc.CreatedAt = prev.CreatedAt
			}
		} else {
			acc.CreatedAt = now
		}
		acc.UpdatedAt = now
		data, err := json.Marshal(acc)
		if err != nil {
			return err
		}
		return b.Put([]byte(acc.ID), data)
	})
}

// Get returns the account with the given id, or ErrNotFound.
func (s *Store) Get(id string) (Account, error) {
	var acc Account
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketAccounts).Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		return json.Unmarshal(data, &acc)
	})
	return acc, err
}

// Delete removes the account with the given id. Deleting a non-existent id
// is not an error -- deletion is idempotent.
func (s *Store) Delete(id string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAccounts).Delete([]byte(id))
	})
}

// List returns every account currently stored, in key order.
func (s *Store) List() ([]Account, error) {
	var out []Account
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketAccounts)
		return b.ForEach(func(_, data []byte) error {
			var acc Account
			if err := json.Unmarshal(data, &acc); err != nil {
				return err
			}
			out = append(out, acc)
			return nil
		})
	})
	return out, err
}
