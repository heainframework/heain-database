// Package accountstore holds heain-database's "inside" account records: a
// reference record of who/what an account is (role + free-form metadata),
// never credential material -- heain-access remains the system of record
// for verification.
//
// Storage: embedded BoltDB (data class "account", sovereignty zone-local
// since Stage B: with zone sync the same sealed records are kept on every
// node of the zone, see internal/replica). Every record is sealed under the app's data key
// from heain-core's KMS and filed under a blinded id (internal/box), so the
// file holds no plaintext id, role or metadata.
package accountstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/heainframework/heain-database/internal/box"
)

// ErrNotFound is returned when a lookup finds no matching record.
var ErrNotFound = errors.New("accountstore: not found")

var bucketAccounts = []byte("accounts")

// Account is a reference record for an account known to the system.
// MetadataKV is free-form so the record fits many industries' notion of
// "account" without a protocol change.
type Account struct {
	ID         string            `json:"id"`
	Role       string            `json:"role"`
	MetadataKV map[string]string `json:"metadata_kv,omitempty"`
	CreatedAt  time.Time         `json:"created_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// Store is a BoltDB-backed, sealed account store.
type Store struct {
	db  *bolt.DB
	box *box.Box
	// OnWrite, when set, is told every local write: the blinded key and the
	// sealed value (nil = deleted). The zone replica logs it (Stage B).
	OnWrite func(key string, sealed []byte) error
}

func (s *Store) wrote(k string, sealed []byte) error {
	if s.OnWrite == nil {
		return nil
	}
	return s.OnWrite(k, sealed)
}

// ApplyRaw stores a sealed value as it is under a blinded key (nil =
// delete): a write that came from another node of the zone.
func (s *Store) ApplyRaw(k string, sealed []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if sealed == nil {
			return tx.Bucket(bucketAccounts).Delete([]byte(k))
		}
		return tx.Bucket(bucketAccounts).Put([]byte(k), sealed)
	})
}

// Rekey seals every account again under this store's box, reading it with
// from (a node data key, before zone sync). It returns how many moved;
// an account that from cannot open is left as it is and counted in skipped.
func (s *Store) Rekey(from *box.Box) (moved, skipped int, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketAccounts)
		type kv struct{ k, v []byte }
		var all []kv
		_ = b.ForEach(func(k, v []byte) error {
			all = append(all, kv{append([]byte(nil), k...), append([]byte(nil), v...)})
			return nil
		})
		for _, e := range all {
			if _, err := s.box.Open(e.v, "account/"+string(e.k)); err == nil {
				continue // already under this box
			}
			p, err := from.Open(e.v, "account/"+string(e.k))
			if err != nil {
				skipped++
				continue
			}
			var acc Account
			if err := json.Unmarshal(p, &acc); err != nil {
				return err
			}
			nk := s.ix(acc.ID)
			if err := b.Delete(e.k); err != nil {
				return err
			}
			if err := b.Put([]byte(nk), s.box.Seal(p, "account/"+nk)); err != nil {
				return err
			}
			moved++
		}
		return nil
	})
	return
}

// All returns every blinded key and its sealed value (to log them once
// when zone sync starts).
func (s *Store) All() (map[string][]byte, error) {
	out := map[string][]byte{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAccounts).ForEach(func(k, v []byte) error {
			out[string(k)] = append([]byte(nil), v...)
			return nil
		})
	})
	return out, err
}

// Open opens (creating if necessary) the BoltDB file at path.
func Open(path string, b *box.Box) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("accountstore: open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketAccounts)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("accountstore: init bucket: %w", err)
	}
	return &Store{db: db, box: b}, nil
}

// Close closes the underlying BoltDB handle.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) ix(id string) string { return s.box.Index("account", id) }

func (s *Store) read(b *bolt.Bucket, k string) (Account, error) {
	var acc Account
	v := b.Get([]byte(k))
	if v == nil {
		return acc, ErrNotFound
	}
	p, err := s.box.Open(v, "account/"+k)
	if err != nil {
		return acc, err
	}
	return acc, json.Unmarshal(p, &acc)
}

// Put creates or replaces the account identified by acc.ID.
func (s *Store) Put(acc Account) (Account, error) {
	if acc.ID == "" {
		return Account{}, errors.New("accountstore: id must not be empty")
	}
	now := time.Now().UTC()
	k := s.ix(acc.ID)
	var sealed []byte
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketAccounts)
		if prev, err := s.read(b, k); err == nil {
			acc.CreatedAt = prev.CreatedAt
		} else {
			acc.CreatedAt = now
		}
		acc.UpdatedAt = now
		data, err := json.Marshal(acc)
		if err != nil {
			return err
		}
		sealed = s.box.Seal(data, "account/"+k)
		return b.Put([]byte(k), sealed)
	})
	if err != nil {
		return acc, err
	}
	return acc, s.wrote(k, sealed)
}

// Get returns the account with the given id, or ErrNotFound.
func (s *Store) Get(id string) (Account, error) {
	var acc Account
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		acc, err = s.read(tx.Bucket(bucketAccounts), s.ix(id))
		return err
	})
	return acc, err
}

// Delete removes the account with the given id (idempotent).
func (s *Store) Delete(id string) error {
	k := s.ix(id)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketAccounts).Delete([]byte(k))
	}); err != nil {
		return err
	}
	return s.wrote(k, nil)
}

// List returns every account, ordered by blinded id (not by id).
func (s *Store) List() ([]Account, error) {
	out := []Account{}
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketAccounts)
		return b.ForEach(func(k, _ []byte) error {
			acc, err := s.read(b, string(k))
			if err != nil {
				return err
			}
			out = append(out, acc)
			return nil
		})
	})
	return out, err
}
