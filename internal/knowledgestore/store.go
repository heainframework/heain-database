// Package knowledgestore holds heain-database's "inside" AI/ML metadata --
// the Knowledge Service concept named in heain-sd's own spec (Language
// Store / Voice Store / Policy Registry) and the learned-knowledge-vs-policy
// split from heain-database's original design rationale, generalized to
// serve any module, not just heain-sd.
//
// Storage: embedded BoltDB, local to this node, matching accountstore.
//
// Entries of Kind "learned" are freely overwritable (e.g. retrained-model
// metadata, usage statistics). Entries of Kind "policy" instead go through
// a pending-approval step before taking effect: Stage A implements this as
// a simple internal approve/reject queue, NOT wired to heain-core's real P5
// Policy Boundary Check yet -- that integration is deferred, the same way
// heain-job's reassignment logic stays Layer-3-local rather than calling
// back into heain-core for every decision.
package knowledgestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrNotFound is returned when a lookup finds no matching record.
var ErrNotFound = errors.New("knowledgestore: not found")

// ErrPendingNotFound is returned when approving/rejecting an unknown
// pending request id.
var ErrPendingNotFound = errors.New("knowledgestore: pending request not found")

// Kind distinguishes freely-overwritable entries from entries that require
// approval before taking effect.
type Kind string

const (
	KindLearned Kind = "learned"
	KindPolicy  Kind = "policy"
)

var (
	bucketEntries = []byte("entries")
	bucketPending = []byte("pending")
)

// Entry is one namespaced key/value record.
type Entry struct {
	Namespace string    `json:"namespace"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Kind      Kind      `json:"kind"`
	Version   uint64    `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

func entryStoreKey(namespace, key string) []byte {
	return []byte(namespace + "\x00" + key)
}

// PendingWrite is a policy-kind write awaiting approval.
type PendingWrite struct {
	RequestID string    `json:"request_id"`
	Namespace string    `json:"namespace"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	CreatedAt time.Time `json:"created_at"`
}

// Store is a BoltDB-backed knowledge store.
type Store struct {
	db *bolt.DB
}

// Open opens (creating if necessary) the BoltDB file at path and ensures
// its buckets exist.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("knowledgestore: open %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketEntries); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(bucketPending)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("knowledgestore: init buckets: %w", err)
	}
	return &Store{db: db}, nil
}

// Close closes the underlying BoltDB handle.
func (s *Store) Close() error {
	return s.db.Close()
}

// PutLearned writes a "learned" entry immediately -- no approval step.
func (s *Store) PutLearned(namespace, key, value string) (Entry, error) {
	return s.putEntry(namespace, key, value, KindLearned)
}

// Get returns the current entry for namespace/key, or ErrNotFound. This
// returns whatever value is currently in effect -- for a policy-kind entry,
// that is the last *approved* value, never a value still pending approval.
func (s *Store) Get(namespace, key string) (Entry, error) {
	var e Entry
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketEntries).Get(entryStoreKey(namespace, key))
		if data == nil {
			return ErrNotFound
		}
		return json.Unmarshal(data, &e)
	})
	return e, err
}

// List returns every entry currently in effect.
func (s *Store) List() ([]Entry, error) {
	var out []Entry
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketEntries).ForEach(func(_, data []byte) error {
			var e Entry
			if err := json.Unmarshal(data, &e); err != nil {
				return err
			}
			out = append(out, e)
			return nil
		})
	})
	return out, err
}

func (s *Store) putEntry(namespace, key, value string, kind Kind) (Entry, error) {
	if namespace == "" || key == "" {
		return Entry{}, errors.New("knowledgestore: namespace and key must not be empty")
	}
	var e Entry
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketEntries)
		storeKey := entryStoreKey(namespace, key)
		var version uint64 = 1
		if existing := b.Get(storeKey); existing != nil {
			var prev Entry
			if err := json.Unmarshal(existing, &prev); err == nil {
				version = prev.Version + 1
			}
		}
		e = Entry{
			Namespace: namespace,
			Key:       key,
			Value:     value,
			Kind:      kind,
			Version:   version,
			UpdatedAt: time.Now().UTC(),
		}
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		return b.Put(storeKey, data)
	})
	return e, err
}

// ProposePolicyWrite queues a "policy" kind write for approval. It does
// NOT take effect until ApprovePolicyWrite is called with the returned
// request id. requestID must be supplied by the caller (the httpapi layer
// generates one) so that the proposal, approval, and audit record can all
// reference the same id.
func (s *Store) ProposePolicyWrite(requestID, namespace, key, value string) (PendingWrite, error) {
	if requestID == "" || namespace == "" || key == "" {
		return PendingWrite{}, errors.New("knowledgestore: request id, namespace, and key must not be empty")
	}
	pw := PendingWrite{
		RequestID: requestID,
		Namespace: namespace,
		Key:       key,
		Value:     value,
		CreatedAt: time.Now().UTC(),
	}
	return pw, s.db.Update(func(tx *bolt.Tx) error {
		data, err := json.Marshal(pw)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketPending).Put([]byte(requestID), data)
	})
}

// ApprovePolicyWrite applies a previously-proposed policy write and removes
// it from the pending queue.
func (s *Store) ApprovePolicyWrite(requestID string) (Entry, error) {
	var pw PendingWrite
	err := s.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucketPending).Get([]byte(requestID))
		if data == nil {
			return ErrPendingNotFound
		}
		return json.Unmarshal(data, &pw)
	})
	if err != nil {
		return Entry{}, err
	}
	e, err := s.putEntry(pw.Namespace, pw.Key, pw.Value, KindPolicy)
	if err != nil {
		return Entry{}, err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPending).Delete([]byte(requestID))
	})
	return e, err
}

// RejectPolicyWrite discards a previously-proposed policy write without
// applying it.
func (s *Store) RejectPolicyWrite(requestID string) error {
	var found bool
	err := s.db.View(func(tx *bolt.Tx) error {
		found = tx.Bucket(bucketPending).Get([]byte(requestID)) != nil
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return ErrPendingNotFound
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPending).Delete([]byte(requestID))
	})
}

// ListPending returns every policy write currently awaiting approval.
func (s *Store) ListPending() ([]PendingWrite, error) {
	var out []PendingWrite
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketPending).ForEach(func(_, data []byte) error {
			var pw PendingWrite
			if err := json.Unmarshal(data, &pw); err != nil {
				return err
			}
			out = append(out, pw)
			return nil
		})
	})
	return out, err
}
