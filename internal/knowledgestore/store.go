// Package knowledgestore holds heain-database's "inside" AI/ML metadata --
// the Knowledge Service concept (heain-sd's Language Store / Voice Store /
// Policy Registry), generalized to serve any module.
//
// Entries of Kind "learned" are written directly (retrained-model metadata,
// usage statistics). Entries of Kind "policy" take effect only after
// heain-core's P5 gate approves them: the api package proposes the write
// (KNOWLEDGE_UPDATE) and calls PutPolicy once an Approver approves. (Stage
// A had its own approve/reject queue; it is replaced by core's P5.)
//
// Storage: embedded BoltDB on this node (data class "knowledge",
// node-local), sealed and blinded like accountstore.
package knowledgestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/heainframework/heain-database/internal/box"
)

// ErrNotFound is returned when a lookup finds no matching record.
var ErrNotFound = errors.New("knowledgestore: not found")

// Kind distinguishes directly written entries from approved policy.
type Kind string

const (
	KindLearned Kind = "learned"
	KindPolicy  Kind = "policy"
)

var bucketEntries = []byte("entries")

// Entry is one namespaced key/value record.
type Entry struct {
	Namespace string    `json:"namespace"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Kind      Kind      `json:"kind"`
	Version   uint64    `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
	// ActionID is the P5 action that approved a policy entry.
	ActionID string `json:"action_id,omitempty"`
}

// Store is a BoltDB-backed, sealed knowledge store.
type Store struct {
	db  *bolt.DB
	box *box.Box
	// OnWrite, when set, is told every local write (blinded key, sealed
	// value). The zone replica logs it (Stage B).
	OnWrite func(key string, sealed []byte) error
}

// ApplyRaw stores a sealed entry as it is (nil = delete): a write that
// came from another node of the zone.
func (s *Store) ApplyRaw(k string, sealed []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if sealed == nil {
			return tx.Bucket(bucketEntries).Delete([]byte(k))
		}
		return tx.Bucket(bucketEntries).Put([]byte(k), sealed)
	})
}

// Rekey seals every entry again under this store's box, reading it with
// from (see accountstore.Rekey).
func (s *Store) Rekey(from *box.Box) (moved, skipped int, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketEntries)
		type kv struct{ k, v []byte }
		var all []kv
		_ = b.ForEach(func(k, v []byte) error {
			all = append(all, kv{append([]byte(nil), k...), append([]byte(nil), v...)})
			return nil
		})
		for _, e := range all {
			if _, err := s.box.Open(e.v, "knowledge/"+string(e.k)); err == nil {
				continue
			}
			p, err := from.Open(e.v, "knowledge/"+string(e.k))
			if err != nil {
				skipped++
				continue
			}
			var en Entry
			if err := json.Unmarshal(p, &en); err != nil {
				return err
			}
			nk := s.box.Index("knowledge", en.Namespace, en.Key)
			if err := b.Delete(e.k); err != nil {
				return err
			}
			if err := b.Put([]byte(nk), s.box.Seal(p, "knowledge/"+nk)); err != nil {
				return err
			}
			moved++
		}
		return nil
	})
	return
}

// All returns every blinded key and its sealed entry.
func (s *Store) All() (map[string][]byte, error) {
	out := map[string][]byte{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketEntries).ForEach(func(k, v []byte) error {
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
		return nil, fmt.Errorf("knowledgestore: open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketEntries)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("knowledgestore: init bucket: %w", err)
	}
	return &Store{db: db, box: b}, nil
}

// Close closes the underlying BoltDB handle.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) read(b *bolt.Bucket, k string) (Entry, error) {
	var e Entry
	v := b.Get([]byte(k))
	if v == nil {
		return e, ErrNotFound
	}
	p, err := s.box.Open(v, "knowledge/"+k)
	if err != nil {
		return e, err
	}
	return e, json.Unmarshal(p, &e)
}

// PutLearned writes a "learned" entry immediately.
func (s *Store) PutLearned(namespace, key, value string) (Entry, error) {
	return s.put(namespace, key, value, KindLearned, "")
}

// PutPolicy writes a "policy" entry; the caller has P5's approval actionID.
func (s *Store) PutPolicy(namespace, key, value, actionID string) (Entry, error) {
	if actionID == "" {
		return Entry{}, errors.New("knowledgestore: a policy entry needs the approving P5 action")
	}
	return s.put(namespace, key, value, KindPolicy, actionID)
}

// Get returns the entry in effect for namespace/key (for policy, the last
// approved value), or ErrNotFound.
func (s *Store) Get(namespace, key string) (Entry, error) {
	var e Entry
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		e, err = s.read(tx.Bucket(bucketEntries), s.box.Index("knowledge", namespace, key))
		return err
	})
	return e, err
}

func (s *Store) put(namespace, key, value string, kind Kind, actionID string) (Entry, error) {
	if namespace == "" || key == "" {
		return Entry{}, errors.New("knowledgestore: namespace and key must not be empty")
	}
	k := s.box.Index("knowledge", namespace, key)
	var e Entry
	var sealed []byte
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketEntries)
		version := uint64(1)
		if prev, err := s.read(b, k); err == nil {
			if prev.Kind == KindPolicy && kind == KindLearned {
				return fmt.Errorf("knowledgestore: %s/%s is policy; it changes only through P5", namespace, key)
			}
			version = prev.Version + 1
		}
		e = Entry{Namespace: namespace, Key: key, Value: value, Kind: kind, Version: version, UpdatedAt: time.Now().UTC(), ActionID: actionID}
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		sealed = s.box.Seal(data, "knowledge/"+k)
		return b.Put([]byte(k), sealed)
	})
	if err != nil || s.OnWrite == nil {
		return e, err
	}
	return e, s.OnWrite(k, sealed)
}
