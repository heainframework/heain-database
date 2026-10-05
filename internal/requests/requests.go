// Package requests keeps heain-database's changes that wait for heain-core's
// P5 gate: policy writes to the knowledge store and dataset purges. Each is
// filed under its P5 action id and sealed (the pending value is data); the
// api package applies it when an Approver approves, or drops it when one
// denies.
package requests

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/heainframework/heain-database/internal/box"
)

// ErrNotFound: no request with this action id.
var ErrNotFound = errors.New("requests: not found")

// Kinds and states.
const (
	KindPolicyWrite  = "policy_write"
	KindDatasetPurge = "dataset_purge"

	StateWaiting = "waiting"
	StateApplied = "applied"
	StateDenied  = "denied"
	StateFailed  = "failed"
)

// Request is one change waiting for, or decided by, P5.
type Request struct {
	ActionID  string     `json:"action_id"`
	Kind      string     `json:"kind"`
	State     string     `json:"state"`
	Namespace string     `json:"namespace,omitempty"`
	Key       string     `json:"key,omitempty"`
	Value     string     `json:"value,omitempty"`
	Dataset   string     `json:"dataset,omitempty"`
	ScopeKey  string     `json:"scope_key,omitempty"`
	Error     string     `json:"error,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	DecidedAt *time.Time `json:"decided_at,omitempty"`
}

var bucket = []byte("requests")

// Store is the sealed request log.
type Store struct {
	db  *bolt.DB
	box *box.Box
}

// Open opens (creating if necessary) the BoltDB file at path.
func Open(path string, b *box.Box) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("requests: open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db, box: b}, nil
}

// Close closes the file.
func (s *Store) Close() error { return s.db.Close() }

// Put files r under r.ActionID.
func (s *Store) Put(r Request) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Put([]byte(r.ActionID), s.box.Seal(data, "request/"+r.ActionID))
	})
}

// Get reads one request.
func (s *Store) Get(actionID string) (Request, error) {
	var r Request
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucket).Get([]byte(actionID))
		if v == nil {
			return ErrNotFound
		}
		p, err := s.box.Open(v, "request/"+actionID)
		if err != nil {
			return err
		}
		return json.Unmarshal(p, &r)
	})
	return r, err
}

// Waiting lists the requests still waiting for P5.
func (s *Store) Waiting() ([]Request, error) {
	var out []Request
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).ForEach(func(k, v []byte) error {
			p, err := s.box.Open(v, "request/"+string(k))
			if err != nil {
				return err
			}
			var r Request
			if err := json.Unmarshal(p, &r); err != nil {
				return err
			}
			if r.State == StateWaiting {
				out = append(out, r)
			}
			return nil
		})
	})
	return out, err
}

// Decide records the outcome of r.
func (s *Store) Decide(r Request, state, errText string) error {
	now := time.Now().UTC()
	r.State, r.Error, r.DecidedAt = state, errText, &now
	if state != StateWaiting {
		r.Value = "" // the pending value is not kept once decided
	}
	return s.Put(r)
}
