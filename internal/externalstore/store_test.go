package externalstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/heainframework/heain-database/internal/box"
)

type fakeKMS struct{ keys map[string][]byte }

func (f *fakeKMS) keysAPI() Keys {
	return Keys{
		Box: func(_ context.Context, n string) (*box.Box, error) {
			if f.keys[n] == nil {
				k := make([]byte, 32)
				_, _ = rand.Read(k)
				f.keys[n] = k
			}
			return box.New(f.keys[n])
		},
		Destroy: func(_ context.Context, n string) error { delete(f.keys, n); return nil },
	}
}

func TestExternalStore(t *testing.T) {
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "ext.db")
	db, err := sql.Open("sqlite3", p)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeKMS{keys: map[string][]byte{}}
	s, err := Open(ctx, db, DialectSQLite, f.keysAPI())
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{{RecordKey: "1100500012345", Value: json.RawMessage(`{"eligible":true,"name":"secret-name-somchai"}`)}}
	if _, err := s.Import(ctx, "voters", "district-9", "EC snapshot", LifecyclePolicy{OnComplete: "bogus"}, recs); err == nil {
		t.Fatal("bad lifecycle accepted")
	}
	ds, err := s.Import(ctx, "voters", "district-9", "EC snapshot", LifecyclePolicy{OnComplete: LifecycleFullPurge}, recs)
	if err != nil || ds.Version != 1 || ds.RecordCount != 1 {
		t.Fatalf("%+v %v", ds, err)
	}
	if _, err := s.Import(ctx, "voters", "district-9", "", LifecyclePolicy{}, []Record{{RecordKey: "a"}, {RecordKey: "a"}}); err == nil {
		t.Fatal("duplicate keys accepted")
	}
	r, err := s.GetRecord(ctx, "voters", "district-9", "1100500012345")
	if err != nil || !bytes.Contains(r.Value, []byte("secret-name-somchai")) {
		t.Fatalf("%+v %v", r, err)
	}
	if _, err := s.GetRecord(ctx, "voters", "district-8", "1100500012345"); !errors.Is(err, ErrNotFound) {
		t.Fatal("lookups are scoped")
	}
	if len(f.keys) != 1 {
		t.Fatal("a lookup in an unknown scope must not create a key")
	}
	ds, _ = s.Import(ctx, "voters", "district-9", "v2", LifecyclePolicy{}, []Record{{RecordKey: "x", Value: json.RawMessage(`1`)}})
	if ds.Version != 2 || ds.LifecyclePolicy.OnComplete != LifecycleRetain {
		t.Fatalf("%+v", ds)
	}
	if _, err := s.GetRecord(ctx, "voters", "district-9", "1100500012345"); !errors.Is(err, ErrNotFound) {
		t.Fatal("an import replaces the scope's snapshot")
	}
	raw, _ := os.ReadFile(p)
	for _, m := range []string{"1100500012345", "secret-name", "\"x\""} {
		if bytes.Contains(raw, []byte(m)) {
			t.Fatalf("plaintext %q in the SQL file", m)
		}
	}
	if err := s.Purge(ctx, "voters", "district-9"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetDataset(ctx, "voters", "district-9"); !errors.Is(err, ErrNotFound) || len(f.keys) != 0 {
		t.Fatal("purge removes the rows and destroys the key")
	}
	if err := s.Purge(ctx, "voters", "district-9"); err != nil {
		t.Fatal("purge is idempotent")
	}
}
