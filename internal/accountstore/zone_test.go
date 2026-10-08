package accountstore

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/heainframework/heain-database/internal/box"
)

func mkbox(t *testing.T, b byte) *box.Box {
	bx, err := box.New(bytes.Repeat([]byte{b}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return bx
}

func TestRekeyAndReplicaHooks(t *testing.T) {
	dir := t.TempDir()
	node, zone := mkbox(t, 1), mkbox(t, 2)
	s, err := Open(filepath.Join(dir, "a.db"), node)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.Put(Account{ID: "citizen-1", Role: "voter"})
	s.Close()
	// reopen under the zone key: the old record moves
	z, _ := Open(filepath.Join(dir, "a.db"), zone)
	defer z.Close()
	if moved, skipped, err := z.Rekey(node); err != nil || moved != 1 || skipped != 0 {
		t.Fatalf("rekey: %d %d %v", moved, skipped, err)
	}
	if a, err := z.Get("citizen-1"); err != nil || a.Role != "voter" {
		t.Fatalf("after rekey: %v %v", a, err)
	}
	if moved, _, _ := z.Rekey(node); moved != 0 {
		t.Fatal("rekey must be idempotent")
	}
	// writes are told to the replica, sealed; another store applies them raw
	var got []string
	var sealed [][]byte
	z.OnWrite = func(k string, v []byte) error { got = append(got, k); sealed = append(sealed, v); return nil }
	_, _ = z.Put(Account{ID: "citizen-2", Role: "officer"})
	_ = z.Delete("citizen-1")
	if len(got) != 2 || sealed[0] == nil || sealed[1] != nil || bytes.Contains(sealed[0], []byte("officer")) {
		t.Fatalf("hooks: %v", got)
	}
	peer, _ := Open(filepath.Join(dir, "b.db"), zone)
	defer peer.Close()
	if err := peer.ApplyRaw(got[0], sealed[0]); err != nil {
		t.Fatal(err)
	}
	if a, err := peer.Get("citizen-2"); err != nil || a.Role != "officer" {
		t.Fatal("a raw copy opens on a peer with the same zone key")
	}
}
