// Package audit is heain-database's interim, self-contained audit log.
//
// heain-audit (the shared platform audit service named in the project's
// revised module roadmap) does not exist yet, so heain-database ships its
// own append-only log for Stage A, structurally mirroring heain-job's own
// internal/audit package. This is explicitly provisional: once heain-audit
// exists, heain-database's writes are expected to be re-pointed at it,
// retiring this package -- the same "ship the simple local version first,
// generalize once the shared service exists" pattern already used
// elsewhere in this project.
package audit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Event is one audit record. Fields is free-form so every call site (an
// import, a purge, a policy approval, an account write) can attach
// whatever detail is relevant without a schema change here.
type Event struct {
	Timestamp time.Time         `json:"timestamp"`
	Action    string            `json:"action"`
	Actor     string            `json:"actor,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// Log is an append-only, newline-delimited-JSON audit log backed by a
// single local file. Every write is flushed immediately -- this is a
// low-volume governance log, not a high-throughput data path.
type Log struct {
	mu   sync.Mutex
	file *os.File
}

// Open opens (creating if necessary) the audit log file at path for
// appending.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("audit: open %s: %w", path, err)
	}
	return &Log{file: f}, nil
}

// Close closes the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Close()
}

// Record appends one event. action and actor are required; fields is
// optional free-form detail.
func (l *Log) Record(action, actor string, fields map[string]string) error {
	if action == "" {
		return fmt.Errorf("audit: action must not be empty")
	}
	ev := Event{
		Timestamp: time.Now().UTC(),
		Action:    action,
		Actor:     actor,
		Fields:    fields,
	}
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.file.Write(data); err != nil {
		return fmt.Errorf("audit: write: %w", err)
	}
	return l.file.Sync()
}

// All reads and returns every event currently in the log, oldest first.
// Intended for small Stage A logs and tests, not a high-volume read path.
func (l *Log) All() ([]Event, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.file.Seek(0, 0); err != nil {
		return nil, err
	}
	defer l.file.Seek(0, 2) //nolint:errcheck // restore append position best-effort

	var out []Event
	scanner := bufio.NewScanner(l.file)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return nil, fmt.Errorf("audit: decode: %w", err)
		}
		out = append(out, ev)
	}
	return out, scanner.Err()
}
