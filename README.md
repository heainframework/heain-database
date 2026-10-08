# heain-database

The storage base app of the heain framework: **"inside"** data — the system's own accounts and AI/ML knowledge — and **"outside"** reference datasets brought in from elsewhere for verification (for example a voter-eligibility registry). Universal infrastructure, like heain-job and heain-access, usable in any industry.

**v2 (Step 4b-2, 2026-10-06)** is rebuilt on heain-sdk v1 and heain-core 1.3. The Stage A version (its own plain-HTTP API, admin by certificate CN, its own approval queue and audit log, plaintext storage) is kept as tag `legacy-v0`.

## What it does

| Domain | Store | Capabilities |
|---|---|---|
| Accounts (who/what an account is; never credentials — heain-access verifies) | BoltDB | `db.account.read`, `db.account.write` |
| Knowledge: `learned` entries written directly | BoltDB | `db.knowledge.read`, `db.knowledge.write` |
| Knowledge: `policy` entries, only after heain-core's P5 Approver approves | BoltDB | `db.policy.propose`, `db.request.status` |
| Outside datasets: bulk snapshot import per `(name, scope_key)`, scoped lookups, purge through P5 | SQL (PostgreSQL default, SQLite by choice) | `db.dataset.import`, `db.dataset.lookup`, `db.dataset.purge` |

Endpoints (mTLS with app certificates, served by the heain-sdk server; every formal call is audited in heain-core):

```
PUT    /v1/accounts/{id}                 {"role", "metadata_kv"}
GET    /v1/accounts/{id}   GET /v1/accounts   DELETE /v1/accounts/{id}
PUT    /v1/knowledge/{ns}/{key}          {"value"}            learned
GET    /v1/knowledge/{ns}/{key}
PUT    /v1/policy/{ns}/{key}             {"value"}  -> 202 {"action_id", "result", "state"}   P5 KNOWLEDGE_UPDATE
GET    /v1/requests/{action_id}          -> {"kind", "state": waiting|applied|denied|failed, "result"}
POST   /v1/datasets/{name}/import        {"scope_key", "source_description", "lifecycle_policy": {"on_complete": retain|full_purge|archive}, "records": [{"record_key", "value"}]}
GET    /v1/datasets/{name}?scope_key=    GET /v1/datasets/{name}/records/{key}?scope_key=
POST   /v1/datasets/{name}/purge         {"scope_key"}  -> 202 {"action_id", ...}             P5 RETENTION_OVERRIDE
```

## Security model

- **Encrypted at rest, keys in core.** The app asks heain-core for data keys (`App.DataKey`, `POST /v1/app/keys`): one key `inside` for accounts, knowledge and pending requests, and one key per `(dataset, scope_key)`. Values are sealed (AES-256-GCM, bound to their place); lookup keys — account ids, record keys such as citizen ids — are stored only blinded (HMAC-SHA256). Neither the BoltDB files nor the SQL database hold a plaintext id, name or value.
- **Purge is crypto-shred.** An approved purge deletes the rows *and* destroys the dataset's key in core, so copies in database backups are unreadable too.
- **Approvals are core's P5.** Policy writes and purges wait for an Approver of the deployment; the Approver sees the namespace/key (and a hash of the value) or the dataset/scope, never the data.
- **Node-local.** All three data classes are `sovereignty: node-local`: heain-core admits heain-database only from its own node (loopback or `-app-local-cidrs`) and never moves its work elsewhere (spec 05 C9). Outside data is never synced across scopes; inside-data sync between nodes stays Stage B.
- **Offline.** Reads, account/knowledge writes and imports keep working while the node is standalone (core is local); policy writes and purges need P5.

## Running it

Any way you like — a plain process, a service unit, a container. It is configured through the heain-sdk `HEAIN_*` variables (`heain.StartFromEnv`) plus:

| Flag / variable | Default | |
|---|---|---|
| `-external-dialect` / `HEAIN_DB_DIALECT` | `postgres` | `postgres` or `sqlite` |
| `-external-dsn` / `HEAIN_DB_DSN` | — (sqlite: `<state>/external.db`) | e.g. `postgres://user@host:5432/db?sslmode=disable` |
| `-max-import-mb` | 64 | largest import request |
| `HEAIN_LISTEN` | `:19460` | where the endpoints are served |

Tests: `go test ./...`; live `bash scripts/live_4b.sh` (needs `~/heain-core`, `~/heain-sdk` and PostgreSQL server binaries; it starts and removes a throwaway cluster on port 15432); conformance `heain-conformance run --app .` (SQLite).

Design history: heain-core `design-notes/n-tier-generalization.md` ("heain-database: inside/outside split", decisions 1–6) and `docs/progress-log.md` (Step 4b).

## Stage B-1b: inside data on every node of the zone (2.1, 2026-10-08)

The author decided (2026-10-08): inside data syncs inside a zone, ciphertext only. This replaces "inside-data sync between nodes stays Stage B" above.

- **Zone key.** Accounts and knowledge are sealed under the zone key `inside` (heain-sdk `App.ZoneKey`; core keeps it on every node of the zone). Their data classes are now `zone-local`. Outside datasets stay `node-local` and are never synced; pending P5 requests stay on the node that proposed them.
- **Change logs.** Every write is recorded in `<state>/replica.db`. Each instance reads the logs of the other instances of the zone (`GET /v1/replica/changes`, capability `db.replica`, found with zone discovery, called over mTLS) every `-zone-sync-every` (5 s) and applies what is newer: the later write wins, then the origin. Records are copied as they are, sealed, never opened. Applied changes are logged again, so a node that joins later, or was cut off, also gets what came from a node that is gone. Only another heain-database instance may read a change log (403 otherwise). Applied batches are audited in core (`db.replica`, counts only).
- **Cut off.** A node cut off from its Master keeps reading and writing (its copy of the zone key); its writes reach the others when it is back.
- **Upgrade.** On the first start with zone sync, records sealed under the node key are sealed again under the zone key, and a new change log starts with everything already held. `-zone-sync=false` (`HEAIN_DB_ZONE_SYNC=off`) keeps the old behaviour: everything on this node, under the node key.
- **Known limits:** a write and its log entry are two files: a crash between them leaves that write unsent until the record is written again. Two nodes cut off from each other that both write one record keep the later write when they meet; the other is lost (last writer wins).

Tests: `scripts/live_b1b.sh` (Master + farm Worker, one instance on each).
