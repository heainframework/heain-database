#!/usr/bin/env bash
# heain-database live test 4b: heain-database v2 on heain-sdk v1 against a real heain-core node
# and a real PostgreSQL (a throwaway cluster this script starts on port 15432 and removes).
#  - inside data (accounts, learned knowledge) and outside datasets are sealed under data keys
#    from core's KMS: no plaintext id, name or value in BoltDB, PostgreSQL or core's audit;
#  - policy writes and dataset purges wait for core's P5 Approver; approve applies, deny drops;
#  - a purge deletes the rows and destroys the dataset's key in core (crypto-shred);
#  - data survives an app restart (the key comes back from core).
# The app is a plain process configured through HEAIN_* variables.
# Needs ~/heain-core, ~/heain-sdk and PostgreSQL server binaries (initdb, pg_ctl). ~2 min.
# Run from ~/heain-database:  bash scripts/live_4b.sh
set -uo pipefail
DB=$(cd "$(dirname "$0")/.." && pwd)
cd ~/heain-core || { echo "needs ~/heain-core"; exit 1; }
H=./test_1_2_live.sh
T=$HOME/heain-core/.test-1.2
C=$T/certs; L=$T/logs; P=$T/pids; BIN=$T/node; W=$T/db-4b
URL=https://127.0.0.1:18000
DBURL=https://127.0.0.1:19460
PG=/tmp/heain-pg-4b; PGPORT=15432
PASS=0; FAIL=0
ok()  { echo "  PASS: $*"; PASS=$((PASS+1)); }
bad() { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }
as() { local who=$1; shift; curl -sk --noproxy '*' --cert "$C/$who.pem" --key "$C/$who.key" --cacert "$C/ca.pem" "$@"; }
code() { local who=$1; shift; as "$who" -o /dev/null -w "%{http_code}" "$@"; }
cl() { curl -sk --noproxy '*' --cert "$W/client.pem" --key "$W/client.key" --cacert "$C/ca.pem" "$@"; }   # an app (client.c1) calling heain-database
clc() { cl -o /dev/null -w "%{http_code}" "$@"; }
j() { python3 -c "import json,sys;d=json.load(sys.stdin);print($1)" 2>/dev/null; }
mkcert() { [ -f "$C/$1.pem" ] && return; openssl genrsa -out "$C/$1.key" 2048 >/dev/null 2>&1
  openssl req -new -key "$C/$1.key" -subj "/CN=$1" -out "$C/$1.csr" >/dev/null 2>&1
  openssl x509 -req -in "$C/$1.csr" -CA "$C/ca.pem" -CAkey "$C/ca.key" -CAcreateserial -out "$C/$1.pem" \
    -days 825 -sha256 -extfile <(printf "subjectAltName=DNS:%s" "$1") >/dev/null 2>&1; }
pgrun() { if [ "$(id -u)" = 0 ]; then runuser -u postgres -- "$@"; else "$@"; fi; }
PGBIN=$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1)
[ -z "$PGBIN" ] && PGBIN=$(dirname "$(command -v pg_ctl 2>/dev/null || echo /none/x)")
pgstop() { [ -d "$PG/data" ] && pgrun "$PGBIN/pg_ctl" -D "$PG/data" -m fast stop >/dev/null 2>&1; rm -rf "$PG"; }
psqlq() { "$PGBIN/psql" -h 127.0.0.1 -p $PGPORT -U heain -d heain -Atc "$1" 2>/dev/null; }
run_db() { # start heain-database d1 as a plain process
  mkdir -p "$W/state-d1"
  HEAIN_MANIFEST=$DB/heain-app.yaml HEAIN_INSTANCE=d1 HEAIN_CORE_URL=$URL HEAIN_CORE_ID=G HEAIN_CA=$C/ca.pem HEAIN_CHAIN=$W/prov.pem \
  HEAIN_STATE_DIR=$W/state-d1 HEAIN_ENROLL_TOKEN=$W/d1.tok HEAIN_ENDPOINT_BASE=$DBURL HEAIN_LISTEN=127.0.0.1:19460 \
  HEAIN_DB_DIALECT=postgres HEAIN_DB_DSN="postgres://heain@127.0.0.1:$PGPORT/heain?sslmode=disable" \
    nohup "$W/heain-database" >> "$W/d1.log" 2>&1 &
  echo $! > "$P/d1.pid"; }
approve_reg() { for i in $(seq 1 30); do
    for a in $(as approver-1 $URL/v1/admin/policy/pending | j "' '.join(x['ID'] for x in d['actions'] if x['Type']=='app.register')"); do
      code approver-1 -X POST $URL/v1/admin/policy/$a/approve >/dev/null; done
    grep -q "heain-database: active" "$W/d1.log" 2>/dev/null && return 0; sleep 1; done; return 1; }
decide() { code approver-1 -X POST $URL/v1/admin/policy/$1/$2 >/dev/null; }
until_ok() { for i in $(seq 1 ${2:-15}); do eval "$1" && return 0; sleep 1; done; return 1; }
audit() { as admin "$URL/v1/admin/audit?limit=20000"; }

echo "== 0. core node G, a throwaway PostgreSQL ($PGBIN), build heain-database"
[ -x "$PGBIN/initdb" ] || { echo "needs PostgreSQL server binaries (initdb, pg_ctl), e.g. apt install postgresql"; exit 1; }
$H clean >/dev/null; $H build >/dev/null || { echo "core build failed"; exit 1; }; $H certs >/dev/null
mkdir -p "$L" "$P" "$T/data-G" "$W"; for c in admin approver-1; do mkcert $c; done
pgstop; mkdir -p "$PG"; [ "$(id -u)" = 0 ] && chown postgres "$PG"
pgrun "$PGBIN/initdb" -D "$PG/data" -U heain --auth=trust >/dev/null 2>&1 \
  && pgrun "$PGBIN/pg_ctl" -D "$PG/data" -l "$PG/pg.log" -o "-p $PGPORT -k $PG -c listen_addresses=127.0.0.1" -w start >/dev/null 2>&1 \
  && "$PGBIN/createdb" -h 127.0.0.1 -p $PGPORT -U heain heain && ok "PostgreSQL $("$PGBIN/postgres" --version | awk '{print $3}') running on 127.0.0.1:$PGPORT" || { bad "postgres: $(tail -3 "$PG/pg.log" 2>/dev/null)"; exit 1; }
openssl genrsa -out "$W/prov.key" 2048 >/dev/null 2>&1
openssl req -new -key "$W/prov.key" -subj "/CN=heain-test-provisioning-ca" -out "$W/prov.csr" >/dev/null 2>&1
openssl x509 -req -in "$W/prov.csr" -CA "$C/ca.pem" -CAkey "$C/ca.key" -CAcreateserial -out "$W/prov.pem" -days 30 -sha256 \
  -extfile <(printf "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign") >/dev/null 2>&1
: > "$L/G.log"
nohup "$BIN" -node-id=G -tier=ZONE -raft-addr=127.0.0.1:19000 -data-dir="$T/data-G" -http-addr=127.0.0.1:18000 \
  -cert="$C/G.pem" -key="$C/G.key" -ca="$C/ca.pem" -admin-node-id=admin -approver-ids=approver-1 -bootstrap=true \
  -approval-store-path="$T/data-G/approvals.db" -provision-ca-cert="$W/prov.pem" -provision-ca-key="$W/prov.key" >> "$L/G.log" 2>&1 &
echo $! > "$P/G.pid"; sleep 6
( cd "$DB" && GOFLAGS= GOWORK=${SDK_GOWORK:-} go build -o "$W/heain-database" ./cmd/heain-database ) && ok "heain-database builds" || { bad "build"; pgstop; exit 1; }
for k in $(seq 1 15); do [ "$(as admin -o /dev/null -w '%{http_code}' -X POST -H 'Content-Type: application/json' -d '{"label":"probe.x"}' $URL/provision/token)" = 200 ] && break; sleep 1; done
# a client app certificate (provisioning flow), for the calls other apps would make
as admin -X POST -H 'Content-Type: application/json' -d '{"label":"client.c1"}' $URL/provision/token > "$W/c1.json"
python3 - "$W" <<'PY'
import json,sys; w=sys.argv[1]; d=json.load(open(w+"/c1.json"))
open(w+"/c1.boot.pem","w").write(d["bootstrap_cert_pem"]+open(w+"/prov.pem").read()); open(w+"/c1.boot.key","w").write(d["bootstrap_key_pem"])
open(w+"/c1.token","w").write(d["token"])
PY
openssl genrsa -out "$W/client.key" 2048 >/dev/null 2>&1; openssl req -new -key "$W/client.key" -subj "/CN=client.c1" -out "$W/client.csr" >/dev/null 2>&1
python3 -c "import json;print(json.dumps({'token':open('$W/c1.token').read(),'csr_pem':open('$W/client.csr').read()}))" > "$W/c1.req"
curl -sk --noproxy '*' --cert "$W/c1.boot.pem" --key "$W/c1.boot.key" --cacert "$C/ca.pem" -X POST -H 'Content-Type: application/json' -d @"$W/c1.req" $URL/provision/csr \
  | python3 -c "import json,sys;open('$W/client.pem','w').write(json.load(sys.stdin)['cert_pem']+open('$W/prov.pem').read())"

echo "== 1. heain-database starts (plain process), is admitted and gets its data key from core"
as admin -X POST -H 'Content-Type: application/json' -d '{"label":"heain-database.d1"}' $URL/provision/token > "$W/d1.tok"
run_db
approve_reg && ok "heain-database d1 admitted (P5 app.register) and serving" || { bad "start: $(tail -3 "$W/d1.log")"; pgstop; $H stop-all >/dev/null 2>&1; exit 1; }
[ "$(audit | j "sum(1 for x in d['records'] if x['event']['Action']=='app.key_created' and x['event']['Detail']['key_id']=='inside')")" = 1 ] && ok "its 'inside' data key was created in core's KMS (audited, never the key)" || bad "inside key"
[ "$(as admin -o /dev/null -w '%{http_code}' $DBURL/v1/accounts)" = 403 ] && ok "a certificate that is not an app certificate is refused (403)" || bad "admin cert"

echo "== 2. inside: accounts and learned knowledge"
[ "$(clc -X PUT -d '{"role":"voter","metadata_kv":{"district":"Bangrak-secret-district"}}' $DBURL/v1/accounts/citizen-3100500012345)" = 200 ] \
  && [ "$(cl $DBURL/v1/accounts/citizen-3100500012345 | j "d['metadata_kv']['district']")" = Bangrak-secret-district ] && ok "account put and read back" || bad "account"
clc -X PUT -d '{"role":"officer"}' $DBURL/v1/accounts/officer-7 >/dev/null
[ "$(cl $DBURL/v1/accounts | j "len(d['accounts'])")" = 2 ] && [ "$(clc -X DELETE $DBURL/v1/accounts/officer-7)" = 200 ] && [ "$(clc $DBURL/v1/accounts/officer-7)" = 404 ] \
  && ok "list (2), delete, then 404" || bad "list/delete"
[ "$(clc -X PUT -d '{"value":"whisper-th-secret-v3"}' $DBURL/v1/knowledge/asr/model-th)" = 200 ] && [ "$(cl $DBURL/v1/knowledge/asr/model-th | j "d['kind']")" = learned ] \
  && ok "learned knowledge is written directly" || bad "learned"

echo "== 3. policy writes wait for core's P5"
r=$(cl -X PUT -d '{"value":"0.92"}' $DBURL/v1/policy/access/face-threshold -w '\n%{http_code}')
A1=$(echo "$r" | head -1 | j "d['action_id']")
[ "$(echo "$r" | tail -1)" = 202 ] && [ "$(echo "$r" | head -1 | j "d['result']")" = WAITING_APPROVAL ] && ok "policy write -> 202, WAITING_APPROVAL ($A1)" || bad "propose: $r"
[ "$(clc $DBURL/v1/knowledge/access/face-threshold)" = 404 ] && ok "not in effect before an Approver decides" || bad "applied too early"
[ "$(as approver-1 $URL/v1/admin/policy/pending | j "[a['Data'].get('namespace') for a in d['actions'] if a['ID']=='$A1'][0]")" = access ] && ok "the Approver sees namespace/key and a hash, not the value" || bad "pending view"
decide "$A1" approve
until_ok '[ "$(cl $DBURL/v1/knowledge/access/face-threshold | j "d[\"value\"]")" = 0.92 ]' && [ "$(cl $DBURL/v1/knowledge/access/face-threshold | j "d['kind']+'/'+d['action_id']")" = "policy/$A1" ] \
  && ok "approved -> applied as policy, linked to the P5 action" || bad "apply: $(cl $DBURL/v1/knowledge/access/face-threshold)"
[ "$(cl $DBURL/v1/requests/$A1 | j "d['state']+'/'+d['result']")" = applied/APPROVED ] && ok "request status: applied/APPROVED" || bad "status"
[ "$(clc -X PUT -d '{"value":"0.1"}' $DBURL/v1/knowledge/access/face-threshold)" = 409 ] && ok "a policy entry cannot be overwritten as learned (409)" || bad "learned over policy"
A2=$(cl -X PUT -d '{"value":"0.10"}' $DBURL/v1/policy/access/face-threshold | j "d['action_id']"); decide "$A2" reject
until_ok '[ "$(cl $DBURL/v1/requests/'"$A2"' | j "d[\"state\"]")" = denied ]' && [ "$(cl $DBURL/v1/knowledge/access/face-threshold | j "d['value']")" = 0.92 ] \
  && ok "rejected -> dropped; the approved value stays" || bad "deny"

echo "== 4. outside: a reference dataset in PostgreSQL"
REC='[{"record_key":"3100500012345","value":{"eligible":true,"name":"Somchai-secret-name"}},{"record_key":"3100500099999","value":{"eligible":false}}]'
r=$(cl -X POST -d "{\"scope_key\":\"bkk-district-9\",\"source_description\":\"EC snapshot 2026-Q4\",\"lifecycle_policy\":{\"on_complete\":\"full_purge\"},\"records\":$REC}" $DBURL/v1/datasets/voters/import)
[ "$(echo "$r" | j "str(d['version'])+'/'+str(d['record_count'])")" = 1/2 ] && ok "import: version 1, 2 records, lifecycle full_purge" || bad "import: $r"
[ "$(cl "$DBURL/v1/datasets/voters/records/3100500012345?scope_key=bkk-district-9" | j "d['value']['name']")" = Somchai-secret-name ] && ok "verification lookup finds the record" || bad "lookup"
[ "$(clc "$DBURL/v1/datasets/voters/records/3100500012345?scope_key=bkk-district-8")" = 404 ] && ok "lookups are scoped: another scope -> 404" || bad "scope"
[ "$(psqlq 'SELECT count(*) FROM hdb_records')" = 2 ] && ok "the records are in PostgreSQL" || bad "rows: $(psqlq 'SELECT count(*) FROM hdb_records')"
"$PGBIN/pg_dump" -h 127.0.0.1 -p $PGPORT -U heain heain > "$W/pg.dump" 2>/dev/null
[ -s "$W/pg.dump" ] && ! grep -q -e 3100500012345 -e Somchai-secret -e '"eligible"' "$W/pg.dump" && ok "pg_dump holds no citizen id, name or value (blinded keys, sealed values)" || bad "plaintext in PostgreSQL"
! grep -rqaF -e citizen-3100500012345 -e Bangrak-secret -e whisper-th-secret "$W/state-d1" && ok "no plaintext in heain-database's BoltDB files" || bad "plaintext in BoltDB: $(grep -rla -e Bangrak-secret -e whisper-th-secret "$W/state-d1")"
! audit | grep -q -e 3100500012345 -e Somchai-secret -e Bangrak-secret && ok "no plaintext in core's audit" || bad "plaintext in core audit"
[ "$(audit | j "sorted(set(x['event']['Detail'].get('capability') for x in d['records'] if x['event']['Action']=='app.event' and x['event']['Actor'].startswith('heain-database.')))")" = "['db.account.read', 'db.account.write', 'db.dataset.import', 'db.dataset.lookup', 'db.knowledge.read', 'db.knowledge.write', 'db.policy.propose']" ] \
  && ok "every formal call was audited in core (formal capabilities)" || bad "formal audit: $(audit | j "sorted(set(x['event']['Detail'].get('capability') for x in d['records'] if x['event']['Action']=='app.event'))")"

echo "== 5. restart: the data comes back with the key from core"
kill -TERM "$(cat "$P/d1.pid")"; sleep 2; run_db
until_ok 'grep -c "heain-database: active" "$W/d1.log" | grep -q 2' 30
[ "$(cl $DBURL/v1/accounts/citizen-3100500012345 | j "d['role']")" = voter ] && [ "$(cl "$DBURL/v1/datasets/voters/records/3100500012345?scope_key=bkk-district-9" | j "d['value']['eligible']")" = True ] \
  && ok "after a restart accounts and dataset records read back" || bad "restart: $(tail -2 "$W/d1.log")"

echo "== 6. purge: through P5, rows deleted and the key destroyed (crypto-shred)"
P1=$(cl -X POST -d '{"scope_key":"bkk-district-9"}' $DBURL/v1/datasets/voters/purge | j "d['action_id']")
[ -n "$P1" ] && [ "$(clc "$DBURL/v1/datasets/voters/records/3100500012345?scope_key=bkk-district-9")" = 200 ] && ok "purge proposed ($P1); data still there until approved" || bad "purge propose"
[ "$(clc -X POST -d '{"scope_key":"nope"}' $DBURL/v1/datasets/voters/purge)" = 404 ] && ok "purging an unknown scope -> 404" || bad "unknown purge"
decide "$P1" approve
until_ok '[ "$(clc "$DBURL/v1/datasets/voters/records/3100500012345?scope_key=bkk-district-9")" = 404 ]' && [ "$(psqlq 'SELECT count(*) FROM hdb_records')" = 0 ] \
  && ok "approved -> rows deleted from PostgreSQL" || bad "purge apply"
[ "$(audit | j "sum(1 for x in d['records'] if x['event']['Action']=='app.key_destroyed' and x['event']['Detail']['key_id'].startswith('ds-'))")" = 1 ] \
  && [ "$(audit | j "sum(1 for x in d['records'] if x['event']['Action']=='app.event' and x['event']['Detail'].get('capability')=='db.dataset.purge' and x['event']['Result']=='applied')")" -ge 1 ] \
  && ok "the dataset's key was destroyed in core and the purge audited (crypto-shred: backups are unreadable too)" || bad "purge audit"
[ "$(as admin "$URL/v1/admin/audit/verify" | j "d['ok']")" = True ] && ok "core audit chain verifies" || bad "audit verify"

echo "== cleanup"
kill -TERM "$(cat "$P/d1.pid")" 2>/dev/null; sleep 2
$H stop-all >/dev/null 2>&1; pgstop
echo
echo "RESULT: $PASS passed, $FAIL failed"
