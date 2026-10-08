#!/usr/bin/env bash
# heain-database live test B-1b (Stage B, author decisions 2026-10-08): inside data kept on every
# node of a zone, ciphertext only.
#   G (18000, Master, farm registry) <- W (18003, farm Worker); heain-database d1 on G, d2 on W
#   (outside store: SQLite).
#  - accounts and knowledge are sealed under the zone key "inside" (core's zone app keys): d1 and d2
#    copy each other's sealed records through their change logs (GET /v1/replica/changes, found with
#    zone discovery), without opening them;
#  - a write on either node reaches the other; the later of two writes wins on both; a delete reaches both;
#  - G down: d2 keeps serving and writing; when G is back, d1 gets what d2 wrote meanwhile;
#  - records written before zone sync (under the node key) are sealed again under the zone key and sent;
#  - outside datasets stay on their node (node-local); pending P5 requests stay on their node;
#  - only heain-database may read a change log; no plaintext on disk or in core's audit.
# Needs ~/heain-core, ~/heain-sdk. ~3 min.  Run from ~/heain-database:  bash scripts/live_b1b.sh
set -uo pipefail
DB=$(cd "$(dirname "$0")/.." && pwd)
cd ~/heain-core || { echo "needs ~/heain-core"; exit 1; }
H=./test_1_2_live.sh
T=$HOME/heain-core/.test-1.2
C=$T/certs; L=$T/logs; P=$T/pids; BIN=$T/node; W=$T/db-b1b
URL=https://127.0.0.1:18000; GW=https://127.0.0.1:18003
D1=https://127.0.0.1:19460; D2=https://127.0.0.1:19461
PASS=0; FAIL=0
ok()  { echo "  PASS: $*"; PASS=$((PASS+1)); }
bad() { echo "  FAIL: $*"; FAIL=$((FAIL+1)); }
as() { local who=$1; shift; curl -sk --noproxy '*' --cert "$C/$who.pem" --key "$C/$who.key" --cacert "$C/ca.pem" "$@"; }
code() { local who=$1; shift; as "$who" -o /dev/null -w "%{http_code}" "$@"; }
cl() { curl -sk --noproxy '*' --cert "$W/client.pem" --key "$W/client.key" --cacert "$C/ca.pem" -H 'Content-Type: application/json' "$@"; }
clc() { cl -o /dev/null -w "%{http_code}" "$@"; }
j() { python3 -c "import json,sys;d=json.load(sys.stdin);print($1)" 2>/dev/null; }
mkcert() { [ -f "$C/$1.pem" ] && return; openssl genrsa -out "$C/$1.key" 2048 >/dev/null 2>&1
  openssl req -new -key "$C/$1.key" -subj "/CN=$1" -out "$C/$1.csr" >/dev/null 2>&1
  openssl x509 -req -in "$C/$1.csr" -CA "$C/ca.pem" -CAkey "$C/ca.key" -CAcreateserial -out "$C/$1.pem" \
    -days 825 -sha256 -extfile <(printf "subjectAltName=DNS:%s" "$1") >/dev/null 2>&1; }
until_ok() { for i in $(seq 1 ${2:-20}); do eval "$1" && return 0; sleep 1; done; return 1; }
audit() { as admin "$1/v1/admin/audit?limit=20000"; }
run_db() { # <instance> <core-url> <core-id> <listen-port> [extra env]
  mkdir -p "$W/state-$1"
  env HEAIN_MANIFEST=$DB/heain-app.yaml HEAIN_INSTANCE=$1 HEAIN_CORE_URL=$2 HEAIN_CORE_ID=$3 HEAIN_CA=$C/ca.pem HEAIN_CHAIN=$W/prov.pem \
    HEAIN_ENROLL_CORE_URL=$URL HEAIN_ENROLL_CORE_ID=G \
    HEAIN_STATE_DIR=$W/state-$1 HEAIN_ENROLL_TOKEN=$W/$1.tok HEAIN_ENDPOINT_BASE=https://127.0.0.1:$4 HEAIN_LISTEN=127.0.0.1:$4 \
    HEAIN_DB_DIALECT=sqlite ${5:-} \
    nohup "$W/heain-database" -zone-sync-every=1s >> "$W/$1.log" 2>&1 &
  echo $! > "$P/db-$1.pid"; }
approve_all() { for u in $URL $GW; do
    for a in $(as approver-1 $u/v1/admin/policy/pending | j "' '.join(x['ID'] for x in d['actions'] if x['Type']=='app.register')"); do
      code approver-1 -X POST $u/v1/admin/policy/$a/approve >/dev/null; done; done; }
active() { for i in $(seq 1 40); do approve_all; [ "$(grep -c 'heain-database: active' "$W/$1.log" 2>/dev/null)" -ge "${2:-1}" ] && return 0; sleep 1; done; return 1; }
stopdb() { [ -f "$P/db-$1.pid" ] && kill -TERM "$(cat "$P/db-$1.pid")" 2>/dev/null; sleep 2; }
cleanup() { for i in d1 d2; do [ -f "$P/db-$i.pid" ] && kill "$(cat "$P/db-$i.pid")" 2>/dev/null; done; true; }
trap cleanup EXIT

echo "== 0. Master G + farm Worker W, build heain-database"
$H clean >/dev/null; $H build >/dev/null || { echo "core build failed"; exit 1; }; $H certs >/dev/null
mkdir -p "$L" "$P" "$T/data-G" "$T/data-W" "$W"; for c in admin approver-1; do mkcert $c; done
openssl genrsa -out "$W/prov.key" 2048 >/dev/null 2>&1
openssl req -new -key "$W/prov.key" -subj "/CN=heain-test-provisioning-ca" -out "$W/prov.csr" >/dev/null 2>&1
openssl x509 -req -in "$W/prov.csr" -CA "$C/ca.pem" -CAkey "$C/ca.key" -CAcreateserial -out "$W/prov.pem" -days 30 -sha256 \
  -extfile <(printf "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign") >/dev/null 2>&1
: > "$L/G.log"; : > "$L/W.log"
startG() { nohup "$BIN" -node-id=G -tier=ZONE -raft-addr=127.0.0.1:19000 -data-dir="$T/data-G" -http-addr=127.0.0.1:18000 \
  -cert="$C/G.pem" -key="$C/G.key" -ca="$C/ca.pem" -admin-node-id=admin -approver-ids=approver-1 -bootstrap=true \
  -approval-store-path="$T/data-G/approvals.db" -farm-registry-ttl=30s -provision-ca-cert="$W/prov.pem" -provision-ca-key="$W/prov.key" >> "$L/G.log" 2>&1 &
  echo $! > "$P/G.pid"; }
startG; sleep 6
nohup "$BIN" -node-id=W -tier=WORKER -raft-addr=127.0.0.1:19003 -data-dir="$T/data-W" -http-addr=127.0.0.1:18003 \
  -cert="$C/W.pem" -key="$C/W.key" -ca="$C/ca.pem" -admin-node-id=admin -approver-ids=approver-1 -bootstrap=false \
  -approval-store-path="$T/data-W/approvals.db" \
  -farm-register-addr=https://127.0.0.1:18000 -farm-register-node-id=G -self-addr=https://127.0.0.1:18003 -farm-register-interval=2s >> "$L/W.log" 2>&1 &
echo $! > "$P/W.pid"; sleep 5
for u in $URL $GW; do code admin -X PUT -d '{"value":"2s"}' $u/v1/admin/config/system/zone.sync_interval >/dev/null; done
( cd "$DB" && GOFLAGS= GOWORK=${SDK_GOWORK:-} go build -o "$W/heain-database" ./cmd/heain-database ) && ok "heain-database builds" || { bad "build"; exit 1; }
for k in $(seq 1 15); do [ "$(code admin -X POST -H 'Content-Type: application/json' -d '{"label":"probe.x"}' $URL/provision/token)" = 200 ] && break; sleep 1; done
for n in client.c1 other.o1; do
  as admin -X POST -H 'Content-Type: application/json' -d "{\"label\":\"$n\"}" $URL/provision/token > "$W/$n.json"
  python3 - "$W" "$n" <<'PY'
import json,sys; w,n=sys.argv[1],sys.argv[2]; d=json.load(open(f"{w}/{n}.json"))
open(f"{w}/{n}.boot.pem","w").write(d["bootstrap_cert_pem"]+open(w+"/prov.pem").read()); open(f"{w}/{n}.boot.key","w").write(d["bootstrap_key_pem"])
open(f"{w}/{n}.token","w").write(d["token"])
PY
  openssl genrsa -out "$W/$n.key" 2048 >/dev/null 2>&1; openssl req -new -key "$W/$n.key" -subj "/CN=$n" -out "$W/$n.csr" >/dev/null 2>&1
  python3 -c "import json;print(json.dumps({'token':open('$W/$n.token').read(),'csr_pem':open('$W/$n.csr').read()}))" > "$W/$n.req"
  curl -sk --noproxy '*' --cert "$W/$n.boot.pem" --key "$W/$n.boot.key" --cacert "$C/ca.pem" -X POST -H 'Content-Type: application/json' -d @"$W/$n.req" $URL/provision/csr \
    | python3 -c "import json,sys;open('$W/$n.pem','w').write(json.load(sys.stdin)['cert_pem']+open('$W/prov.pem').read())"
done
cp "$W/client.c1.pem" "$W/client.pem"; cp "$W/client.c1.key" "$W/client.key"

echo "== 1. d1 on G starts with zone sync OFF and writes (sealed under the node key)"
as admin -X POST -H 'Content-Type: application/json' -d '{"label":"heain-database.d1"}' $URL/provision/token > "$W/d1.tok"
run_db d1 $URL G 19460 HEAIN_DB_ZONE_SYNC=off
active d1 && ok "d1 active on G (zone sync off)" || { bad "d1 start: $(tail -3 "$W/d1.log")"; exit 1; }
[ "$(clc -X PUT -d '{"role":"voter","metadata_kv":{"district":"Bangrak-secret-district"}}' $D1/v1/accounts/citizen-early)" = 200 ] && ok "account citizen-early written before zone sync" || bad "early write"
stopdb d1
run_db d1 $URL G 19460
active d1 2 && grep -q "sealed again under the zone key" "$W/d1.log" && grep -q "zone sync on" "$W/d1.log" \
  && ok "restarted with zone sync: the old record is sealed again under the zone key" || bad "migration: $(grep -i zone "$W/d1.log" | tail -3)"
[ "$(cl $D1/v1/accounts/citizen-early | j "d['role']")" = voter ] && ok "and still reads back" || bad "read after migration"
[ "$(audit $URL | j "sum(1 for x in d['records'] if x['event']['Action']=='app.zone_key_created' and x['event']['Detail']['key_id']=='inside')")" = 1 ] \
  && ok "core made the zone key 'inside' (audited, never the key)" || bad "zone key in core"

echo "== 2. d2 on W joins the zone and gets everything"
as admin -X POST -H 'Content-Type: application/json' -d '{"label":"heain-database.d2"}' $URL/provision/token > "$W/d2.tok"
run_db d2 $GW W 19461
active d2 && ok "d2 active on W (enrolled through G, admitted on W)" || { bad "d2 start: $(tail -3 "$W/d2.log")"; exit 1; }
until_ok '[ "$(cl $D2/v1/accounts/citizen-early | j "d[\"role\"]")" = voter ]' && ok "d2 has the record written on G before zone sync" || bad "d2 initial copy: $(tail -3 "$W/d2.log")"
r=$(as admin "$URL/v1/admin/audit?limit=20000" | j "sorted(set(x['event']['Detail'].get('member','') for x in d['records'] if x['event']['Action'] in ('app.zone_key_wrapped','app.zone_keys_synced')))")
[ "$r" = "['W']" ] && ok "G wrapped the zone key to W only (asked for, or pulled by the zone sync)" || bad "zone key wrapped to: $r"

echo "== 3. writes go both ways"
clc -X PUT -d '{"role":"officer"}' $D1/v1/accounts/officer-7 >/dev/null
clc -X PUT -d '{"value":"whisper-th-secret-v3"}' $D2/v1/knowledge/asr/model-th >/dev/null
until_ok '[ "$(cl $D2/v1/accounts/officer-7 | j "d[\"role\"]")" = officer ] && [ "$(cl $D1/v1/knowledge/asr/model-th | j "d[\"value\"]")" = whisper-th-secret-v3 ]' \
  && ok "an account written on G reaches W; knowledge written on W reaches G" || bad "both ways"
clc -X PUT -d '{"role":"first"}' $D2/v1/accounts/race >/dev/null; sleep 0.2; clc -X PUT -d '{"role":"second"}' $D1/v1/accounts/race >/dev/null
until_ok '[ "$(cl $D1/v1/accounts/race | j "d[\"role\"]")" = second ] && [ "$(cl $D2/v1/accounts/race | j "d[\"role\"]")" = second ]' \
  && ok "two writes of one account: the later one wins on both nodes" || bad "last writer wins: d1=$(cl $D1/v1/accounts/race | j "d['role']") d2=$(cl $D2/v1/accounts/race | j "d['role']")"
clc -X DELETE $D2/v1/accounts/officer-7 >/dev/null
until_ok '[ "$(clc $D1/v1/accounts/officer-7)" = 404 ]' && ok "a delete on W reaches G" || bad "delete"

echo "== 4. G down: W keeps working, then G catches up"
kill "$(cat "$P/G.pid")"; stopdb d1; sleep 12   # past W's 10 s certificate cache: W uses the last answer of G
[ "$(clc -X PUT -d '{"role":"islander"}' $D2/v1/accounts/written-alone)" = 200 ] && [ "$(cl $D2/v1/accounts/citizen-early | j "d['role']")" = voter ] \
  && ok "G and d1 down: d2 still reads and writes (zone key copy on W)" || bad "W alone: $(tail -3 "$W/d2.log")"
startG; sleep 6; run_db d1 $URL G 19460
active d1 3 >/dev/null
until_ok '[ "$(cl $D1/v1/accounts/written-alone | j "d[\"role\"]")" = islander ]' 30 && ok "G and d1 back: d1 gets what d2 wrote meanwhile" || bad "catch-up: $(tail -3 "$W/d1.log")"

echo "== 5. what stays on its node"
cl -X POST -d '{"scope_key":"bkk-9","source_description":"EC","records":[{"record_key":"3100500012345","value":{"eligible":true}}]}' $D1/v1/datasets/voters/import >/dev/null
sleep 3
[ "$(clc "$D1/v1/datasets/voters/records/3100500012345?scope_key=bkk-9")" = 200 ] && [ "$(clc "$D2/v1/datasets/voters/records/3100500012345?scope_key=bkk-9")" = 404 ] \
  && ok "an outside dataset imported on G is not on W (node-local)" || bad "outside data moved"
A=$(cl -X PUT -d '{"value":"0.92"}' $D1/v1/policy/access/face-threshold | j "d['action_id']")
[ -n "$A" ] && [ "$(clc $D2/v1/requests/$A)" = 404 ] && ok "a pending P5 request stays on its node" || bad "request moved"
code approver-1 -X POST $URL/v1/admin/policy/$A/approve >/dev/null
until_ok '[ "$(cl $D2/v1/knowledge/access/face-threshold | j "d[\"kind\"]")" = policy ]' && ok "once approved and applied on G, the policy entry reaches W" || bad "policy entry to W"

echo "== 6. only heain-database reads a change log"
c=$(curl -sk --noproxy '*' --cert "$W/other.o1.pem" --key "$W/other.o1.key" --cacert "$C/ca.pem" -o /dev/null -w '%{http_code}' $D1/v1/replica/changes)
[ "$c" = 403 ] && ok "another app is refused (403)" || bad "change log to another app: $c"
! grep -rqaF -e citizen-early -e Bangrak-secret -e whisper-th-secret -e islander "$W/state-d1" "$W/state-d2" && ok "no plaintext in either node's files (records travel sealed)" || bad "plaintext on disk"
! audit $URL | grep -q -e Bangrak-secret -e whisper-th-secret && ! audit $GW | grep -q -e Bangrak-secret -e whisper-th-secret && ok "no plaintext in core's audit" || bad "plaintext in audit"
[ "$(audit $GW | j "any(x['event']['Action']=='app.event' and x['event']['Detail'].get('capability')=='db.replica' for x in d['records'])")" = True ] \
  && ok "applied changes are audited in core (db.replica, counts only)" || bad "replica audit"

echo "== cleanup"
cleanup; $H stop-all >/dev/null 2>&1
echo
echo "RESULT: $PASS passed, $FAIL failed"
echo "(logs: $W)"
