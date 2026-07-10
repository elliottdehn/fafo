#!/bin/sh
# Four processes, one world. Then kill everything and resume as two.
set -e
cd "$(dirname "$0")"
DATA=${DATA:-./data-cluster}
rm -rf "$DATA"
cargo build --release 2>/dev/null

say() { printf '\n== %s\n' "$1"; }
spawn() { # port claim-spec
  DATA_DIR=$DATA PORT=$1 LOGICAL_WORKERS=16 CLAIM=$2 CLUSTER_SECRET=demo \
    ./target/release/fafo > /dev/null 2>&1 &
  echo $!
}

say "start 4 processes, 4 logical workers each"
P1=$(spawn 8791 0-3); P2=$(spawn 8792 4-7); P3=$(spawn 8793 8-11); P4=$(spawn 8794 12-15)
sleep 1

say "create alice (via process 1) and bob (via process 4)"
curl -s -H "content-type: application/json" localhost:8791/objects/alice/exec -d '{"ops":[
  {"sql":"CREATE TABLE account (balance INTEGER NOT NULL CHECK (balance >= 0))"},
  {"sql":"INSERT INTO account (balance) VALUES (100)"}]}'; echo
curl -s -H "content-type: application/json" localhost:8794/objects/bob/exec -d '{"ops":[
  {"sql":"CREATE TABLE account (balance INTEGER NOT NULL CHECK (balance >= 0))"},
  {"sql":"INSERT INTO account (balance) VALUES (100)"}]}'; echo

say "atomic transfer submitted to process 2 (owns neither object)"
curl -s -H "content-type: application/json" localhost:8792/txn -d '{"objects":["alice","bob"],"ops":[
  {"object":"alice","sql":"UPDATE account SET balance = balance - 60"},
  {"object":"bob","sql":"UPDATE account SET balance = balance + 60"}]}'; echo

say "balances, asked of two different processes (expect 40 / 160)"
curl -s -H "content-type: application/json" localhost:8791/objects/alice/query -d '{"sql":"SELECT balance FROM account"}'; echo
curl -s -H "content-type: application/json" localhost:8793/objects/bob/query -d '{"sql":"SELECT balance FROM account"}'; echo

say "STOP THE WORLD (kill -9 all four)"
kill -9 $P1 $P2 $P3 $P4 2>/dev/null || true
sleep 0.5

say "resume as TWO auto-claiming processes (no per-instance config)"
P5=$(spawn 8795 auto:8); P6=$(spawn 8796 auto:8)
sleep 1

say "balances survived, served by the new world (expect 40 / 160)"
curl -s -H "content-type: application/json" localhost:8795/objects/alice/query -d '{"sql":"SELECT balance FROM account"}'; echo
curl -s -H "content-type: application/json" localhost:8796/objects/bob/query -d '{"sql":"SELECT balance FROM account"}'; echo

say "another atomic transfer in the new world"
curl -s -H "content-type: application/json" localhost:8796/txn -d '{"objects":["alice","bob"],"ops":[
  {"object":"bob","sql":"UPDATE account SET balance = balance - 100"},
  {"object":"alice","sql":"UPDATE account SET balance = balance + 100"}]}'; echo
curl -s -H "content-type: application/json" localhost:8795/objects/alice/query -d '{"sql":"SELECT balance FROM account"}'; echo
curl -s -H "content-type: application/json" localhost:8795/objects/bob/query -d '{"sql":"SELECT balance FROM account"}'; echo

kill -9 $P5 $P6 2>/dev/null || true
say "done"
