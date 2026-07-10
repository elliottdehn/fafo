#!/bin/sh
# Demo: two account objects, one atomic transfer, one rejected overdraft.
# Start the server first: cargo run
BASE=${BASE:-http://127.0.0.1:8787}

say() { printf '\n== %s\n' "$1"; }

say "create alice with balance 100 (one object = one sqlite db)"
curl -s $BASE/objects/alice/exec -H 'content-type: application/json' -d '{
  "ops": [
    {"sql": "CREATE TABLE IF NOT EXISTS account (balance INTEGER NOT NULL CHECK (balance >= 0))"},
    {"sql": "INSERT INTO account (balance) VALUES (?1)", "params": [100]}
  ]}'

say "create bob with balance 100"
curl -s $BASE/objects/bob/exec -H 'content-type: application/json' -d '{
  "ops": [
    {"sql": "CREATE TABLE IF NOT EXISTS account (balance INTEGER NOT NULL CHECK (balance >= 0))"},
    {"sql": "INSERT INTO account (balance) VALUES (?1)", "params": [100]}
  ]}'

say "list objects"
curl -s $BASE/objects

say "transfer 60 alice -> bob, atomically, participants declared up-front"
curl -s $BASE/txn -H 'content-type: application/json' -d '{
  "objects": ["alice", "bob"],
  "ops": [
    {"object": "alice", "sql": "UPDATE account SET balance = balance - 60"},
    {"object": "bob",   "sql": "UPDATE account SET balance = balance + 60"}
  ]}'

say "balances after transfer (expect 40 / 160)"
curl -s $BASE/objects/alice/query -H 'content-type: application/json' -d '{"sql": "SELECT balance FROM account"}'
curl -s $BASE/objects/bob/query -H 'content-type: application/json' -d '{"sql": "SELECT balance FROM account"}'

say "overdraft: bob is credited FIRST, then alice fails the CHECK -> whole txn rolls back"
curl -s $BASE/txn -H 'content-type: application/json' -d '{
  "objects": ["alice", "bob"],
  "ops": [
    {"object": "bob",   "sql": "UPDATE account SET balance = balance + 500"},
    {"object": "alice", "sql": "UPDATE account SET balance = balance - 500"}
  ]}'

say "balances unchanged (expect 40 / 160)"
curl -s $BASE/objects/alice/query -H 'content-type: application/json' -d '{"sql": "SELECT balance FROM account"}'
curl -s $BASE/objects/bob/query -H 'content-type: application/json' -d '{"sql": "SELECT balance FROM account"}'

say "writes are rejected on the query endpoint"
curl -s $BASE/objects/alice/query -H 'content-type: application/json' -d '{"sql": "DELETE FROM account"}'

say "coordinator stats: after the first cross-worker transfer, alice and bob cohabit"
curl -s $BASE/stats
echo
