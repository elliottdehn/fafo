"""Zero-dependency fafo client (stdlib only).

    from fafo import Fafo
    db = Fafo()  # http://127.0.0.1:8787

    db.exec("alice", "CREATE TABLE IF NOT EXISTS account (balance INTEGER CHECK (balance >= 0))")
    db.exec("alice", "INSERT INTO account (balance) VALUES (?1)", [100])

    # Cross-object atomic transaction: declare every participant up-front.
    db.txn(["alice", "bob"], [
        ("alice", "UPDATE account SET balance = balance - 60"),
        ("bob",   "UPDATE account SET balance = balance + 60"),
    ])

    rows = db.query("alice", "SELECT balance FROM account")
    # -> [{"balance": 40}]
"""

import json
import urllib.error
import urllib.request


class FafoError(Exception):
    def __init__(self, status, message):
        super().__init__(f"{status}: {message}")
        self.status = status
        self.message = message


class Fafo:
    def __init__(self, base="http://127.0.0.1:8787", token=None, timeout=30):
        self.base = base.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _call(self, method, path, body=None):
        req = urllib.request.Request(
            self.base + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={"content-type": "application/json"},
        )
        if self.token:
            req.add_header("authorization", f"Bearer {self.token}")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            try:
                message = json.loads(e.read()).get("error", str(e))
            except Exception:
                message = str(e)
            raise FafoError(e.code, message) from None

    def txn(self, objects, ops, optimistic=False):
        """ops: iterable of (object, sql) or (object, sql, params).

        optimistic=True acks after local apply; durability follows within
        one storage round trip (a crash in that window loses the txn,
        consistently). A later optimistic=False txn is a durability barrier.
        """
        payload = [
            {"object": op[0], "sql": op[1], "params": list(op[2]) if len(op) > 2 else []}
            for op in ops
        ]
        return self._call(
            "POST",
            "/txn",
            {"objects": list(objects), "ops": payload, "optimistic": optimistic},
        )

    def exec(self, obj, sql, params=None):
        return self._call("POST", f"/objects/{obj}/exec", {"sql": sql, "params": params or []})

    def exec_many(self, obj, statements):
        """statements: iterable of sql or (sql, params)."""
        ops = [
            {"sql": s, "params": []} if isinstance(s, str) else {"sql": s[0], "params": list(s[1])}
            for s in statements
        ]
        return self._call("POST", f"/objects/{obj}/exec", {"ops": ops})

    def query(self, obj, sql, params=None):
        out = self._call("POST", f"/objects/{obj}/query", {"sql": sql, "params": params or []})
        return out.get("rows", [])

    def objects(self):
        return self._call("GET", "/objects")["objects"]

    def stats(self):
        return self._call("GET", "/stats")
