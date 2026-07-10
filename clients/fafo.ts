// Zero-dependency fafo client (fetch only; Node 18+, Bun, Deno, browsers, Workers).
//
//   import { Fafo } from "./fafo";
//   const db = new Fafo(); // http://127.0.0.1:8787
//
//   await db.exec("alice", "CREATE TABLE IF NOT EXISTS account (balance INTEGER CHECK (balance >= 0))");
//   await db.exec("alice", "INSERT INTO account (balance) VALUES (?1)", [100]);
//
//   // Cross-object atomic transaction: declare every participant up-front.
//   await db.txn(["alice", "bob"], [
//     { object: "alice", sql: "UPDATE account SET balance = balance - 60" },
//     { object: "bob",   sql: "UPDATE account SET balance = balance + 60" },
//   ]);
//
//   const rows = await db.query("alice", "SELECT balance FROM account");
//   // -> [{ balance: 40 }]

export type Param = string | number | boolean | null;
export interface Op {
  object: string;
  sql: string;
  params?: Param[];
}
export type OpResult = { rows: Record<string, unknown>[] } | { rows_affected: number };
export interface TxnResponse {
  txn_id: string;
  results: OpResult[];
  /** Poll replies only: feed back as `baseline` for change detection. */
  hash?: string;
}

export interface PollOpts {
  params?: Param[];
  /** Judge the condition only against durable (shipped) state. */
  durable?: boolean;
  /**
   * Change detection: the reply comes when the result hash differs from
   * this. Pass "" to bootstrap (immediate snapshot + hash), then feed each
   * reply's hash back in. Omit entirely for condition-variable semantics
   * (reply when the result is non-empty).
   */
  baseline?: string;
}

export interface PollResult {
  rows: Record<string, unknown>[];
  hash: string;
}

export class FafoError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(`${status}: ${message}`);
  }
}

export class Fafo {
  constructor(
    private base: string = "http://127.0.0.1:8787",
    private token?: string,
  ) {
    this.base = base.replace(/\/+$/, "");
  }

  private async call<T>(method: string, path: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (this.token) headers.authorization = `Bearer ${this.token}`;
    const resp = await fetch(this.base + path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const json = (await resp.json()) as Record<string, unknown>;
    if (!resp.ok) throw new FafoError(resp.status, String(json.error ?? resp.statusText));
    return json as T;
  }

  /**
   * Cross-object atomic transaction; every op's object must be declared.
   * `optimistic` acks after local apply; durability follows within one
   * storage round trip (a crash in that window loses the txn, consistently).
   * A later non-optimistic txn acts as a durability barrier.
   */
  txn(objects: string[], ops: Op[], opts?: { optimistic?: boolean }): Promise<TxnResponse> {
    return this.call("POST", "/txn", { objects, ops, optimistic: opts?.optimistic ?? false });
  }

  /** Single-object transaction, single statement. */
  exec(object: string, sql: string, params: Param[] = []): Promise<TxnResponse> {
    return this.call("POST", `/objects/${object}/exec`, { sql, params });
  }

  /** Single-object transaction, several statements, all-or-nothing. */
  execMany(object: string, statements: { sql: string; params?: Param[] }[]): Promise<TxnResponse> {
    return this.call("POST", `/objects/${object}/exec`, { ops: statements });
  }

  /** Read-only single statement; rejected if the SQL writes. */
  async query(object: string, sql: string, params: Param[] = []): Promise<Record<string, unknown>[]> {
    const out = await this.call<{ rows?: Record<string, unknown>[] }>(
      "POST",
      `/objects/${object}/query`,
      { sql, params },
    );
    return out.rows ?? [];
  }

  /**
   * Long-poll: resolves when the query's condition holds — non-empty
   * results, or (with `baseline`) a result hash different from the last
   * one seen. The subscription is your loop:
   *
   *   let cursor = 0;
   *   for (;;) {
   *     const { rows } = await db.poll("chan",
   *       "SELECT * FROM msgs WHERE id > ?1 ORDER BY id", { params: [cursor] });
   *     for (const m of rows) { handle(m); cursor = m.id as number; }
   *   }
   *
   * On "re-poll" errors (migration, revert, shutdown), just loop again.
   */
  async poll(object: string, sql: string, opts?: PollOpts): Promise<PollResult> {
    const out = await this.call<{ rows?: Record<string, unknown>[]; hash?: string }>(
      "POST",
      `/objects/${object}/poll`,
      { sql, params: opts?.params ?? [], durable: opts?.durable ?? false, baseline: opts?.baseline },
    );
    return { rows: out.rows ?? [], hash: out.hash ?? "" };
  }

  async objects(): Promise<string[]> {
    return (await this.call<{ objects: string[] }>("GET", "/objects")).objects;
  }

  stats(): Promise<Record<string, unknown>> {
    return this.call("GET", "/stats");
  }

  /**
   * Mint a capability token (root token required): per-object, per-verb
   * grants safe to hand to untrusted end-user devices. Verbs: read,
   * insert, update, delete, ddl, poll — or "write" (= the four write
   * verbs). Objects match exactly or by prefix glob ("user-77-*").
   * Keep TTLs short; verification is stateless, so expiry IS revocation.
   */
  async grant(
    grants: { objects: string; verbs: string[] }[],
    ttlSecs: number,
    sub?: string,
  ): Promise<{ token: string; exp: number }> {
    return this.call("POST", "/grant", { grants, ttl_secs: ttlSecs, sub });
  }

  /** Open a persistent connection: many transactions, one socket. */
  connect(): Promise<FafoSocket> {
    return FafoSocket.open(this["base"], this["token"]);
  }
}

/**
 * The production path: transactions as frames on one WebSocket. After the
 * upgrade, frames skip the per-request platform overhead — think of it as
 * the database connection, with pipelining for free.
 *
 *   const conn = await new Fafo(url, token).connect();
 *   await conn.txn([{ object: "alice", sql: "..." }]); // objects inferred
 */
export class FafoSocket {
  private next = 1;
  private pending = new Map<number, { resolve: (v: TxnResponse) => void; reject: (e: Error) => void }>();

  private constructor(private ws: WebSocket) {}

  static open(base: string, token?: string, pinTo?: string): Promise<FafoSocket> {
    const url = base.replace(/^http/, "ws") + "/ws" + (pinTo ? `?for=${pinTo}` : "");
    // The token rides the subprotocol header, never the URL (query strings
    // end up in access logs). Server selects "fafo" back.
    const protocols = token ? ["fafo", `fafo-token.${token}`] : ["fafo"];
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url, protocols);
      const sock = new FafoSocket(ws);
      ws.onopen = () => resolve(sock);
      ws.onerror = () => reject(new Error("websocket failed to open"));
      ws.onmessage = (ev) => {
        const msg = JSON.parse(String(ev.data)) as {
          id: number;
          result?: TxnResponse;
          error?: string;
          status?: number;
        };
        const p = sock.pending.get(msg.id);
        if (!p) return;
        sock.pending.delete(msg.id);
        if (msg.error !== undefined) p.reject(new FafoError(msg.status ?? 500, msg.error));
        else p.resolve(msg.result as TxnResponse);
      };
      ws.onclose = () => {
        for (const p of sock.pending.values()) p.reject(new Error("connection closed"));
        sock.pending.clear();
      };
    });
  }

  /** objects may be omitted; they're inferred from the ops. */
  txn(ops: Op[], opts?: { objects?: string[]; optimistic?: boolean; readOnly?: boolean }): Promise<TxnResponse> {
    const id = this.next++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(
        JSON.stringify({
          id,
          objects: opts?.objects ?? [],
          ops,
          optimistic: opts?.optimistic ?? false,
          read_only: opts?.readOnly ?? false,
        }),
      );
    });
  }

  /**
   * Long-poll as a frame on this socket (the production shape). Same
   * semantics as Fafo.poll; pin the socket to the object's owner with
   * FafoSocket.open(base, token, object) first.
   */
  poll(object: string, sql: string, opts?: PollOpts): { result: Promise<PollResult>; cancel: () => void } {
    const id = this.next++;
    const result = new Promise<TxnResponse>((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(
        JSON.stringify({
          id,
          poll: {
            object,
            sql,
            params: opts?.params ?? [],
            durable: opts?.durable ?? false,
            baseline: opts?.baseline,
          },
        }),
      );
    }).then((r) => {
      const first = r.results[0];
      const rows = first && "rows" in first ? first.rows : [];
      return { rows, hash: r.hash ?? "" };
    });
    return { result, cancel: () => this.ws.send(JSON.stringify({ id, cancel: true })) };
  }

  /**
   * Arm a last-will transaction: it runs (atomically, like any txn) when
   * this socket dies — clean close, drop, or error. One will per
   * connection; arming again replaces it. MQTT got this right.
   *
   *   await conn.txn([{ object: "room", sql: "INSERT INTO presence ..." }]);
   *   await conn.setWill([{ object: "room",
   *     sql: "DELETE FROM presence WHERE session = ?1", params: [sid] }]);
   *
   * Caveat: a will runs on socket close at the node; if the node itself
   * dies, it can't. Pair presence rows with an expires_at column refreshed
   * by heartbeat and filter it in your view query.
   */
  setWill(ops: Op[], opts?: { objects?: string[]; optimistic?: boolean }): Promise<unknown> {
    return this.willFrame({ objects: opts?.objects ?? [], ops, optimistic: opts?.optimistic ?? false });
  }

  /** Disarm the current will. */
  clearWill(): Promise<unknown> {
    return this.willFrame({ objects: [], ops: [], optimistic: false });
  }

  private willFrame(will: { objects: string[]; ops: Op[]; optimistic: boolean }): Promise<unknown> {
    const id = this.next++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve: resolve as (v: TxnResponse) => void, reject });
      this.ws.send(JSON.stringify({ id, will }));
    });
  }

  close(): void {
    this.ws.close();
  }
}
