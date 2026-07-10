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

  async objects(): Promise<string[]> {
    return (await this.call<{ objects: string[] }>("GET", "/objects")).objects;
  }

  stats(): Promise<Record<string, unknown>> {
    return this.call("GET", "/stats");
  }
}
