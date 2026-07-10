// The Worker in front of the fafo fleet. Three jobs:
//   1. Route inter-node RPC: /internal/instance/<name>/<path> reaches that
//      specific instance — Cloudflare Containers have no direct
//      container-to-container networking, so nodes advertise Worker-routed
//      URLs and their RPC rides back through here.
//   2. Route PUBLIC requests straight to the owning instance: the Worker
//      computes the same FNV-1a placement hash as the engine, and instances
//      claim deterministic worker ranges — so a request for an un-migrated
//      object lands on its owner with ZERO inter-instance hairpins
//      (measured at ~1.3s each). Migrated objects bounce internally once.
//   3. Spread everything else across the fleet.
//
// Instances are named fafo-0..N-1 and each claims the worker range
// [N*W/I, (N+1)*W/I) — deterministic, so routing needs no state and rolling
// deploys reclaim the same range via the self-address takeover path.

import { Container } from "@cloudflare/containers";

interface Env {
  FAFO_NODE: DurableObjectNamespace<FafoNode>;
  FAFO_INSTANCES: string;
  PUBLIC_URL: string;
  CLUSTER_SECRET: string;
  API_TOKEN: string;
  R2_ACCOUNT_ID: string;
  R2_BUCKET: string;
  R2_ACCESS_KEY_ID: string;
  R2_SECRET_ACCESS_KEY: string;
}

const LOGICAL_WORKERS = 64;

// FNV-1a 64 — identical to fafo's default_worker in src/cluster.rs.
function defaultWorker(object: string, logical: number): number {
  const bytes = new TextEncoder().encode(object);
  let h = 0xcbf29ce484222325n;
  const mask = 0xffffffffffffffffn;
  for (const b of bytes) {
    h ^= BigInt(b);
    h = (h * 0x100000001b3n) & mask;
  }
  return Number(h % BigInt(logical));
}

function instanceFor(worker: number, instances: number): string {
  return `fafo-${Math.floor((worker * instances) / LOGICAL_WORKERS)}`;
}

export class FafoNode extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "2h";
  enableInternet = true; // outbound HTTPS to R2

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const name = ctx.id.name ?? "fafo-0";
    const n = Number(name.split("-")[1] ?? "0");
    const instances = Number(env.FAFO_INSTANCES ?? "1");
    const lo = Math.floor((n * LOGICAL_WORKERS) / instances);
    const hi = Math.floor(((n + 1) * LOGICAL_WORKERS) / instances) - 1;
    this.envVars = {
      HOST: "0.0.0.0",
      PORT: "8080",
      DATA_DIR: "/tmp/fafo",
      BLOB_STORE: "r2",
      LOGICAL_WORKERS: String(LOGICAL_WORKERS),
      CLAIM: `${lo}-${hi}`,
      ADVERTISE: `${env.PUBLIC_URL}/internal/instance/${name}`,
      CLUSTER_SECRET: env.CLUSTER_SECRET,
      API_TOKEN: env.API_TOKEN,
      R2_ACCOUNT_ID: env.R2_ACCOUNT_ID,
      R2_BUCKET: env.R2_BUCKET,
      R2_ACCESS_KEY_ID: env.R2_ACCESS_KEY_ID,
      R2_SECRET_ACCESS_KEY: env.R2_SECRET_ACCESS_KEY,
    };
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const fleet = Number(env.FAFO_INSTANCES ?? "1");

    // Inter-node RPC and health checks, addressed by instance name.
    const m = url.pathname.match(/^\/internal\/instance\/([\w-]+)(\/.*)$/);
    if (m) {
      url.pathname = m[2];
      return env.FAFO_NODE.getByName(m[1]).fetch(new Request(url.toString(), request));
    }

    // Single-object routes: land directly on the hash-owner's instance.
    const obj = url.pathname.match(/^\/objects\/([\w-]+)\//);
    if (obj) {
      const name = instanceFor(defaultWorker(obj[1], LOGICAL_WORKERS), fleet);
      return env.FAFO_NODE.getByName(name).fetch(request);
    }

    // Transactions: peek the body and aim for the first participant's
    // owner — the node re-routes internally if placement has migrated.
    if (url.pathname === "/txn" && request.method === "POST") {
      const body = await request.text();
      let name = "fafo-0";
      try {
        const parsed = JSON.parse(body) as { objects?: string[] };
        const first = parsed.objects?.[0];
        if (first) {
          name = instanceFor(defaultWorker(first, LOGICAL_WORKERS), fleet);
        }
      } catch {
        // malformed body: let the node reject it with a real error
      }
      return env.FAFO_NODE.getByName(name).fetch(
        new Request(request.url, {
          method: request.method,
          headers: request.headers,
          body,
        }),
      );
    }

    // Everything else (/objects list, /stats, /healthz): spread randomly.
    const name = `fafo-${Math.floor(Math.random() * fleet)}`;
    return env.FAFO_NODE.getByName(name).fetch(request); // fetch() auto-starts
  },
};
