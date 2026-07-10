// The Worker in front of the fafo fleet. Two jobs:
//   1. Route inter-node RPC: /internal/instance/<name>/<path> reaches that
//      specific instance — Cloudflare Containers have no direct
//      container-to-container networking, so nodes advertise Worker-routed
//      URLs and their RPC rides back through here.
//   2. Spread public API traffic across the fleet; any node routes or
//      proxies transactions to the right owner internally.
//
// Container identity IS logical-worker assignment: instances are named
// fafo-0..N-1, and each auto-claims its share of logical workers from the
// leases in R2. Kill an instance and the survivors (or its replacement)
// claim its workers at a bumped epoch.

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

export class FafoNode extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "2h";
  enableInternet = true; // outbound HTTPS to R2

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const name = ctx.id.name ?? "fafo-0";
    const perInstance = Math.ceil(LOGICAL_WORKERS / Number(env.FAFO_INSTANCES ?? "1"));
    this.envVars = {
      HOST: "0.0.0.0",
      PORT: "8080",
      DATA_DIR: "/tmp/fafo",
      BLOB_STORE: "r2",
      LOGICAL_WORKERS: String(LOGICAL_WORKERS),
      CLAIM: `auto:${perInstance}`,
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

    // Inter-node RPC and health checks, addressed by instance name.
    const m = url.pathname.match(/^\/internal\/instance\/([\w-]+)(\/.*)$/);
    if (m) {
      const instance = env.FAFO_NODE.getByName(m[1]);
      url.pathname = m[2];
      return instance.fetch(new Request(url.toString(), request));
    }

    // Public API: pick any instance; fafo routes internally from there.
    const fleet = Number(env.FAFO_INSTANCES ?? "1");
    const name = `fafo-${Math.floor(Math.random() * fleet)}`;
    return env.FAFO_NODE.getByName(name).fetch(request); // fetch() auto-starts
  },
};
