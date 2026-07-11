// The surface: HTTP for clients, RPC between nodes.
pub mod api;
pub mod rpc;

// The machine: topology + leases, and the serial worker loops that
// admit, execute, and ship transactions.
pub mod cluster;
pub mod worker;

// Persistence: the blob store is the only durable truth. Local SQLite
// files are working copies; large objects ship page deltas.
pub mod store;
pub mod r2;
pub mod object;
pub mod delta;

// Policy: capability tokens and container resource budgets.
pub mod grants;
pub mod limits;
