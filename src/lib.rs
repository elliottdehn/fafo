// The surface: HTTP for clients, RPC between nodes.
pub mod api;
pub mod rpc;

/// Deterministic hash maps. `DefaultHasher::new()` is fixed-key SipHash,
/// so iteration order is a function of the data alone — a simulator run
/// replays bit-for-bit. (std's default `RandomState` reseeds per process,
/// and that randomness leaks into blob-write order through every loop
/// over a map.)
pub type Map<K, V> =
    std::collections::HashMap<K, V, std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>>;
pub type Set<T> =
    std::collections::HashSet<T, std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>>;

/// The paranoia switch. Off (production): internal inconsistencies are
/// survived quietly — the goal in prod is to never crash. On (the
/// simulator, or FAFO_PARANOIA=1): every `fafo_assert!` is live and an
/// inconsistency crashes on the spot — the goal in sim is to crash the
/// instant anything is off, at a seed that replays it.
pub static PARANOIA: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

pub fn paranoia() -> bool {
    PARANOIA.load(std::sync::atomic::Ordering::Relaxed)
}

/// A sage assert: free in production, fatal under simulation.
macro_rules! fafo_assert {
    ($cond:expr, $($msg:tt)+) => {
        if crate::paranoia() && !$cond {
            panic!("fafo_assert failed: {}", format_args!($($msg)+));
        }
    };
}
pub(crate) use fafo_assert;

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
pub mod objlog;

// Policy: capability tokens and container resource budgets.
pub mod grants;
pub mod limits;

// Durable last-wills: promises that outlive the node holding the socket.
pub mod wills;

// The adversary: deterministic simulation testing (see plan.md and the
// dst binary). Ships in the crate so `cargo run --bin dst` needs nothing
// special — anyone can mine.
pub mod sim;
