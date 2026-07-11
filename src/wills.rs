//! Durable last-wills: a will that survives the death of the node holding
//! the socket.
//!
//! The plain will (in `api::Session`) lives in one node's memory and fires
//! when that node observes the socket close. That is the fast path, and it
//! is enough when the node stays up. It is NOT enough when the NODE dies:
//! the socket is just as dead, but the promise died with the process.
//!
//! So an armed will is also written here, into a system object `_wills`,
//! with a `deadline`. While the connection lives, the holding node keeps
//! pushing the deadline forward (the refresher). When the node dies, the
//! refreshing stops; the deadline lapses; and ANY surviving node's sweeper
//! claims the lapsed will and fires it. A clean close deletes the durable
//! copy so it never double-fires.
//!
//! Firing is at-least-once with a reclaimable claim: a sweeper that dies
//! mid-fire lets another retry, so the will ops must be idempotent for
//! money-like effects — the same contract the plain HTTP path documents.
//! The claim keeps the COMMON case single-fire.

use crate::cluster::{Node, Op, OpResult, TxnResponse, submit_system};
use crate::grants::Capability;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The system object all durable wills live in. Underscore-prefixed like
/// every other system key, so it can never collide with a user object id.
pub const WILLS_OBJECT: &str = "_wills";

/// The frozen transaction a will runs, plus the grants it runs under —
/// everything a sweeper on another node needs to fire it faithfully.
#[derive(Serialize, Deserialize, Clone)]
pub struct DurableWill {
    pub objects: Vec<String>,
    pub ops: Vec<Op>,
    pub optimistic: bool,
    /// The capability frozen at arm time; the will fires under it even
    /// after the token expires (a promise already authorized). None = the
    /// will was armed by a root connection.
    pub cap: Option<Capability>,
}

const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS wills (\
    session TEXT PRIMARY KEY, \
    deadline INTEGER NOT NULL, \
    claim_deadline INTEGER NOT NULL DEFAULT 0, \
    payload TEXT NOT NULL)";

fn op(sql: &str, params: Vec<Value>) -> Op {
    Op {
        object: WILLS_OBJECT.to_string(),
        sql: sql.to_string(),
        params,
    }
}

/// Persist (or replace) a session's will, deadline = now + ttl. Pessimistic:
/// the client is told "armed" only once the will is durable.
pub async fn arm(node: &Node, session: &str, will: &DurableWill, deadline_ms: u64) -> Result<(), crate::api::ApiError> {
    let payload = serde_json::to_string(will).expect("will serializes");
    submit_system(
        node,
        vec![WILLS_OBJECT.to_string()],
        vec![
            op(SCHEMA, vec![]),
            op(
                "INSERT INTO wills (session, deadline, claim_deadline, payload) \
                 VALUES (?1, ?2, 0, ?3) \
                 ON CONFLICT(session) DO UPDATE SET deadline = ?2, claim_deadline = 0, payload = ?3",
                vec![json!(session), json!(deadline_ms), json!(payload)],
            ),
        ],
        false,
    )
    .await
    .map(|_| ())
}

/// Delete a session's durable will (clean disarm or clean close). Pessimistic
/// so the sweeper is guaranteed never to re-fire a will the client retired.
pub async fn disarm(node: &Node, session: &str) {
    // A missing table (no will ever armed cluster-wide) is fine.
    let _ = submit_system(
        node,
        vec![WILLS_OBJECT.to_string()],
        vec![
            op(SCHEMA, vec![]),
            op("DELETE FROM wills WHERE session = ?1", vec![json!(session)]),
        ],
        false,
    )
    .await;
}

/// Push the deadline forward for this node's still-connected wills. A lost
/// refresh only shortens a will's life a little, so it may ride optimistic.
pub async fn refresh(node: &Node, sessions: &[String], deadline_ms: u64) {
    if sessions.is_empty() {
        return;
    }
    let list = serde_json::to_string(sessions).expect("sessions serialize");
    let _ = submit_system(
        node,
        vec![WILLS_OBJECT.to_string()],
        vec![
            op(SCHEMA, vec![]),
            op(
                "UPDATE wills SET deadline = ?1 \
                 WHERE session IN (SELECT value FROM json_each(?2))",
                vec![json!(deadline_ms), json!(list)],
            ),
        ],
        true,
    )
    .await;
}

fn rows_affected(resp: &TxnResponse) -> usize {
    match resp.results.last() {
        Some(OpResult::Affected { rows_affected }) => *rows_affected,
        _ => 0,
    }
}

/// One sweep pass: fire every will whose deadline has lapsed and whose
/// claim (if any) has also lapsed. Runs on every node; the claim keeps two
/// sweepers from firing the same will at once, and the claim's own deadline
/// lets a second sweeper reclaim one whose claimer died mid-fire.
pub async fn sweep(node: &Node, now_ms: u64, claim_ttl_ms: u64) {
    // SELECT on a fresh cluster errors "no such table"; treat as empty.
    let listing = submit_system(
        node,
        vec![WILLS_OBJECT.to_string()],
        vec![op(
            "SELECT session, payload FROM wills \
             WHERE deadline < ?1 AND claim_deadline < ?1 ORDER BY session",
            vec![json!(now_ms)],
        )],
        false,
    )
    .await;
    let Ok(resp) = listing else { return };
    let Some(OpResult::Rows { rows }) = resp.results.into_iter().next() else {
        return;
    };

    for row in rows {
        let (Some(session), Some(payload)) = (row["session"].as_str(), row["payload"].as_str())
        else {
            continue;
        };

        // Claim: exactly one sweeper wins the CAS-like conditional update.
        let claim = submit_system(
            node,
            vec![WILLS_OBJECT.to_string()],
            vec![op(
                "UPDATE wills SET claim_deadline = ?1 \
                 WHERE session = ?2 AND deadline < ?3 AND claim_deadline < ?3",
                vec![json!(now_ms + claim_ttl_ms), json!(session), json!(now_ms)],
            )],
            false,
        )
        .await;
        if !claim.map(|r| rows_affected(&r) == 1).unwrap_or(false) {
            continue; // someone else is firing it, or it was disarmed
        }

        let Ok(will) = serde_json::from_str::<DurableWill>(payload) else {
            // Corrupt payload: consume it so it stops tripping the sweep.
            eprintln!("wills: dropping unparseable will for {session}");
            let _ = submit_system(
                node,
                vec![WILLS_OBJECT.to_string()],
                vec![op("DELETE FROM wills WHERE session = ?1", vec![json!(session)])],
                false,
            )
            .await;
            continue;
        };

        // Fire under the frozen capability (the authorizer still gates
        // every action), then consume the durable record.
        eprintln!("wills: firing crash-orphaned will {session}");
        let fired = crate::cluster::submit_as(
            node,
            will.cap.map(std::sync::Arc::new),
            will.objects,
            will.ops,
            false,
            will.optimistic,
        )
        .await;
        if fired.is_ok() {
            let _ = submit_system(
                node,
                vec![WILLS_OBJECT.to_string()],
                vec![op("DELETE FROM wills WHERE session = ?1", vec![json!(session)])],
                false,
            )
            .await;
        }
        // On failure we leave the row: the claim lapses and another sweep
        // reclaims it. Idempotent will ops make the replay safe.
    }
}
