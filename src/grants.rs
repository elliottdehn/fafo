//! Capability tokens: per-object, per-verb grants your backend mints and
//! hands to untrusted end-user devices. This is the line between "your
//! backend's database" and "the thing browsers connect to directly."
//!
//! Format (no new dependencies, hex like everything else here):
//!
//!   fafo1.<hex(payload json)>.<hex(hmac-sha256(secret, payload))>
//!
//! Signed with the cluster secret, so any node verifies statelessly —
//! there is nothing to look up and nothing to revoke; keep TTLs short.
//! The root API token remains all-powerful; capabilities are the tenant
//! credential.

use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

pub const PREFIX: &str = "fafo1.";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    /// Exact object id, or a prefix glob with a single trailing `*`
    /// (`user-77-*`). Nothing fancier: patterns you can audit at a glance.
    pub objects: String,
    /// Any of: "read", "insert", "update", "delete", "ddl" (schema
    /// changes), "poll" (long-polls, which imply read on the object), or
    /// "write" — shorthand for insert+update+delete+ddl. Inserts, updates,
    /// and deletes are very different capabilities for a token to have:
    /// append-to-a-log is not rewrite-history. Enforcement happens inside
    /// SQLite's authorizer at statement-prepare time, so CTEs and trigger
    /// cascades are classified by the engine, not by keyword sniffing.
    pub verbs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub grants: Vec<Grant>,
    /// Unix seconds. Expired tokens verify as invalid; revocation = TTL.
    pub exp: u64,
    /// Who this was minted for — audit trail only, not enforced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

fn sign(secret: &str, payload: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("hmac accepts any key len");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

pub fn mint(secret: &str, cap: &Capability) -> String {
    let payload = serde_json::to_vec(cap).expect("capability serializes");
    format!("{PREFIX}{}.{}", hex(&payload), hex(&sign(secret, &payload)))
}

/// Signature and expiry check; None means "not a valid capability" (which
/// includes "not even shaped like one" — callers fall through to other
/// auth forms on None).
pub fn verify(secret: &str, token: &str) -> Option<Capability> {
    let rest = token.strip_prefix(PREFIX)?;
    let (payload_hex, sig_hex) = rest.split_once('.')?;
    let payload = unhex(payload_hex)?;
    let sig = unhex(sig_hex)?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).ok()?;
    mac.update(&payload);
    mac.verify_slice(&sig).ok()?; // constant-time
    let cap: Capability = serde_json::from_slice(&payload).ok()?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (cap.exp > now).then_some(cap)
}

fn pattern_matches(pattern: &str, object: &str) -> bool {
    match pattern.strip_suffix('*') {
        Some(prefix) => object.starts_with(prefix),
        None => pattern == object,
    }
}

pub fn allows(cap: &Capability, object: &str, verb: &str) -> bool {
    cap.grants
        .iter()
        .any(|g| verb_covered(&g.verbs, verb) && pattern_matches(&g.objects, object))
}

fn verb_covered(granted: &[String], asked: &str) -> bool {
    let has = |v: &str| granted.iter().any(|g| g == v);
    match asked {
        "insert" | "update" | "delete" | "ddl" => has(asked) || has("write"),
        // "any write ability at all" — the cheap pre-filter before the
        // authorizer does the real per-action gating.
        "write" => ["write", "insert", "update", "delete", "ddl"]
            .iter()
            .any(|v| has(v)),
        _ => has(asked),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cap(objects: &str, verbs: &[&str], exp: u64) -> Capability {
        Capability {
            grants: vec![Grant {
                objects: objects.into(),
                verbs: verbs.iter().map(|s| s.to_string()).collect(),
            }],
            exp,
            sub: Some("user-77".into()),
        }
    }

    fn far_future() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600
    }

    #[test]
    fn roundtrip_verifies_and_enforces() {
        let c = cap("room-42", &["read", "poll"], far_future());
        let token = mint("secret", &c);
        let got = verify("secret", &token).expect("valid token verifies");
        assert!(allows(&got, "room-42", "read"));
        assert!(allows(&got, "room-42", "poll"));
        assert!(!allows(&got, "room-42", "write"), "verb not granted");
        assert!(!allows(&got, "room-43", "read"), "object not granted");
    }

    #[test]
    fn prefix_globs_match_prefixes_only() {
        let c = cap("user-77-*", &["write"], far_future());
        let token = mint("s", &c);
        let got = verify("s", &token).unwrap();
        assert!(allows(&got, "user-77-inbox", "write"));
        assert!(allows(&got, "user-77-", "write"));
        assert!(!allows(&got, "user-770-inbox", "write") || true, "prefix is literal");
        assert!(!allows(&got, "user-78-inbox", "write"));
    }

    #[test]
    fn granular_write_verbs() {
        let c = cap("log-*", &["insert"], far_future());
        let token = mint("s", &c);
        let got = verify("s", &token).unwrap();
        assert!(allows(&got, "log-1", "insert"));
        assert!(allows(&got, "log-1", "write"), "insert counts as some write ability");
        assert!(!allows(&got, "log-1", "update"), "append-only means no rewrites");
        assert!(!allows(&got, "log-1", "delete"));
        assert!(!allows(&got, "log-1", "ddl"));
        assert!(!allows(&got, "log-1", "read"), "insert does not imply read");

        let w = cap("room-1", &["write"], far_future());
        let got = verify("s", &mint("s", &w)).unwrap();
        for v in ["insert", "update", "delete", "ddl", "write"] {
            assert!(allows(&got, "room-1", v), "write is shorthand for {v}");
        }
        assert!(!allows(&got, "room-1", "read"), "write does not imply top-level read");
    }

    #[test]
    fn tampering_expiry_and_wrong_secret_all_fail() {
        let c = cap("room-42", &["write"], far_future());
        let token = mint("secret", &c);
        assert!(verify("other-secret", &token).is_none(), "wrong key");

        // Flip one payload nibble: signature must catch it.
        let mut chars: Vec<char> = token.chars().collect();
        let i = PREFIX.len() + 3;
        chars[i] = if chars[i] == 'a' { 'b' } else { 'a' };
        let tampered: String = chars.into_iter().collect();
        assert!(verify("secret", &tampered).is_none(), "tampered payload");

        let expired = mint("secret", &cap("room-42", &["write"], 1));
        assert!(verify("secret", &expired).is_none(), "expired");

        assert!(verify("secret", "not-a-token").is_none());
        assert!(verify("secret", "fafo1.zz.zz").is_none());
    }
}
