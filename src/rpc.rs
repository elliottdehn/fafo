//! Inter-node RPC over HTTP: POST {peer}/internal/rpc with a JSON body,
//! authenticated by the cluster secret header. HTTP (not raw TCP) because
//! Cloudflare Containers have no direct container-to-container TCP — traffic
//! between instances must ride something the Worker can route. Locally it's
//! just another route on the same axum server.
//!
//! Messages: Health (unused over RPC — liveness is GET /healthz, which must
//! work without the secret), Txn (whole transaction proxied to the target's
//! node), Take (quiesce + transfer object ownership), Adopt (hysteresis
//! return-to-home).

use crate::api::ApiError;
use crate::cluster::{Node, Op, TakeError, TransferMeta, TxnResponse};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub const SECRET_HEADER: &str = "x-fafo-cluster-secret";
const CALL_TIMEOUT: Duration = Duration::from_secs(30);
const HEALTH_TIMEOUT: Duration = Duration::from_millis(800);

#[derive(Serialize, Deserialize)]
pub enum Request {
    Txn {
        objects: Vec<String>,
        ops: Vec<Op>,
        read_only: bool,
        #[serde(default)]
        optimistic: bool,
        /// The caller's capability travels with a proxied txn so the owning
        /// node's authorizer enforces the same verbs. The RPC channel is
        /// cluster-secret-authed, so the grants are trusted as presented.
        #[serde(default)]
        cap: Option<crate::grants::Capability>,
    },
    Take {
        worker: usize,
        object: String,
        taker: usize,
    },
    Adopt {
        worker: usize,
        object: String,
        meta: TransferMeta,
    },
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    Ok,
    Txn(Result<TxnResponse, WireError>),
    Take(Result<TransferMeta, TakeError>),
    Err(String),
}

#[derive(Serialize, Deserialize)]
pub struct WireError {
    pub status: u16,
    pub message: String,
}

impl From<ApiError> for WireError {
    fn from(e: ApiError) -> Self {
        Self {
            status: e.status.as_u16(),
            message: e.message,
        }
    }
}

impl From<WireError> for ApiError {
    fn from(e: WireError) -> Self {
        ApiError {
            status: axum::http::StatusCode::from_u16(e.status)
                .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR),
            message: e.message,
        }
    }
}

async fn call(node: &Node, base: &str, req: &Request) -> anyhow::Result<Response> {
    let resp = node
        .http
        .post(format!("{base}/internal/rpc"))
        .header(SECRET_HEADER, &node.secret)
        .json(req)
        .timeout(CALL_TIMEOUT)
        .send()
        .await?;
    anyhow::ensure!(
        resp.status().is_success(),
        "rpc to {base} returned {}",
        resp.status()
    );
    Ok(resp.json().await?)
}

/// Liveness probe used by lease claiming. Deliberately unauthenticated
/// (GET /healthz) so a node with a rotated secret still reads as alive.
pub async fn health(node: &Node, base: &str) -> bool {
    node.http
        .get(format!("{base}/healthz"))
        .timeout(HEALTH_TIMEOUT)
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

pub async fn forward_txn(
    node: &Node,
    base: &str,
    cap: Option<crate::grants::Capability>,
    objects: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
    optimistic: bool,
) -> Result<TxnResponse, ApiError> {
    match call(
        node,
        base,
        &Request::Txn {
            objects,
            ops,
            read_only,
            optimistic,
            cap,
        },
    )
    .await
    {
        Ok(Response::Txn(result)) => result.map_err(ApiError::from),
        Ok(Response::Err(e)) => Err(ApiError::internal(e)),
        Ok(_) => Err(ApiError::internal("rpc protocol mismatch")),
        Err(e) => Err(ApiError::internal(format!("rpc to {base} failed: {e}"))),
    }
}

pub async fn take(
    node: &Node,
    base: &str,
    worker: usize,
    object: &str,
    taker: usize,
) -> anyhow::Result<Result<TransferMeta, TakeError>> {
    match call(
        node,
        base,
        &Request::Take {
            worker,
            object: object.to_string(),
            taker,
        },
    )
    .await?
    {
        Response::Take(result) => Ok(result),
        Response::Err(e) => Ok(Err(TakeError::Failed(e))),
        _ => anyhow::bail!("rpc protocol mismatch"),
    }
}

pub async fn adopt(
    node: &Node,
    base: &str,
    worker: usize,
    object: String,
    meta: TransferMeta,
) -> anyhow::Result<()> {
    match call(node, base, &Request::Adopt { worker, object, meta }).await? {
        Response::Ok => Ok(()),
        Response::Err(e) => anyhow::bail!(e),
        _ => anyhow::bail!("rpc protocol mismatch"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_errors_carry_status_across_the_boundary() {
        let api = ApiError::bad_request("nope");
        let wire = WireError::from(api);
        assert_eq!(wire.status, 400);
        let back = ApiError::from(wire);
        assert_eq!(back.status, axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(back.message, "nope");

        // A status a peer invented must degrade safely, not panic. (Codes
        // 100-999 are all representable; only true garbage hits the 500.)
        let alien = WireError { status: 42, message: "corrupt".into() };
        assert_eq!(ApiError::from(alien).status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Scripted replies: (status, body) pairs served in order.
    type Script = std::sync::Arc<std::sync::Mutex<std::collections::VecDeque<(u16, String)>>>;

    /// A peer that answers /internal/rpc from a script — wrong statuses,
    /// wrong variants — to prove every mismatch degrades to a clean error.
    async fn scripted_peer(script: Script) -> String {
        use axum::extract::State;
        async fn answer(
            State(script): State<Script>,
        ) -> (axum::http::StatusCode, [(&'static str, &'static str); 1], String) {
            let (status, body) = script.lock().unwrap().pop_front().expect("scripted reply");
            (
                axum::http::StatusCode::from_u16(status).unwrap(),
                [("content-type", "application/json")],
                body,
            )
        }
        let app = axum::Router::new()
            .route("/internal/rpc", axum::routing::post(answer))
            .with_state(script);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn wire_failures_and_mismatches_become_clean_errors() {
        let dir = tempfile::tempdir().unwrap();
        let node = crate::cluster::start(crate::cluster::NodeConfig {
            store: std::sync::Arc::new(
                crate::store::FsBlobStore::new(dir.path().join("blobs")).unwrap(),
            ),
            live_dir: dir.path().join("live"),
            logical: 4,
            claim: crate::cluster::ClaimSpec::All,
            bind: "127.0.0.1:0".into(),
            advertise: None,
            hysteresis: 200,
            secret: "s".into(),
            api_token: None,
            max_unshipped: crate::cluster::DEFAULT_MAX_UNSHIPPED,
            limits: crate::limits::Limits::detect(),
            fence_ttl: Duration::from_secs(60),
        })
        .await
        .unwrap();
        let script = Script::default();
        let peer = scripted_peer(script.clone()).await;
        let push = |status: u16, body: &str| {
            script.lock().unwrap().push_back((status, body.to_string()));
        };
        let meta = || TransferMeta {
            settled: false,
            home: 0,
            visit: None,
        };

        // HTTP-level failure: the status is in the error, per call site.
        push(500, "{}");
        let err = forward_txn(&node, &peer, None, vec!["a".into()], vec![], false, false)
            .await
            .unwrap_err();
        assert!(err.message.contains("rpc to"), "{}", err.message);
        push(500, "{}");
        assert!(take(&node, &peer, 0, "o", 1).await.is_err());
        push(500, "{}");
        assert!(adopt(&node, &peer, 0, "o".into(), meta()).await.is_err());

        // Application-level Err payloads map to each call's error shape.
        push(200, r#"{"Err":"boom"}"#);
        let err = forward_txn(&node, &peer, None, vec!["a".into()], vec![], false, false)
            .await
            .unwrap_err();
        assert_eq!(err.message, "boom");
        push(200, r#"{"Err":"boom"}"#);
        match take(&node, &peer, 0, "o", 1).await.unwrap() {
            Err(TakeError::Failed(e)) => assert_eq!(e, "boom"),
            other => panic!("expected Failed, got {other:?}"),
        }
        push(200, r#"{"Err":"boom"}"#);
        let err = adopt(&node, &peer, 0, "o".into(), meta()).await.unwrap_err();
        assert!(err.to_string().contains("boom"));

        // Wrong-variant replies are protocol mismatches, never panics.
        push(200, r#""Ok""#);
        let err = forward_txn(&node, &peer, None, vec!["a".into()], vec![], false, false)
            .await
            .unwrap_err();
        assert!(err.message.contains("protocol mismatch"), "{}", err.message);
        push(200, r#"{"Txn":{"Ok":{"txn_id":"x","results":[]}}}"#);
        assert!(take(&node, &peer, 0, "o", 1).await.is_err());
        push(200, r#"{"Take":{"Err":{"Failed":"x"}}}"#);
        assert!(adopt(&node, &peer, 0, "o".into(), meta()).await.is_err());
        node.shutdown().await;
    }

    #[test]
    fn requests_survive_json_and_old_peers_can_omit_new_fields() {
        // A Txn from a peer built before `optimistic`/`cap` existed.
        let old = r#"{"Txn":{"objects":["a"],"ops":[],"read_only":false}}"#;
        let req: Request = serde_json::from_str(old).unwrap();
        match req {
            Request::Txn { optimistic, cap, objects, .. } => {
                assert!(!optimistic, "defaults off");
                assert!(cap.is_none(), "defaults to root");
                assert_eq!(objects, vec!["a"]);
            }
            _ => panic!("wrong variant"),
        }
    }
}
