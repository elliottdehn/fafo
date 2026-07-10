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
    objects: Vec<String>,
    ops: Vec<Op>,
    read_only: bool,
) -> Result<TxnResponse, ApiError> {
    match call(
        node,
        base,
        &Request::Txn {
            objects,
            ops,
            read_only,
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
