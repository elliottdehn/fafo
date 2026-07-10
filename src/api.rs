//! The (borat) very nice (/borat) HTTP API. Any process answers any request;
//! transactions are routed (or proxied over RPC) to the process holding the
//! target logical worker's lease.
//!
//!   POST /txn                   cross-object transaction; participants
//!                               declared up-front in `objects`
//!   POST /objects/{id}/exec     single-object transaction (sugar over /txn)
//!   POST /objects/{id}/query    read-only single statement
//!   GET  /objects               list object ids
//!   GET  /stats                 this process's workers, txns, takes, returns

use crate::cluster::{Node, Op, StatsSnapshot, TxnResponse, submit};
use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: msg.into(),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: msg.into(),
        }
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self::internal(e.to_string())
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        Self::internal(e.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

pub fn router(node: Node) -> Router {
    Router::new()
        .route("/txn", post(txn_handler))
        .route("/objects", get(list_objects))
        .route("/objects/{id}/exec", post(exec_handler))
        .route("/objects/{id}/query", post(query_handler))
        .route("/stats", get(stats_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            node.clone(),
            require_api_token,
        ))
        // Liveness must stay unauthenticated: lease claiming health-checks
        // peers with it, and the platform pings it.
        .route("/healthz", get(healthz))
        .route("/internal/rpc", post(rpc_handler))
        .with_state(node)
}

/// Public API auth: if API_TOKEN is configured, require it as a bearer.
async fn require_api_token(
    State(node): State<Node>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if let Some(token) = &node.api_token {
        let ok = req
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|t| t == token);
        if !ok {
            return ApiError {
                status: StatusCode::UNAUTHORIZED,
                message: "missing or invalid bearer token".into(),
            }
            .into_response();
        }
    }
    next.run(req).await
}

async fn healthz(State(node): State<Node>) -> Json<Value> {
    Json(json!({ "ok": true, "workers": node.claimed().len() }))
}

/// Inter-node RPC endpoint. Guarded by the cluster secret, not the API token
/// — nodes talk to each other regardless of how the public edge is secured.
async fn rpc_handler(
    State(node): State<Node>,
    headers: axum::http::HeaderMap,
    Json(req): Json<crate::rpc::Request>,
) -> Result<Json<crate::rpc::Response>, ApiError> {
    let authed = headers
        .get(crate::rpc::SECRET_HEADER)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s == node.secret);
    if !authed {
        return Err(ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "bad cluster secret".into(),
        });
    }
    use crate::rpc::{Request as Rpc, Response as RpcResp};
    use crate::worker::WorkerMsg;
    use tokio::sync::oneshot;
    let resp = match req {
        Rpc::Txn {
            objects,
            ops,
            read_only,
        } => RpcResp::Txn(
            crate::cluster::submit_routed(&node, objects, ops, read_only)
                .await
                .map_err(crate::rpc::WireError::from),
        ),
        Rpc::Take {
            worker,
            object,
            taker,
        } => {
            let tx = node.local.read().unwrap().get(&worker).cloned();
            match tx {
                Some(tx) => {
                    let (rtx, rrx) = oneshot::channel();
                    if tx
                        .send(WorkerMsg::Take {
                            object,
                            taker,
                            resp: rtx,
                        })
                        .is_ok()
                        && let Ok(result) = rrx.await
                    {
                        RpcResp::Take(result)
                    } else {
                        RpcResp::Err("worker is gone".into())
                    }
                }
                None => RpcResp::Take(Err(crate::cluster::TakeError::NotMine { hint: None })),
            }
        }
        Rpc::Adopt {
            worker,
            object,
            meta,
        } => {
            let tx = node.local.read().unwrap().get(&worker).cloned();
            match tx {
                Some(tx) if tx.send(WorkerMsg::Adopt { object, meta }).is_ok() => RpcResp::Ok,
                _ => RpcResp::Err("worker is gone".into()),
            }
        }
    };
    Ok(Json(resp))
}

#[derive(Deserialize)]
struct TxnRequest {
    objects: Vec<String>,
    ops: Vec<Op>,
}

async fn txn_handler(
    State(node): State<Node>,
    Json(req): Json<TxnRequest>,
) -> Result<Json<TxnResponse>, ApiError> {
    Ok(Json(submit(&node, req.objects, req.ops, false).await?))
}

#[derive(Deserialize)]
struct Statement {
    sql: String,
    #[serde(default)]
    params: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ExecBody {
    Single(Statement),
    Many { ops: Vec<Statement> },
}

async fn exec_handler(
    State(node): State<Node>,
    UrlPath(id): UrlPath<String>,
    Json(body): Json<ExecBody>,
) -> Result<Json<TxnResponse>, ApiError> {
    let stmts = match body {
        ExecBody::Single(s) => vec![s],
        ExecBody::Many { ops } => ops,
    };
    let ops = stmts
        .into_iter()
        .map(|s| Op {
            object: id.clone(),
            sql: s.sql,
            params: s.params,
        })
        .collect();
    Ok(Json(submit(&node, vec![id], ops, false).await?))
}

async fn query_handler(
    State(node): State<Node>,
    UrlPath(id): UrlPath<String>,
    Json(stmt): Json<Statement>,
) -> Result<Json<Value>, ApiError> {
    let ops = vec![Op {
        object: id.clone(),
        sql: stmt.sql,
        params: stmt.params,
    }];
    let mut res = submit(&node, vec![id], ops, true).await?;
    let result = res
        .results
        .pop()
        .ok_or_else(|| ApiError::internal("no result"))?;
    Ok(Json(serde_json::to_value(result)?))
}

async fn stats_handler(State(node): State<Node>) -> Json<StatsSnapshot> {
    Json(node.stats().await)
}

async fn list_objects(State(node): State<Node>) -> Result<Json<Value>, ApiError> {
    let keys = node.store.list("objects/").await?;
    let ids: Vec<String> = keys
        .iter()
        .filter_map(|k| Some(k.strip_prefix("objects/")?.strip_suffix(".db")?.to_string()))
        .collect();
    Ok(Json(json!({ "objects": ids })))
}
