//! The (borat) very nice (/borat) HTTP API. Any process answers any request;
//! transactions are routed (or proxied over RPC) to the process holding the
//! target logical worker's lease.
//!
//!   POST /txn                   cross-object transaction; participants
//!                               declared up-front in `objects`
//!   POST /objects/{id}/exec     single-object transaction (sugar over /txn)
//!   POST /objects/{id}/query    read-only single statement
//!   POST /objects/{id}/poll     long-poll: replies when the query's
//!                               condition holds (non-empty, or hash
//!                               differs from `baseline`)
//!   GET  /objects               list object ids
//!   GET  /stats                 this process's workers, txns, takes, returns

use crate::cluster::{
    Node, Op, StatsSnapshot, TxnResponse, cancel_poll, submit_as, submit_poll, validate_txn,
};
use crate::grants;
use axum::Extension;
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

/// Who is calling: the root API token (all-powerful), or a capability
/// token minted by the root holder with per-object, per-verb grants —
/// safe to hand to untrusted end-user devices. No token configured means
/// an open node (local dev): everyone is root.
#[derive(Clone)]
pub enum Auth {
    Root,
    Cap(std::sync::Arc<grants::Capability>),
}

impl Auth {
    /// The capability to attach to a txn (None for root = unrestricted).
    fn cap(&self) -> Option<std::sync::Arc<grants::Capability>> {
        match self {
            Auth::Root => None,
            Auth::Cap(c) => Some(c.clone()),
        }
    }
}

fn authorize(auth: &Auth, objects: &[String], verb: &str) -> Result<(), ApiError> {
    match auth {
        Auth::Root => Ok(()),
        Auth::Cap(cap) => {
            for object in objects {
                if !grants::allows(cap, object, verb) {
                    return Err(ApiError {
                        status: StatusCode::FORBIDDEN,
                        message: format!("capability does not grant {verb:?} on {object:?}"),
                    });
                }
            }
            Ok(())
        }
    }
}

fn require_root(auth: &Auth) -> Result<(), ApiError> {
    match auth {
        Auth::Root => Ok(()),
        Auth::Cap(_) => Err(ApiError {
            status: StatusCode::FORBIDDEN,
            message: "root API token required".into(),
        }),
    }
}

pub fn router(node: Node) -> Router {
    Router::new()
        .route("/txn", post(txn_handler))
        .route("/objects", get(list_objects))
        .route("/objects/{id}/exec", post(exec_handler))
        .route("/objects/{id}/query", post(query_handler))
        .route("/objects/{id}/poll", post(poll_handler))
        .route("/grant", post(grant_handler))
        .route("/stats", get(stats_handler))
        .route_layer(axum::middleware::from_fn_with_state(
            node.clone(),
            require_api_token,
        ))
        // Liveness must stay unauthenticated: lease claiming health-checks
        // peers with it, and the platform pings it.
        .route("/healthz", get(healthz))
        .route("/internal/rpc", post(rpc_handler))
        // The database-connection experience: one WebSocket, many
        // transactions as frames. After the upgrade, frames bypass the
        // per-request platform machinery entirely — this is the low-latency
        // production path. Auth via Authorization: Bearer or the
        // fafo-token.<TOKEN> subprotocol (never the URL), checked in the
        // handler.
        .route("/ws", get(ws_handler))
        .with_state(node)
}

#[derive(Deserialize)]
struct WsFrame {
    /// Client-chosen correlation id, echoed in the reply.
    id: u64,
    /// May be omitted: derived from the ops' objects.
    #[serde(default)]
    objects: Vec<String>,
    #[serde(default)]
    ops: Vec<Op>,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    optimistic: bool,
    /// Long-poll: the reply is held until the query's condition holds.
    #[serde(default)]
    poll: Option<PollBody>,
    /// Abandon the outstanding poll originally sent with this frame's id.
    #[serde(default)]
    cancel: bool,
    /// Last-will transaction: runs when this socket dies, MQTT-style.
    /// One will per connection; re-arming replaces it, empty ops disarm.
    #[serde(default)]
    will: Option<WillBody>,
}

#[derive(Deserialize)]
struct WillBody {
    /// May be omitted: derived from the ops' objects.
    #[serde(default)]
    objects: Vec<String>,
    /// Empty = disarm the current will.
    #[serde(default)]
    ops: Vec<Op>,
    #[serde(default)]
    optimistic: bool,
}

/// An armed will, validated at registration so a bad one is rejected while
/// the client can still hear about it.
struct ArmedWill {
    objects: Vec<String>,
    ops: Vec<Op>,
    optimistic: bool,
    /// The grants frozen at arm time. The will runs under them even if the
    /// token has since expired: the server is keeping a promise it already
    /// authorized, not accepting a new request.
    cap: Option<std::sync::Arc<grants::Capability>>,
}

#[derive(Deserialize)]
struct PollBody {
    object: String,
    sql: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
    /// Judge the condition only against durable (shipped) state.
    #[serde(default)]
    durable: bool,
    /// Change detection: reply when the result hash differs from this
    /// (from the previous reply). "" bootstraps with an immediate snapshot.
    #[serde(default)]
    baseline: Option<String>,
}

async fn ws_handler(
    State(node): State<Node>,
    headers: axum::http::HeaderMap,
    ws: axum::extract::ws::WebSocketUpgrade,
) -> Response {
    // Never in the URL — query strings live forever in access logs. Accept
    // the token via Authorization (clients that can set headers) or the
    // subprotocol smuggle `fafo-token.<TOKEN>` (the one header browsers CAN
    // set on a WebSocket). The server selects plain "fafo" back. Either
    // slot takes the root token or a capability token; grants are then
    // enforced per frame.
    let auth = match &node.api_token {
        None => Some(Auth::Root),
        Some(token) => {
            let bearer = headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "))
                .and_then(|t| resolve_auth(&node, token, t));
            let subproto = || {
                headers
                    .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
                    .and_then(|v| v.to_str().ok())
                    .into_iter()
                    .flat_map(|protos| protos.split(',').map(str::trim))
                    .filter_map(|p| p.strip_prefix("fafo-token."))
                    .find_map(|t| resolve_auth(&node, token, t))
            };
            bearer.or_else(subproto)
        }
    };
    let Some(auth) = auth else {
        return ApiError {
            status: StatusCode::UNAUTHORIZED,
            message:
                "authenticate via Authorization: Bearer or the fafo-token.<TOKEN> subprotocol"
                    .into(),
        }
        .into_response();
    };
    ws.protocols(["fafo"])
        .on_upgrade(move |socket| ws_conn(node, socket, auth))
}

/// Connection ids disambiguate parked polls across sockets (and HTTP
/// requests) for cancellation.
fn next_conn_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

async fn ws_conn(node: Node, socket: axum::extract::ws::WebSocket, auth: Auth) {
    use axum::extract::ws::Message;
    use futures::{SinkExt, StreamExt};
    let (mut sink, mut stream) = socket.split();
    // Frames execute concurrently (pipelining, like a real DB connection);
    // a single writer task serializes replies onto the socket.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        while let Some(reply) = rx.recv().await {
            if sink.send(Message::Text(reply.into())).await.is_err() {
                break;
            }
        }
    });
    let conn = next_conn_id();
    // frame id -> object, for polls parked right now on this connection.
    // Cancel frames look up here; socket teardown cancels the rest so a
    // dead client's polls don't linger until the object's next write.
    let outstanding: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<u64, String>>> =
        Default::default();
    let will: std::sync::Arc<std::sync::Mutex<Option<ArmedWill>>> = Default::default();
    while let Some(Ok(msg)) = stream.next().await {
        let Message::Text(text) = msg else {
            if matches!(msg, Message::Close(_)) {
                break;
            }
            continue;
        };
        let node = node.clone();
        let tx = tx.clone();
        let outstanding = outstanding.clone();
        let will = will.clone();
        let auth = auth.clone();
        tokio::spawn(async move {
            if let Some(reply) =
                handle_ws_frame(&node, &auth, conn, &outstanding, &will, text.as_str()).await
            {
                let _ = tx.send(reply);
            }
        });
    }
    // The socket is dead: execute the will first (an ordinary transaction —
    // it may well wake polls parked by the living), then sweep our own.
    let armed = will.lock().unwrap().take(); // guard drops before the await
    if let Some(w) = armed
        && let Err(e) = submit_as(&node, w.cap, w.objects, w.ops, false, w.optimistic).await
    {
        eprintln!("conn {conn}: last-will transaction failed: {}", e.message);
    }
    for (frame, object) in outstanding.lock().unwrap().drain() {
        cancel_poll(&node, &object, conn, frame);
    }
    drop(tx);
    let _ = writer.await;
}

async fn handle_ws_frame(
    node: &Node,
    auth: &Auth,
    conn: u64,
    outstanding: &std::sync::Mutex<std::collections::HashMap<u64, String>>,
    will: &std::sync::Mutex<Option<ArmedWill>>,
    text: &str,
) -> Option<String> {
    fn deny(id: u64, e: ApiError) -> Option<String> {
        Some(json!({ "id": id, "error": e.message, "status": e.status.as_u16() }).to_string())
    }
    let frame: WsFrame = match serde_json::from_str(text) {
        Ok(f) => f,
        Err(e) => {
            return Some(json!({ "id": null, "error": format!("bad frame: {e}") }).to_string());
        }
    };
    if frame.cancel {
        // The canceled poll's own task replies with its error; no ack here.
        if let Some(object) = outstanding.lock().unwrap().remove(&frame.id) {
            cancel_poll(node, &object, conn, frame.id);
        }
        return None;
    }
    if let Some(w) = frame.will {
        if w.ops.is_empty() {
            *will.lock().unwrap() = None;
            return Some(json!({ "id": frame.id, "result": { "will": "disarmed" } }).to_string());
        }
        let mut objects = w.objects;
        if objects.is_empty() {
            objects = w.ops.iter().map(|op| op.object.clone()).collect();
        }
        return Some(match validate_txn(objects, &w.ops) {
            // A will is a deferred write: gate it at arm time, while the
            // client can still hear the refusal.
            Ok(ids) => match authorize(auth, &ids, "write") {
                Ok(()) => {
                    *will.lock().unwrap() = Some(ArmedWill {
                        objects: ids,
                        ops: w.ops,
                        optimistic: w.optimistic,
                        cap: auth.cap(),
                    });
                    json!({ "id": frame.id, "result": { "will": "armed" } }).to_string()
                }
                Err(e) => return deny(frame.id, e),
            },
            Err(e) => json!({ "id": frame.id, "error": e.message, "status": e.status.as_u16() })
                .to_string(),
        });
    }
    if let Some(poll) = frame.poll {
        if let Err(e) = authorize(auth, std::slice::from_ref(&poll.object), "poll") {
            return deny(frame.id, e);
        }
        outstanding
            .lock()
            .unwrap()
            .insert(frame.id, poll.object.clone());
        let out = submit_poll(
            node,
            poll.object,
            poll.sql,
            poll.params,
            poll.durable,
            poll.baseline,
            conn,
            frame.id,
        )
        .await;
        outstanding.lock().unwrap().remove(&frame.id);
        return Some(match out {
            Ok(result) => json!({ "id": frame.id, "result": result }).to_string(),
            Err(e) => json!({ "id": frame.id, "error": e.message, "status": e.status.as_u16() })
                .to_string(),
        });
    }
    let mut objects = frame.objects;
    if objects.is_empty() {
        objects = frame.ops.iter().map(|op| op.object.clone()).collect();
    }
    let verb = if frame.read_only { "read" } else { "write" };
    if let Err(e) = authorize(auth, &objects, verb) {
        return deny(frame.id, e);
    }
    Some(
        match submit_as(
            node,
            auth.cap(),
            objects,
            frame.ops,
            frame.read_only,
            frame.optimistic,
        )
        .await
        {
            Ok(result) => json!({ "id": frame.id, "result": result }).to_string(),
            Err(e) => json!({ "id": frame.id, "error": e.message, "status": e.status.as_u16() })
                .to_string(),
        },
    )
}

/// Public API auth: if API_TOKEN is configured, the bearer must be either
/// it (root) or a valid capability token (grants enforced per handler).
async fn require_api_token(
    State(node): State<Node>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let auth = match &node.api_token {
        None => Some(Auth::Root),
        Some(token) => req
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .and_then(|t| resolve_auth(&node, token, t)),
    };
    match auth {
        Some(auth) => {
            req.extensions_mut().insert(auth);
            next.run(req).await
        }
        None => ApiError {
            status: StatusCode::UNAUTHORIZED,
            message: "missing or invalid bearer token".into(),
        }
        .into_response(),
    }
}

fn resolve_auth(node: &Node, root_token: &str, presented: &str) -> Option<Auth> {
    if presented == root_token {
        return Some(Auth::Root);
    }
    grants::verify(&node.secret, presented)
        .map(|cap| Auth::Cap(std::sync::Arc::new(cap)))
}

async fn healthz(State(node): State<Node>) -> Json<Value> {
    // Location vars are injected by Cloudflare Containers; geography is the
    // dominant latency term (measured 0.11s..0.9s per-instance), so make it
    // visible.
    Json(json!({
        "ok": true,
        "workers": node.claimed_workers(),
        "location": std::env::var("CLOUDFLARE_LOCATION").unwrap_or_default(),
        "region": std::env::var("CLOUDFLARE_REGION").unwrap_or_default(),
    }))
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
            optimistic,
            cap,
        } => RpcResp::Txn(
            crate::cluster::submit_routed(
                &node,
                cap.map(std::sync::Arc::new),
                objects,
                ops,
                read_only,
                optimistic,
            )
            .await
            .map_err(crate::rpc::WireError::from),
        ),
        Rpc::Take {
            worker,
            object,
            taker,
        } => {
            let tx = crate::cluster::local_sender(&node, worker);
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
            let tx = crate::cluster::local_sender(&node, worker);
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
    /// Ack after local apply; durability rides the next boat. A crash in
    /// the shipping window loses the txn (with everything after it,
    /// consistently). Default false = ack only when durable.
    #[serde(default)]
    optimistic: bool,
}

async fn txn_handler(
    State(node): State<Node>,
    Extension(auth): Extension<Auth>,
    Json(req): Json<TxnRequest>,
) -> Result<Json<TxnResponse>, ApiError> {
    authorize(&auth, &req.objects, "write")?;
    Ok(Json(
        submit_as(&node, auth.cap(), req.objects, req.ops, false, req.optimistic).await?,
    ))
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
    Many {
        ops: Vec<Statement>,
        #[serde(default)]
        optimistic: bool,
    },
    Single(Statement),
}

async fn exec_handler(
    State(node): State<Node>,
    Extension(auth): Extension<Auth>,
    UrlPath(id): UrlPath<String>,
    Json(body): Json<ExecBody>,
) -> Result<Json<TxnResponse>, ApiError> {
    authorize(&auth, std::slice::from_ref(&id), "write")?;
    let (stmts, optimistic) = match body {
        ExecBody::Single(s) => (vec![s], false),
        ExecBody::Many { ops, optimistic } => (ops, optimistic),
    };
    let ops = stmts
        .into_iter()
        .map(|s| Op {
            object: id.clone(),
            sql: s.sql,
            params: s.params,
        })
        .collect();
    Ok(Json(
        submit_as(&node, auth.cap(), vec![id], ops, false, optimistic).await?,
    ))
}

async fn query_handler(
    State(node): State<Node>,
    Extension(auth): Extension<Auth>,
    UrlPath(id): UrlPath<String>,
    Json(stmt): Json<Statement>,
) -> Result<Json<Value>, ApiError> {
    authorize(&auth, std::slice::from_ref(&id), "read")?;
    let ops = vec![Op {
        object: id.clone(),
        sql: stmt.sql,
        params: stmt.params,
    }];
    let mut res = submit_as(&node, auth.cap(), vec![id], ops, true, false).await?;
    let result = res
        .results
        .pop()
        .ok_or_else(|| ApiError::internal("no result"))?;
    Ok(Json(serde_json::to_value(result)?))
}

#[derive(Deserialize)]
struct PollRequest {
    sql: String,
    #[serde(default)]
    params: Vec<Value>,
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    baseline: Option<String>,
}

/// HTTP long-poll: the response is held open until the condition holds.
/// If the client gives up (drops the request), the parked poll is swept
/// lazily on the object's next write. The WS frame is the production path;
/// this is the curl-able one.
async fn poll_handler(
    State(node): State<Node>,
    Extension(auth): Extension<Auth>,
    UrlPath(id): UrlPath<String>,
    Json(req): Json<PollRequest>,
) -> Result<Json<Value>, ApiError> {
    authorize(&auth, std::slice::from_ref(&id), "poll")?;
    let mut res = submit_poll(
        &node,
        id,
        req.sql,
        req.params,
        req.durable,
        req.baseline,
        next_conn_id(),
        0,
    )
    .await?;
    let result = res
        .results
        .pop()
        .ok_or_else(|| ApiError::internal("no result"))?;
    let mut out = serde_json::to_value(result)?;
    if let (Value::Object(map), Some(hash)) = (&mut out, res.hash) {
        map.insert("hash".into(), Value::String(hash));
    }
    Ok(Json(out))
}

async fn stats_handler(
    State(node): State<Node>,
    Extension(auth): Extension<Auth>,
) -> Result<Json<StatsSnapshot>, ApiError> {
    require_root(&auth)?;
    Ok(Json(node.stats().await))
}

#[derive(Deserialize)]
struct GrantRequest {
    grants: Vec<grants::Grant>,
    /// Keep these short — revocation is expiry (verification is stateless).
    ttl_secs: u64,
    #[serde(default)]
    sub: Option<String>,
}

/// Mint a capability token: per-object, per-verb grants safe to hand to
/// untrusted end-user devices. Root only — this is your backend's job,
/// done either here or by signing the same envelope itself (see
/// src/grants.rs for the 10-line format).
async fn grant_handler(
    State(node): State<Node>,
    Extension(auth): Extension<Auth>,
    Json(req): Json<GrantRequest>,
) -> Result<Json<Value>, ApiError> {
    require_root(&auth)?;
    if req.grants.is_empty() {
        return Err(ApiError::bad_request("grant at least one capability"));
    }
    for g in &req.grants {
        for v in &g.verbs {
            if !matches!(
                v.as_str(),
                "read" | "insert" | "update" | "delete" | "ddl" | "write" | "poll"
            ) {
                return Err(ApiError::bad_request(format!(
                    "unknown verb {v:?} (read, insert, update, delete, ddl, poll; \
                     write = shorthand for the four write verbs)"
                )));
            }
        }
    }
    let exp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        + req.ttl_secs;
    let cap = grants::Capability {
        grants: req.grants,
        exp,
        sub: req.sub,
    };
    Ok(Json(
        json!({ "token": grants::mint(&node.secret, &cap), "exp": exp }),
    ))
}

async fn list_objects(
    State(node): State<Node>,
    Extension(auth): Extension<Auth>,
) -> Result<Json<Value>, ApiError> {
    require_root(&auth)?;
    let keys = node.store.list("objects/").await?;
    let ids: Vec<String> = keys
        .iter()
        .filter_map(|k| Some(k.strip_prefix("objects/")?.strip_suffix(".db")?.to_string()))
        .collect();
    Ok(Json(json!({ "objects": ids })))
}
