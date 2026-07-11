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
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

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
    Cap(Arc<grants::Capability>),
}

impl Auth {
    /// The capability to attach to a txn (None for root = unrestricted).
    fn cap(&self) -> Option<Arc<grants::Capability>> {
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
    cap: Option<Arc<grants::Capability>>,
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
    let outstanding: Arc<Mutex<HashMap<u64, String>>> =
        Default::default();
    let will: Arc<Mutex<Option<ArmedWill>>> = Default::default();
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
    outstanding: &Mutex<HashMap<u64, String>>,
    will: &Mutex<Option<ArmedWill>>,
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
        .map(|cap| Auth::Cap(Arc::new(cap)))
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
                cap.map(Arc::new),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{ClaimSpec, DEFAULT_MAX_UNSHIPPED, NodeConfig, start};
    use crate::grants::{Capability, Grant};
    use crate::store::{BlobStore, FsBlobStore};
    use reqwest::Method;

    /// Boot a real node (HTTP server on a random port) and return its base URL.
    async fn serve(api_token: Option<&str>) -> (tempfile::TempDir, Node, String) {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn BlobStore> =
            Arc::new(FsBlobStore::new(dir.path().join("blobs")).unwrap());
        let node = start(NodeConfig {
            store,
            live_dir: dir.path().join("live"),
            logical: 4,
            claim: ClaimSpec::All,
            bind: "127.0.0.1:0".into(),
            advertise: None,
            hysteresis: 200,
            secret: "cluster-secret".into(),
            api_token: api_token.map(str::to_string),
            max_unshipped: DEFAULT_MAX_UNSHIPPED,
            limits: crate::limits::Limits::detect(),
            fence_ttl: std::time::Duration::from_secs(60),
        })
        .await
        .unwrap();
        let base = node.advertise.clone();
        (dir, node, base)
    }

    async fn req(method: Method, url: &str, token: Option<&str>, body: Option<Value>) -> (u16, Value) {
        static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
        let mut r = CLIENT.get_or_init(reqwest::Client::new).request(method, url);
        if let Some(t) = token {
            r = r.bearer_auth(t);
        }
        if let Some(b) = body {
            r = r.json(&b);
        }
        let resp = r.send().await.unwrap();
        let status = resp.status().as_u16();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn post(url: &str, token: Option<&str>, body: Value) -> (u16, Value) {
        req(Method::POST, url, token, Some(body)).await
    }

    fn exec_body(sql: &str) -> Value {
        json!({ "sql": sql })
    }

    #[tokio::test]
    async fn healthz_is_open_but_everything_else_is_gated() {
        let (_dir, node, base) = serve(Some("root")).await;
        let (status, v) = req(Method::GET, &format!("{base}/healthz"), None, None).await;
        assert_eq!(status, 200);
        assert_eq!(v["ok"], true);

        for (method, path) in [
            (Method::POST, "/txn"),
            (Method::POST, "/objects/x/exec"),
            (Method::POST, "/objects/x/query"),
            (Method::GET, "/objects"),
            (Method::GET, "/stats"),
            (Method::POST, "/grant"),
        ] {
            let (status, v) = req(method.clone(), &format!("{base}{path}"), None, Some(json!({}))).await;
            assert_eq!(status, 401, "{path} without a token");
            assert!(v["error"].is_string());
            let (status, _) = req(method, &format!("{base}{path}"), Some("wrong"), Some(json!({}))).await;
            assert_eq!(status, 401, "{path} with a bad token");
        }
        node.shutdown().await;
    }

    #[tokio::test]
    async fn root_token_drives_the_whole_http_surface() {
        let (_dir, node, base) = serve(Some("root")).await;
        let root = Some("root");

        // exec: bare-statement sugar, then the ops form.
        let (status, _) = post(
            &format!("{base}/objects/acct/exec"),
            root,
            exec_body("CREATE TABLE account (balance INTEGER NOT NULL CHECK (balance >= 0))"),
        )
        .await;
        assert_eq!(status, 200);
        let (status, v) = post(
            &format!("{base}/objects/acct/exec"),
            root,
            json!({ "ops": [
                { "sql": "INSERT INTO account (balance) VALUES (?1)", "params": [100] }
            ]}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(v["results"][0]["rows_affected"], 1);

        // A cross-object /txn (creates the second object on first write).
        let (status, _) = post(
            &format!("{base}/txn"),
            root,
            json!({
                "objects": ["acct", "acct2"],
                "ops": [
                    { "object": "acct2", "sql": "CREATE TABLE account (balance INTEGER)" },
                    { "object": "acct2", "sql": "INSERT INTO account VALUES (60)" },
                    { "object": "acct", "sql": "UPDATE account SET balance = balance - 60" }
                ]
            }),
        )
        .await;
        assert_eq!(status, 200);

        // query sees the committed state.
        let (status, v) = post(
            &format!("{base}/objects/acct/query"),
            root,
            exec_body("SELECT balance FROM account"),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(v["rows"][0]["balance"], 40);

        // Listing and stats are root-only conveniences.
        let (status, v) = req(Method::GET, &format!("{base}/objects"), root, None).await;
        assert_eq!(status, 200);
        let ids: Vec<&str> = v["objects"].as_array().unwrap().iter().map(|s| s.as_str().unwrap()).collect();
        assert!(ids.contains(&"acct") && ids.contains(&"acct2"));
        let (status, v) = req(Method::GET, &format!("{base}/stats"), root, None).await;
        assert_eq!(status, 200);
        assert!(v["total_txns"].as_u64().unwrap() >= 3);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn capability_tokens_are_scoped_over_http() {
        let (_dir, node, base) = serve(Some("root")).await;
        let root = Some("root");
        post(&format!("{base}/objects/log-1/exec"), root, exec_body("CREATE TABLE t (n INTEGER)")).await;

        let (status, v) = post(
            &format!("{base}/grant"),
            root,
            json!({ "grants": [{ "objects": "log-*", "verbs": ["insert", "read"] }], "ttl_secs": 600 }),
        )
        .await;
        assert_eq!(status, 200);
        let token = v["token"].as_str().unwrap().to_string();
        let cap = Some(token.as_str());

        // Granted: append and read, on matching objects only.
        let (status, _) = post(&format!("{base}/objects/log-1/exec"), cap, exec_body("INSERT INTO t (n) VALUES (1)")).await;
        assert_eq!(status, 200);
        let (status, v) = post(&format!("{base}/objects/log-1/query"), cap, exec_body("SELECT n FROM t")).await;
        assert_eq!(status, 200);
        assert_eq!(v["rows"][0]["n"], 1);

        // The authorizer catches a rewrite at prepare time...
        let (status, v) = post(&format!("{base}/objects/log-1/exec"), cap, exec_body("UPDATE t SET n = 9")).await;
        assert_eq!(status, 400);
        assert!(v["error"].as_str().unwrap().contains("not authorized"));
        // ...and the pre-filter refuses objects outside the grant outright.
        let (status, _) = post(&format!("{base}/objects/other/exec"), cap, exec_body("INSERT INTO t (n) VALUES (1)")).await;
        assert_eq!(status, 403);

        // Root-only surfaces stay root-only.
        for path in ["/objects", "/stats"] {
            let (status, _) = req(Method::GET, &format!("{base}{path}"), cap, None).await;
            assert_eq!(status, 403, "{path} must require root");
        }
        let (status, _) = post(
            &format!("{base}/grant"),
            cap,
            json!({ "grants": [{ "objects": "*", "verbs": ["write"] }], "ttl_secs": 60 }),
        )
        .await;
        assert_eq!(status, 403, "capabilities must not mint capabilities");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn grant_endpoint_validates_its_input() {
        let (_dir, node, base) = serve(Some("root")).await;
        let root = Some("root");
        let (status, v) = post(
            &format!("{base}/grant"),
            root,
            json!({ "grants": [{ "objects": "x", "verbs": ["fly"] }], "ttl_secs": 60 }),
        )
        .await;
        assert_eq!(status, 400);
        assert!(v["error"].as_str().unwrap().contains("fly"));
        let (status, _) = post(&format!("{base}/grant"), root, json!({ "grants": [], "ttl_secs": 60 })).await;
        assert_eq!(status, 400);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn query_rejects_writes_and_invalid_ids_reject_everywhere() {
        let (_dir, node, base) = serve(None).await; // open node: everyone is root
        let (status, v) = post(&format!("{base}/objects/q1/query"), None, exec_body("CREATE TABLE t (n INTEGER)")).await;
        assert_eq!(status, 400);
        assert!(v["error"].as_str().unwrap().contains("read-only"));

        for bad in ["_meta", "has.dot"] {
            let (status, v) = post(&format!("{base}/objects/{bad}/exec"), None, exec_body("SELECT 1")).await;
            assert_eq!(status, 400, "{bad:?}");
            assert!(v["error"].as_str().unwrap().contains("invalid object id"));
        }
        node.shutdown().await;
    }

    #[tokio::test]
    async fn internal_rpc_requires_the_cluster_secret() {
        let (_dir, node, base) = serve(Some("root")).await;
        let rpc_body = json!({ "Txn": {
            "objects": ["rpcobj"],
            "ops": [{ "object": "rpcobj", "sql": "CREATE TABLE t (n INTEGER)", "params": [] }],
            "read_only": false
        }});
        let client = reqwest::Client::new();

        let resp = client.post(format!("{base}/internal/rpc")).json(&rpc_body).send().await.unwrap();
        assert_eq!(resp.status().as_u16(), 401, "no secret header");
        let resp = client
            .post(format!("{base}/internal/rpc"))
            .header(crate::rpc::SECRET_HEADER, "wrong")
            .json(&rpc_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 401, "bad secret");

        let resp = client
            .post(format!("{base}/internal/rpc"))
            .header(crate::rpc::SECRET_HEADER, "cluster-secret")
            .json(&rpc_body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200);
        let v: Value = resp.json().await.unwrap();
        assert!(v["Txn"]["Ok"]["txn_id"].is_string(), "got: {v}");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn http_poll_replies_with_rows_and_hash() {
        let (_dir, node, base) = serve(None).await;
        post(
            &format!("{base}/objects/chan/exec"),
            None,
            json!({ "ops": [
                { "sql": "CREATE TABLE msgs (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)" },
                { "sql": "INSERT INTO msgs (body) VALUES ('hello')" }
            ]}),
        )
        .await;
        let (status, v) = post(
            &format!("{base}/objects/chan/poll"),
            None,
            json!({ "sql": "SELECT body FROM msgs WHERE id > ?1", "params": [0] }),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(v["rows"][0]["body"], "hello");
        assert!(v["hash"].is_string(), "change-detection needs the hash to feed back");
        node.shutdown().await;
    }

    // ------------------------------------------------------------ WS frames
    //
    // The socket is plumbing; the protocol lives in handle_ws_frame, which
    // is testable directly: JSON in, JSON out.

    async fn frame(
        node: &Node,
        auth: &Auth,
        outstanding: &Mutex<HashMap<u64, String>>,
        will: &Mutex<Option<ArmedWill>>,
        text: &str,
    ) -> Option<Value> {
        handle_ws_frame(node, auth, 1, outstanding, will, text)
            .await
            .map(|s| serde_json::from_str(&s).unwrap())
    }

    fn fresh_conn_state() -> (Mutex<HashMap<u64, String>>, Mutex<Option<ArmedWill>>) {
        (Mutex::new(HashMap::new()), Mutex::new(None))
    }

    #[tokio::test]
    async fn ws_frames_run_transactions_and_report_errors() {
        let (_dir, node, _base) = serve(None).await;
        let (outstanding, will) = fresh_conn_state();

        // Objects may be omitted — inferred from the ops.
        let v = frame(&node, &Auth::Root, &outstanding, &will,
            r#"{"id":1,"ops":[{"object":"wsobj","sql":"CREATE TABLE t (n INTEGER)"}]}"#).await.unwrap();
        assert_eq!(v["id"], 1);
        assert!(v["result"]["txn_id"].is_string());

        let v = frame(&node, &Auth::Root, &outstanding, &will,
            r#"{"id":2,"read_only":true,"ops":[{"object":"wsobj","sql":"SELECT COUNT(*) AS c FROM t"}]}"#).await.unwrap();
        assert_eq!(v["result"]["results"][0]["rows"][0]["c"], 0);

        // A failing op reports the rollback with its status, same id.
        let v = frame(&node, &Auth::Root, &outstanding, &will,
            r#"{"id":3,"ops":[{"object":"wsobj","sql":"INSERT INTO missing VALUES (1)"}]}"#).await.unwrap();
        assert_eq!(v["id"], 3);
        assert_eq!(v["status"], 400);
        assert!(v["error"].as_str().unwrap().contains("rolled back"));

        // Unparseable frames still get an answer (id unknown: null).
        let v = frame(&node, &Auth::Root, &outstanding, &will, "not json").await.unwrap();
        assert!(v["id"].is_null());
        assert!(v["error"].as_str().unwrap().contains("bad frame"));
        node.shutdown().await;
    }

    #[tokio::test]
    async fn ws_frames_enforce_capability_verbs() {
        let (_dir, node, _base) = serve(None).await;
        let (outstanding, will) = fresh_conn_state();
        frame(&node, &Auth::Root, &outstanding, &will,
            r#"{"id":1,"ops":[{"object":"wslog","sql":"CREATE TABLE t (n INTEGER)"}]}"#).await;

        let appender = Auth::Cap(Arc::new(Capability {
            grants: vec![Grant { objects: "wslog".into(), verbs: vec!["insert".into()] }],
            exp: u64::MAX,
            sub: None,
        }));
        let v = frame(&node, &appender, &outstanding, &will,
            r#"{"id":2,"ops":[{"object":"wslog","sql":"INSERT INTO t (n) VALUES (1)"}]}"#).await.unwrap();
        assert!(v["result"].is_object(), "insert is granted: {v}");

        let v = frame(&node, &appender, &outstanding, &will,
            r#"{"id":3,"read_only":true,"ops":[{"object":"wslog","sql":"SELECT n FROM t"}]}"#).await.unwrap();
        assert_eq!(v["status"], 403, "insert does not imply read: {v}");

        // A will outside the grant is refused at arm time...
        let v = frame(&node, &appender, &outstanding, &will,
            r#"{"id":4,"will":{"ops":[{"object":"other","sql":"DELETE FROM t"}]}}"#).await.unwrap();
        assert_eq!(v["status"], 403, "will must stay inside the grant: {v}");
        assert!(will.lock().unwrap().is_none());
        // ...and polls need the poll verb explicitly.
        let v = frame(&node, &appender, &outstanding, &will,
            r#"{"id":5,"poll":{"object":"wslog","sql":"SELECT n FROM t"}}"#).await.unwrap();
        assert_eq!(v["status"], 403, "insert does not imply poll: {v}");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn ws_teardown_sweeps_polls_and_survives_a_failing_will() {
        let (_dir, node, base) = serve(None).await;
        post(&format!("{base}/objects/sweep/exec"), None,
            exec_body("CREATE TABLE msgs (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)")).await;
        post(&format!("{base}/objects/quiet/exec"), None,
            exec_body("CREATE TABLE msgs (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)")).await;

        let mut ws = ws_connect(&base, Some("fafo"), None).await.unwrap();
        // Park a poll on an object nothing will touch — only the socket
        // teardown can clean it up — and arm a will that is doomed: its
        // table will be gone by the time the socket dies.
        ws.send(Message::Text(json!({
            "id": 5, "poll": { "object": "quiet", "sql": "SELECT * FROM msgs WHERE id > 99" }
        }).to_string().into())).await.unwrap();
        let v = ws_roundtrip(&mut ws, json!({
            "id": 6, "will": { "ops": [{ "object": "sweep", "sql": "DELETE FROM msgs" }] }
        })).await;
        assert_eq!(v["result"]["will"], "armed");
        post(&format!("{base}/objects/sweep/exec"), None, exec_body("DROP TABLE msgs")).await;

        // Say goodbye properly this time (a Close frame, not a vanish).
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        ws.close(None).await.unwrap();
        drop(ws);

        // The will fails (table dropped) without wedging teardown, and the
        // parked poll is canceled rather than left to rot.
        let mut swept = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let parked: usize = node.stats().await.per_worker.iter().map(|w| w.parked_polls).sum();
            if parked == 0 {
                swept = true;
                break;
            }
        }
        assert!(swept, "socket teardown must sweep its parked polls");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn ws_wills_arm_disarm_and_validate() {
        let (_dir, node, _base) = serve(None).await;
        let (outstanding, will) = fresh_conn_state();

        let v = frame(&node, &Auth::Root, &outstanding, &will,
            r#"{"id":1,"will":{"ops":[{"object":"pres","sql":"DELETE FROM p WHERE u = 's1'"}]}}"#).await.unwrap();
        assert_eq!(v["result"]["will"], "armed");
        assert_eq!(will.lock().unwrap().as_ref().unwrap().objects, vec!["pres"]);

        // Re-arming replaces; empty ops disarm.
        frame(&node, &Auth::Root, &outstanding, &will,
            r#"{"id":2,"will":{"ops":[{"object":"other","sql":"DELETE FROM p"}]}}"#).await.unwrap();
        assert_eq!(will.lock().unwrap().as_ref().unwrap().objects, vec!["other"]);
        let v = frame(&node, &Auth::Root, &outstanding, &will, r#"{"id":3,"will":{"ops":[]}}"#).await.unwrap();
        assert_eq!(v["result"]["will"], "disarmed");
        assert!(will.lock().unwrap().is_none());

        // A bad will is refused at arm time, while the client can hear it.
        let v = frame(&node, &Auth::Root, &outstanding, &will,
            r#"{"id":4,"will":{"ops":[{"object":"_nope","sql":"DELETE FROM p"}]}}"#).await.unwrap();
        assert!(v["error"].as_str().unwrap().contains("invalid object id"));
        assert!(will.lock().unwrap().is_none(), "a refused will must not arm");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn ws_cancel_clears_the_outstanding_entry() {
        let (_dir, node, _base) = serve(None).await;
        let (outstanding, will) = fresh_conn_state();
        outstanding.lock().unwrap().insert(9, "chan".into());

        // Cancel is fire-and-forget: no reply frame, entry gone.
        let reply = frame(&node, &Auth::Root, &outstanding, &will, r#"{"id":9,"cancel":true}"#).await;
        assert!(reply.is_none());
        assert!(outstanding.lock().unwrap().is_empty());

        // Canceling something unknown is a quiet no-op.
        let reply = frame(&node, &Auth::Root, &outstanding, &will, r#"{"id":404,"cancel":true}"#).await;
        assert!(reply.is_none());
        node.shutdown().await;
    }

    // ---------------------------------------------------------- live WS
    //
    // Real sockets via tungstenite: the upgrade handshake (auth lives
    // there), pipelined frames, and the teardown promises (wills, poll
    // sweeps) that only firing a disconnect can prove.

    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, protocol::Message};

    type WsClient = tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >;

    async fn ws_connect(base: &str, subprotocol: Option<&str>, bearer: Option<&str>) -> anyhow::Result<WsClient> {
        let url = format!("{}/ws", base.replace("http://", "ws://"));
        let mut req = url.into_client_request()?;
        if let Some(p) = subprotocol {
            req.headers_mut().insert("sec-websocket-protocol", p.parse()?);
        }
        if let Some(t) = bearer {
            req.headers_mut().insert("authorization", format!("Bearer {t}").parse()?);
        }
        let (ws, resp) = connect_async(req).await?;
        if subprotocol.is_some() {
            assert_eq!(
                resp.headers().get("sec-websocket-protocol").unwrap(),
                "fafo",
                "the server selects the plain protocol back"
            );
        }
        Ok(ws)
    }

    async fn ws_roundtrip(ws: &mut WsClient, frame: Value) -> Value {
        ws.send(Message::Text(frame.to_string().into())).await.unwrap();
        ws_recv(ws).await
    }

    async fn ws_recv(ws: &mut WsClient) -> Value {
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(5), ws.next())
                .await
                .expect("reply within 5s")
                .expect("socket open")
                .unwrap()
            {
                Message::Text(t) => return serde_json::from_str(&t).unwrap(),
                _ => continue, // pings etc.
            }
        }
    }

    #[tokio::test]
    async fn ws_upgrade_authenticates_via_subprotocol_or_bearer() {
        let (_dir, node, base) = serve(Some("root")).await;

        assert!(ws_connect(&base, None, None).await.is_err(), "no credentials");
        assert!(
            ws_connect(&base, Some("fafo, fafo-token.wrong"), None).await.is_err(),
            "bad token in the subprotocol slot"
        );

        // The browser path: token smuggled through the subprotocol header.
        let mut ws = ws_connect(&base, Some("fafo, fafo-token.root"), None).await.unwrap();
        let v = ws_roundtrip(&mut ws, json!({
            "id": 1, "ops": [{ "object": "wsup", "sql": "CREATE TABLE t (n INTEGER)" }]
        })).await;
        assert!(v["result"]["txn_id"].is_string(), "got: {v}");

        // The backend path: a plain Authorization header.
        let mut ws = ws_connect(&base, None, Some("root")).await.unwrap();
        let v = ws_roundtrip(&mut ws, json!({
            "id": 2, "read_only": true,
            "ops": [{ "object": "wsup", "sql": "SELECT COUNT(*) AS c FROM t" }]
        })).await;
        assert_eq!(v["result"]["results"][0]["rows"][0]["c"], 0);
        node.shutdown().await;
    }

    #[tokio::test]
    async fn ws_polls_park_fire_and_cancel_over_a_live_socket() {
        let (_dir, node, base) = serve(None).await;
        let mut ws = ws_connect(&base, Some("fafo"), None).await.unwrap();
        ws_roundtrip(&mut ws, json!({
            "id": 1,
            "ops": [{ "object": "wschan", "sql": "CREATE TABLE msgs (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT)" }]
        })).await;

        // Park a poll; the socket stays fully usable (pipelining) while it
        // waits; a write from elsewhere fires it.
        ws.send(Message::Text(json!({
            "id": 2,
            "poll": { "object": "wschan", "sql": "SELECT body FROM msgs WHERE id > ?1", "params": [0] }
        }).to_string().into())).await.unwrap();
        let v = ws_roundtrip(&mut ws, json!({
            "id": 3, "read_only": true,
            "ops": [{ "object": "wschan", "sql": "SELECT COUNT(*) AS c FROM msgs" }]
        })).await;
        assert_eq!(v["id"], 3, "later frames overtake a parked poll");

        post(&format!("{base}/objects/wschan/exec"), None,
            exec_body("INSERT INTO msgs (body) VALUES ('wake')")).await;
        let v = ws_recv(&mut ws).await;
        assert_eq!(v["id"], 2);
        assert_eq!(v["result"]["results"][0]["rows"][0]["body"], "wake");

        // Cancel: the canceled poll's own reply slot reports it.
        ws.send(Message::Text(json!({
            "id": 9,
            "poll": { "object": "wschan", "sql": "SELECT * FROM msgs WHERE id > 99" }
        }).to_string().into())).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        ws.send(Message::Text(json!({ "id": 9, "cancel": true }).to_string().into())).await.unwrap();
        let v = ws_recv(&mut ws).await;
        assert_eq!(v["id"], 9);
        assert!(v["error"].as_str().unwrap().contains("canceled"), "got: {v}");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn ws_will_fires_when_the_socket_dies() {
        let (_dir, node, base) = serve(None).await;
        post(&format!("{base}/objects/room/exec"), None, json!({ "ops": [
            { "sql": "CREATE TABLE presence (session TEXT PRIMARY KEY)" },
            { "sql": "INSERT INTO presence VALUES ('s-1')" }
        ]})).await;

        let mut ws = ws_connect(&base, Some("fafo"), None).await.unwrap();
        // Binary frames are ignored; the connection shrugs and continues.
        ws.send(Message::Binary(vec![1, 2, 3].into())).await.unwrap();
        let v = ws_roundtrip(&mut ws, json!({
            "id": 1,
            "will": { "ops": [{ "object": "room", "sql": "DELETE FROM presence WHERE session = ?1", "params": ["s-1"] }] }
        })).await;
        assert_eq!(v["result"]["will"], "armed");

        // Die without saying goodbye. The will must run anyway.
        drop(ws);
        let mut deleted = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let (_, v) = post(&format!("{base}/objects/room/query"), None,
                exec_body("SELECT COUNT(*) AS c FROM presence")).await;
            if v["rows"][0]["c"] == 0 {
                deleted = true;
                break;
            }
        }
        assert!(deleted, "the armed will must delete the presence row");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn api_errors_wrap_foreign_errors_as_500s() {
        let e = ApiError::from(anyhow::anyhow!("boom"));
        assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(e.message, "boom");
        let bad_json: Result<Value, _> = serde_json::from_str("{");
        let e = ApiError::from(bad_json.unwrap_err());
        assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn internal_rpc_take_and_adopt_answer_for_unowned_workers() {
        let (_dir, node, base) = serve(None).await;
        let client = reqwest::Client::new();
        let rpc = |body: Value| {
            let client = client.clone();
            let url = format!("{base}/internal/rpc");
            async move {
                let resp = client
                    .post(url)
                    .header(crate::rpc::SECRET_HEADER, "cluster-secret")
                    .json(&body)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(resp.status().as_u16(), 200);
                resp.json::<Value>().await.unwrap()
            }
        };

        // Owned worker, unknown object: the take still resolves (transfer
        // of an object that has never been written is just metadata).
        let owned = crate::cluster::id_on_worker(0, 4, "take");
        let v = rpc(json!({ "Take": { "worker": 0, "object": owned, "taker": 1 } })).await;
        assert!(v["Take"]["Ok"].is_object(), "got: {v}");

        // A worker this node holds no lease for: NotMine, with a hint.
        let v = rpc(json!({ "Take": { "worker": 99, "object": "whatever", "taker": 1 } })).await;
        assert!(v["Take"]["Err"]["NotMine"].is_object(), "got: {v}");

        // Adopt: accepted for an owned worker, refused for a foreign one.
        let meta = json!({ "settled": false, "home": 0, "visit": null });
        let v = rpc(json!({ "Adopt": { "worker": 0, "object": owned, "meta": meta } })).await;
        assert_eq!(v.as_str(), Some("Ok"), "got: {v}");
        let meta = json!({ "settled": false, "home": 99, "visit": null });
        let v = rpc(json!({ "Adopt": { "worker": 99, "object": "whatever", "meta": meta } })).await;
        assert!(v["Err"].is_string(), "got: {v}");
        node.shutdown().await;
    }

    #[tokio::test]
    async fn resolve_auth_prefers_root_and_verifies_capability_signatures() {
        let (_dir, node, _base) = serve(Some("root-token")).await;
        assert!(matches!(resolve_auth(&node, "root-token", "root-token"), Some(Auth::Root)));
        assert!(resolve_auth(&node, "root-token", "junk").is_none());

        let cap = Capability {
            grants: vec![Grant { objects: "x".into(), verbs: vec!["read".into()] }],
            exp: u64::MAX,
            sub: None,
        };
        let good = grants::mint(&node.secret, &cap);
        assert!(matches!(resolve_auth(&node, "root-token", &good), Some(Auth::Cap(_))));
        let forged = grants::mint("attacker-secret", &cap);
        assert!(resolve_auth(&node, "root-token", &forged).is_none());
        node.shutdown().await;
    }
}
