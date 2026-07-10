use fafo::cluster::{self, ClaimSpec, NodeConfig};
use fafo::store::BlobStore;
use fafo::{r2, store};
use std::sync::Arc;

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn blob_store_from_env(data_dir: &str) -> anyhow::Result<Arc<dyn BlobStore>> {
    match env_or("BLOB_STORE", "fs").as_str() {
        "fs" => Ok(Arc::new(store::FsBlobStore::new(format!(
            "{data_dir}/blobs"
        ))?)),
        "r2" => {
            let endpoint = match std::env::var("R2_ENDPOINT") {
                Ok(e) => e,
                Err(_) => {
                    let account = std::env::var("R2_ACCOUNT_ID")
                        .map_err(|_| anyhow::anyhow!("set R2_ENDPOINT or R2_ACCOUNT_ID"))?;
                    format!("https://{account}.r2.cloudflarestorage.com")
                }
            };
            Ok(Arc::new(r2::R2BlobStore::new(
                endpoint,
                std::env::var("R2_BUCKET").map_err(|_| anyhow::anyhow!("set R2_BUCKET"))?,
                std::env::var("R2_ACCESS_KEY_ID")
                    .map_err(|_| anyhow::anyhow!("set R2_ACCESS_KEY_ID"))?,
                std::env::var("R2_SECRET_ACCESS_KEY")
                    .map_err(|_| anyhow::anyhow!("set R2_SECRET_ACCESS_KEY"))?,
            )?))
        }
        other => anyhow::bail!("BLOB_STORE must be fs or r2, got {other:?}"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let data_dir = env_or("DATA_DIR", "./data");
    let host = env_or("HOST", "127.0.0.1"); // container images set HOST=0.0.0.0
    let port = env_or("PORT", "8787");
    let logical: usize = env_or("LOGICAL_WORKERS", "64").parse()?;
    let hysteresis: u64 = env_or("HYST", "200").parse()?;
    let secret = match std::env::var("CLUSTER_SECRET") {
        Ok(s) => s,
        Err(_) => {
            eprintln!("WARNING: CLUSTER_SECRET not set; using a dev default. Set it in production.");
            "dev-secret".to_string()
        }
    };

    let node = cluster::start(NodeConfig {
        store: blob_store_from_env(&data_dir)?,
        live_dir: format!("{data_dir}/live/p{port}").into(),
        logical,
        claim: ClaimSpec::parse(&env_or("CLAIM", "all"), logical),
        bind: format!("{host}:{port}"),
        advertise: std::env::var("ADVERTISE").ok(),
        hysteresis,
        secret,
        api_token: std::env::var("API_TOKEN").ok(),
    })
    .await?;

    // Run until SIGTERM (Cloudflare Containers' stop signal; 15 min of grace
    // before SIGKILL) or ctrl-c, then release leases so the replacement
    // instance claims them without waiting for a failed health check.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = sigterm.recv() => println!("SIGTERM: releasing leases"),
        _ = tokio::signal::ctrl_c() => println!("interrupt: releasing leases"),
    }
    node.shutdown().await;
    Ok(())
}
