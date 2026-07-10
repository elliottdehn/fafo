//! Persistence. The blob store is the source of truth for all object state;
//! local SQLite files are disposable working copies.
//!
//! Swapping disk for R2: implement `BlobStore` over the S3 API (R2 is
//! S3-compatible) — `put` is PUT, `get` is GET, `delete` is DELETE, `list` is
//! ListObjectsV2. Nothing else in the system changes. The transaction commit
//! record (`txns/<id>.json`) is a single put, which is the atomic commit
//! point; on R2, guard it with a conditional write (If-None-Match: *) if more
//! than one coordinator could ever run.

use async_trait::async_trait;
use std::path::{Path, PathBuf};

#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>>;
    async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()>;
    async fn delete(&self, key: &str) -> anyhow::Result<()>;
    /// All keys starting with `prefix`.
    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>>;
    /// Create-if-absent: returns true if this call created the blob, false
    /// if the key already existed. This is the system's only consensus
    /// primitive — lease claims race through it. On R2/S3 it maps to a PUT
    /// with `If-None-Match: *`.
    async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool>;

    /// Read `len` bytes at `offset`. Used to peek a snapshot's SQLite
    /// change counter (4 bytes) without downloading the file — the cheap
    /// staleness check behind delta-aware activation. Default falls back to
    /// a full get; R2 overrides with a Range request.
    async fn get_range(&self, key: &str, offset: u64, len: u64) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.get(key).await?.map(|bytes| {
            let start = (offset as usize).min(bytes.len());
            let end = ((offset + len) as usize).min(bytes.len());
            bytes[start..end].to_vec()
        }))
    }
}

pub struct FsBlobStore {
    root: PathBuf,
}

impl FsBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path_for(&self, key: &str) -> anyhow::Result<PathBuf> {
        if key
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        {
            anyhow::bail!("invalid blob key: {key:?}");
        }
        Ok(self.root.join(key))
    }
}

#[async_trait]
impl BlobStore for FsBlobStore {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        match std::fs::read(self.path_for(key)?) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let path = self.path_for(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Write-then-rename: a crash mid-put can never leave a torn blob,
        // matching the all-or-nothing behavior of an object-store PUT.
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match std::fs::remove_file(self.path_for(key)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        walk(&self.root, &self.root, &mut keys)?;
        keys.retain(|k| k.starts_with(prefix));
        keys.sort();
        Ok(keys)
    }

    async fn get_range(&self, key: &str, offset: u64, len: u64) -> anyhow::Result<Option<Vec<u8>>> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = match std::fs::File::open(self.path_for(key)?) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len as usize];
        let mut read = 0;
        while read < buf.len() {
            match file.read(&mut buf[read..])? {
                0 => break,
                n => read += n,
            }
        }
        buf.truncate(read);
        Ok(Some(buf))
    }

    async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
        let path = self.path_for(key)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Write content to a temp file, then hard-link it into place:
        // link(2) fails if the target exists, giving atomic create-if-absent
        // with complete content — the FS analog of If-None-Match: *.
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, bytes)?;
        let result = std::fs::hard_link(&tmp, &path);
        let _ = std::fs::remove_file(&tmp);
        match result {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

fn walk(dir: &Path, root: &Path, out: &mut Vec<String>) -> anyhow::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let path = entry?.path();
        if path.is_dir() {
            walk(&path, root, out)?;
        } else {
            out.push(path.strip_prefix(root)?.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}
