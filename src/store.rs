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
        // Walk only the directory the prefix implies, not the whole root —
        // with a million lease files, a full-root walk per list turns every
        // lookup into O(everything).
        let dir = prefix.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
        let base = if dir.is_empty() {
            self.root.clone()
        } else {
            self.root.join(dir)
        };
        let mut keys = Vec::new();
        walk(&base, &self.root, &mut keys)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn store() -> (tempfile::TempDir, FsBlobStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path().join("blobs")).unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let (_dir, s) = store();
        assert_eq!(s.get("a/b.json").await.unwrap(), None);
        s.put("a/b.json", b"one").await.unwrap();
        assert_eq!(s.get("a/b.json").await.unwrap().unwrap(), b"one");
        s.put("a/b.json", b"two").await.unwrap();
        assert_eq!(s.get("a/b.json").await.unwrap().unwrap(), b"two", "put overwrites");
        s.delete("a/b.json").await.unwrap();
        assert_eq!(s.get("a/b.json").await.unwrap(), None);
        s.delete("a/b.json").await.unwrap(); // deleting a missing key is fine
    }

    #[tokio::test]
    async fn create_is_first_writer_wins() {
        let (_dir, s) = store();
        assert!(s.create("lease.json", b"alpha").await.unwrap());
        assert!(!s.create("lease.json", b"beta").await.unwrap());
        assert_eq!(
            s.get("lease.json").await.unwrap().unwrap(),
            b"alpha",
            "the loser must not clobber the winner's content"
        );
    }

    #[tokio::test]
    async fn concurrent_creates_elect_exactly_one_winner() {
        let (_dir, s) = store();
        let s = Arc::new(s);
        let mut tasks = Vec::new();
        for i in 0..16 {
            let s = s.clone();
            tasks.push(tokio::spawn(async move {
                s.create("race.json", format!("claimer-{i}").as_bytes())
                    .await
                    .unwrap()
            }));
        }
        let mut winners = 0;
        for t in tasks {
            if t.await.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "create-if-absent is the consensus primitive");
    }

    #[tokio::test]
    async fn list_walks_only_the_prefix() {
        let (_dir, s) = store();
        s.put("objects/a.db", b"x").await.unwrap();
        s.put("objects/a.d.0000000003", b"x").await.unwrap();
        s.put("objects/b.db", b"x").await.unwrap();
        s.put("_lease/b0/e1.json", b"x").await.unwrap();
        assert_eq!(
            s.list("objects/a.d.").await.unwrap(),
            vec!["objects/a.d.0000000003"]
        );
        assert_eq!(s.list("objects/").await.unwrap().len(), 3);
        assert_eq!(s.list("_lease/").await.unwrap(), vec!["_lease/b0/e1.json"]);
        assert!(s.list("nothing/here/").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn traversal_and_malformed_keys_are_rejected() {
        let (_dir, s) = store();
        for key in ["../escape", "a/../b", "a//b", "./a", "a/./b"] {
            assert!(s.put(key, b"x").await.is_err(), "{key:?} must be rejected");
            assert!(s.get(key).await.is_err(), "{key:?} must be rejected");
        }
    }

    #[tokio::test]
    async fn get_range_clamps_to_the_file() {
        let (_dir, s) = store();
        s.put("f", b"0123456789").await.unwrap();
        assert_eq!(s.get_range("f", 2, 4).await.unwrap().unwrap(), b"2345");
        assert_eq!(s.get_range("f", 8, 100).await.unwrap().unwrap(), b"89");
        assert_eq!(s.get_range("f", 50, 4).await.unwrap().unwrap(), b"");
        assert_eq!(s.get_range("missing", 0, 4).await.unwrap(), None);
    }

    #[tokio::test]
    async fn trait_default_get_range_matches_the_override() {
        // Callers must not care which impl serves the peek.
        struct Defaulted(FsBlobStore);
        #[async_trait]
        impl BlobStore for Defaulted {
            async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
                self.0.get(key).await
            }
            async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
                self.0.put(key, bytes).await
            }
            async fn delete(&self, key: &str) -> anyhow::Result<()> {
                self.0.delete(key).await
            }
            async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
                self.0.list(prefix).await
            }
            async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
                self.0.create(key, bytes).await
            }
        }
        let (_dir, s) = store();
        s.put("f", b"0123456789").await.unwrap();
        let d = Defaulted(FsBlobStore::new(s.root.clone()).unwrap());
        for (offset, len) in [(0, 4), (2, 4), (8, 100), (50, 4)] {
            assert_eq!(
                d.get_range("f", offset, len).await.unwrap(),
                s.get_range("f", offset, len).await.unwrap(),
                "offset {offset} len {len}"
            );
        }
        assert_eq!(d.get_range("missing", 0, 4).await.unwrap(), None);
        // The pass-throughs exist only to satisfy the trait; touch them.
        d.put("g", b"1").await.unwrap();
        assert!(d.create("h", b"1").await.unwrap());
        assert_eq!(d.list("g").await.unwrap(), vec!["g"]);
        d.delete("g").await.unwrap();
    }

    #[tokio::test]
    async fn create_surfaces_real_io_errors() {
        let (_dir, s) = store();
        s.put("f", b"x").await.unwrap();
        // The parent of the new key is an existing FILE: not a race we can
        // resolve, so it must be an error, not a quiet false.
        assert!(s.create("f/child", b"x").await.is_err());
        assert!(s.put("f/child", b"x").await.is_err());
    }
}
