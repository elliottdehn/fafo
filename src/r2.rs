//! R2 blob store over the S3-compatible API. Hand-rolled SigV4 (verified
//! against the AWS test vectors below) over reqwest+rustls — the five
//! operations fafo needs don't justify an AWS SDK.
//!
//! Endpoint: https://<account>.r2.cloudflarestorage.com, region "auto".
//! `create` maps to PUT with `If-None-Match: *`; R2 answers 412 (or 409
//! under concurrency) when the key exists — the same create-if-absent CAS
//! the filesystem store provides with hard links.
//!
//! Key charset note: fafo keys are [A-Za-z0-9_/.-], which lets us extract
//! list results with plain tag scanning instead of an XML parser. Reusing
//! this store for arbitrary keys would need real escaping.

use crate::store::BlobStore;
use async_trait::async_trait;
use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime};

type HmacSha256 = Hmac<Sha256>;

const RETRIES: usize = 3;

pub struct R2BlobStore {
    http: reqwest::Client,
    /// e.g. https://<account>.r2.cloudflarestorage.com
    endpoint: String,
    host: String,
    bucket: String,
    access_key: String,
    secret_key: String,
    region: String,
}

impl R2BlobStore {
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> anyhow::Result<Self> {
        let endpoint = endpoint.into().trim_end_matches('/').to_string();
        let host = endpoint
            .strip_prefix("https://")
            .or_else(|| endpoint.strip_prefix("http://"))
            .ok_or_else(|| anyhow::anyhow!("R2 endpoint must include scheme"))?
            .to_string();
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            endpoint,
            host,
            bucket: bucket.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            region: "auto".into(),
        })
    }

    fn path_for(&self, key: &str) -> String {
        format!("/{}/{}", self.bucket, uri_encode(key, false))
    }

    async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &[(String, String)],
        body: Vec<u8>,
        extra_headers: &[(&str, &str)],
    ) -> anyhow::Result<reqwest::Response> {
        let payload_hash = hex(&Sha256::digest(&body));
        let (date, time) = utc_now_stamps();
        let extra: Vec<(String, String)> = extra_headers
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let (authorization, amz_date) = sign_request(
            &self.secret_key,
            &self.access_key,
            &self.region,
            method.as_str(),
            &self.host,
            path,
            query,
            &extra,
            &payload_hash,
            &date,
            &time,
        );
        let mut url = format!("{}{}", self.endpoint, path);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&canonical_query(query));
        }
        let mut req = self
            .http
            .request(method, url)
            .header("host", &self.host)
            .header("x-amz-date", amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header("authorization", authorization);
        for (k, v) in extra_headers {
            req = req.header(*k, *v);
        }
        Ok(req.body(body).send().await?)
    }

    /// Retry transient failures (5xx, transport). All fafo blob ops are
    /// idempotent except `create`, whose ambiguous-retry case (we won but a
    /// timeout ate the ack, so the retry sees "exists" and reports false)
    /// only under-claims a lease — a liveness wrinkle, never a safety one.
    async fn with_retries<F, Fut, T>(&self, mut op: F) -> anyhow::Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Option<T>>>,
    {
        let mut last = anyhow::anyhow!("no attempts");
        for attempt in 0..RETRIES {
            match op().await {
                Ok(Some(v)) => return Ok(v),
                Ok(None) => last = anyhow::anyhow!("transient (5xx)"),
                Err(e) => last = e,
            }
            tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
        }
        Err(last)
    }
}

#[async_trait]
impl BlobStore for R2BlobStore {
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let path = self.path_for(key);
        self.with_retries(|| {
            let path = path.clone();
            async move {
                let resp = self
                    .request(reqwest::Method::GET, &path, &[], Vec::new(), &[])
                    .await?;
                match resp.status().as_u16() {
                    200 => Ok(Some(Some(resp.bytes().await?.to_vec()))),
                    404 => Ok(Some(None)),
                    s if s >= 500 => Ok(None),
                    s => anyhow::bail!("R2 GET {key}: {s}: {}", resp.text().await?),
                }
            }
        })
        .await
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> anyhow::Result<()> {
        let path = self.path_for(key);
        self.with_retries(|| {
            let path = path.clone();
            let body = bytes.to_vec();
            async move {
                let resp = self
                    .request(reqwest::Method::PUT, &path, &[], body, &[])
                    .await?;
                match resp.status().as_u16() {
                    200 => Ok(Some(())),
                    s if s >= 500 => Ok(None),
                    s => anyhow::bail!("R2 PUT {key}: {s}: {}", resp.text().await?),
                }
            }
        })
        .await
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = self.path_for(key);
        self.with_retries(|| {
            let path = path.clone();
            async move {
                let resp = self
                    .request(reqwest::Method::DELETE, &path, &[], Vec::new(), &[])
                    .await?;
                match resp.status().as_u16() {
                    200 | 204 | 404 => Ok(Some(())),
                    s if s >= 500 => Ok(None),
                    s => anyhow::bail!("R2 DELETE {key}: {s}: {}", resp.text().await?),
                }
            }
        })
        .await
    }

    async fn list(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = vec![
                ("list-type".to_string(), "2".to_string()),
                ("prefix".to_string(), prefix.to_string()),
            ];
            if let Some(t) = &token {
                query.push(("continuation-token".to_string(), t.clone()));
            }
            let path = format!("/{}", self.bucket);
            let xml = self
                .with_retries(|| {
                    let path = path.clone();
                    let query = query.clone();
                    async move {
                        let resp = self
                            .request(reqwest::Method::GET, &path, &query, Vec::new(), &[])
                            .await?;
                        match resp.status().as_u16() {
                            200 => Ok(Some(resp.text().await?)),
                            s if s >= 500 => Ok(None),
                            s => anyhow::bail!("R2 LIST {prefix}: {s}: {}", resp.text().await?),
                        }
                    }
                })
                .await?;
            keys.extend(extract_tags(&xml, "Key"));
            if extract_tags(&xml, "IsTruncated").first().map(String::as_str) == Some("true") {
                token = extract_tags(&xml, "NextContinuationToken").into_iter().next();
                if token.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        keys.sort();
        Ok(keys)
    }

    async fn get_range(&self, key: &str, offset: u64, len: u64) -> anyhow::Result<Option<Vec<u8>>> {
        let path = self.path_for(key);
        let range = format!("bytes={offset}-{}", offset + len - 1);
        self.with_retries(|| {
            let path = path.clone();
            let range = range.clone();
            async move {
                let resp = self
                    .request(
                        reqwest::Method::GET,
                        &path,
                        &[],
                        Vec::new(),
                        &[("range", range.as_str())],
                    )
                    .await?;
                match resp.status().as_u16() {
                    200 | 206 => Ok(Some(Some(resp.bytes().await?.to_vec()))),
                    404 => Ok(Some(None)),
                    416 => Ok(Some(Some(Vec::new()))), // range beyond EOF
                    s if s >= 500 => Ok(None),
                    s => anyhow::bail!("R2 RANGE {key}: {s}: {}", resp.text().await?),
                }
            }
        })
        .await
    }

    async fn create(&self, key: &str, bytes: &[u8]) -> anyhow::Result<bool> {
        let path = self.path_for(key);
        self.with_retries(|| {
            let path = path.clone();
            let body = bytes.to_vec();
            async move {
                let resp = self
                    .request(
                        reqwest::Method::PUT,
                        &path,
                        &[],
                        body,
                        &[("if-none-match", "*")],
                    )
                    .await?;
                match resp.status().as_u16() {
                    200 => Ok(Some(true)),
                    // 412 = exists; 409 = concurrent conditional writers,
                    // and someone else won.
                    412 | 409 => Ok(Some(false)),
                    s if s >= 500 => Ok(None),
                    s => anyhow::bail!("R2 CREATE {key}: {s}: {}", resp.text().await?),
                }
            }
        })
        .await
    }
}

// ------------------------------------------------------------------- sigv4

/// Pure SigV4 signer, testable against the AWS test vectors.
#[allow(clippy::too_many_arguments)]
fn sign_request(
    secret_key: &str,
    access_key: &str,
    region: &str,
    method: &str,
    host: &str,
    path: &str,
    query: &[(String, String)],
    extra_headers: &[(String, String)],
    payload_hash: &str,
    date: &str, // YYYYMMDD
    time: &str, // HHMMSS
) -> (String, String) {
    let amz_date = format!("{date}T{time}Z");
    let mut headers: Vec<(String, String)> = vec![
        ("host".into(), host.to_string()),
        ("x-amz-content-sha256".into(), payload_hash.to_string()),
        ("x-amz-date".into(), amz_date.clone()),
    ];
    for (k, v) in extra_headers {
        headers.push((k.to_lowercase(), v.trim().to_string()));
    }
    headers.sort();
    let canonical_headers: String = headers
        .iter()
        .map(|(k, v)| format!("{k}:{v}\n"))
        .collect();
    let signed_headers: String = headers
        .iter()
        .map(|(k, _)| k.as_str())
        .collect::<Vec<_>>()
        .join(";");

    let canonical_request = format!(
        "{method}\n{path}\n{}\n{canonical_headers}\n{signed_headers}\n{payload_hash}",
        canonical_query(query)
    );
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );

    let mut key = hmac(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    for part in [region, "s3", "aws4_request"] {
        key = hmac(&key, part.as_bytes());
    }
    let signature = hex(&hmac(&key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );
    (authorization, amz_date)
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<String> = query
        .iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, true), uri_encode(v, true)))
        .collect();
    pairs.sort();
    pairs.join("&")
}

/// RFC 3986 encoding as SigV4 requires: unreserved chars pass, '/' passes in
/// paths but not in query values.
fn uri_encode(s: &str, encode_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            b'/' if !encode_slash => out.push('/'),
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn utc_now_stamps() -> (String, String) {
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("clock after 1970")
        .as_secs();
    let days = (secs / 86_400) as i64;
    let (y, m, d) = civil_from_days(days);
    let rem = secs % 86_400;
    (
        format!("{y:04}{m:02}{d:02}"),
        format!("{:02}{:02}{:02}", rem / 3600, (rem % 3600) / 60, rem % 60),
    )
}

/// Days-since-epoch to (year, month, day). Howard Hinnant's civil_from_days.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (y + i64::from(m <= 2), m, d)
}

fn extract_tags(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let after = &rest[start + open.len()..];
        let Some(end) = after.find(&close) else { break };
        out.push(after[..end].to_string());
        rest = &after[end + close.len()..];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical AWS SigV4 example: GET test.txt from examplebucket,
    /// 2013-05-24, with a signed Range header.
    /// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    #[test]
    fn sigv4_matches_aws_test_vector() {
        let empty_hash = hex(&Sha256::digest(b""));
        let (authorization, amz_date) = sign_request(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "AKIAIOSFODNN7EXAMPLE",
            "us-east-1",
            "GET",
            "examplebucket.s3.amazonaws.com",
            "/test.txt",
            &[],
            &[("Range".into(), "bytes=0-9".into())],
            &empty_hash,
            "20130524",
            "000000",
        );
        assert_eq!(amz_date, "20130524T000000Z");
        assert!(
            authorization.ends_with(
                "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
            ),
            "got: {authorization}"
        );
    }

    #[test]
    fn civil_dates_are_correct() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1)); // leap year
        assert_eq!(civil_from_days(19_782), (2024, 2, 29));
        assert_eq!(civil_from_days(20_643), (2026, 7, 9));
    }

    #[test]
    fn list_xml_extraction() {
        let xml = r#"<ListBucketResult><IsTruncated>true</IsTruncated>
            <Contents><Key>objects/a.db</Key></Contents>
            <Contents><Key>objects/b.db</Key></Contents>
            <NextContinuationToken>tok123</NextContinuationToken></ListBucketResult>"#;
        assert_eq!(extract_tags(xml, "Key"), vec!["objects/a.db", "objects/b.db"]);
        assert_eq!(extract_tags(xml, "NextContinuationToken"), vec!["tok123"]);
        assert_eq!(extract_tags(xml, "IsTruncated"), vec!["true"]);
    }
}
