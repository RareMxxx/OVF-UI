//! Ephemeral, loopback-only TLS adapter. OVF Tool remains the transfer engine.
//! Cloud credentials never appear in its arguments or config files.
use crate::api::CloudSession;
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use http_body_util::{BodyExt, Full, StreamBody, combinators::UnsyncBoxBody};
use hyper::{
    Request, Response,
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::rt::TokioIo;
use sha1::{Digest, Sha1};
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Mutex, time::Instant};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{self, pki_types::PrivatePkcs8KeyDer},
};
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

type Body = UnsyncBoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

#[cfg(test)]
#[path = "session_tests.rs"]
mod session_tests;

#[derive(Default)]
pub struct Traffic {
    pub received: AtomicU64,
    pub sent: AtomicU64,
    pub limit: AtomicU64,
    pub diagnostics: std::sync::Mutex<Vec<String>>,
    pub cloud: std::sync::Mutex<crate::transfer_state::CloudTrace>,
}

impl Traffic {
    fn record(&self, message: String) {
        if let Ok(mut lines) = self.diagnostics.lock() {
            if lines.len() >= 100 {
                lines.remove(0);
            }
            lines.push(crate::store::redact(&message));
        }
    }
    pub fn diagnostics(&self) -> Vec<String> {
        self.diagnostics
            .lock()
            .map(|v| v.clone())
            .unwrap_or_default()
    }
}

pub struct Limiter {
    next: Mutex<Instant>,
    pub traffic: Arc<Traffic>,
}

impl Limiter {
    pub fn new(traffic: Arc<Traffic>) -> Self {
        Self {
            next: Mutex::new(Instant::now()),
            traffic,
        }
    }
    pub async fn consume(&self, n: usize) {
        let rate = self.traffic.limit.load(Ordering::Relaxed);
        if rate == 0 {
            return;
        }
        let deadline = {
            let mut next = self.next.lock().await;
            let now = Instant::now();
            if *next < now {
                *next = now;
            }
            *next += Duration::from_secs_f64(n as f64 / rate as f64);
            *next
        };
        tokio::time::sleep_until(deadline).await;
    }
}

struct State {
    session: CloudSession,
    origin: String,
    capability: String,
    mappings: Mutex<HashMap<String, Url>>,
    limiter: Limiter,
    cancel: CancellationToken,
}

pub struct Bridge {
    pub port: u16,
    pub ticket: String,
    pub thumbprint: String,
    pub traffic: Arc<Traffic>,
    cancel: CancellationToken,
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl Bridge {
    pub async fn start(
        session: CloudSession,
        traffic: Arc<Traffic>,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let cert =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()])?;
        let thumbprint = Sha1::digest(cert.cert.der())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
            )?;
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let ticket = Uuid::new_v4().simple().to_string();
        let state = Arc::new(State {
            session,
            origin: format!("https://127.0.0.1:{port}"),
            capability: ticket.clone(),
            mappings: Mutex::new(HashMap::new()),
            limiter: Limiter::new(traffic.clone()),
            cancel: cancel.child_token(),
        });
        let stop = state.cancel.clone();
        // A single disk stream can last longer than the access token. Renew independently
        // of HTTP activity; the session gate also coordinates other bridges and API polls.
        let renewal = state.clone();
        tokio::spawn(async move {
            while let Some(delay) = renewal.session.renewal_delay().await {
                tokio::select! {
                    _ = renewal.cancel.cancelled() => break,
                    _ = tokio::time::sleep(delay) => {}
                }
                tokio::select! {
                    _ = renewal.cancel.cancelled() => break,
                    result = renewal.session.renew_in_background() => {
                        if let Err(error) = result {
                            renewal.limiter.traffic.record(format!("Продление сессии: {error:#}"));
                        }
                    }
                }
            }
        });
        tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _=state.cancel.cancelled()=>break,
                    Some(_)=connections.join_next(), if !connections.is_empty()=>{},
                    accepted=listener.accept()=>{
                        let Ok((socket,address))=accepted else {break};
                        if !address.ip().is_loopback(){continue}
                        let _ = socket.set_nodelay(true);
                        let acceptor=acceptor.clone();let state=state.clone();
                        connections.spawn(async move {
                            let Ok(Ok(tls))=tokio::time::timeout(Duration::from_secs(10),acceptor.accept(socket)).await else{return};
                            let cancel=state.cancel.clone();
                            let service=service_fn(move |request| {let state=state.clone();async move {
                                let traffic=state.limiter.traffic.clone();
                                let response=match relay(state,request).await {Ok(r)=>r,Err(e)=>{traffic.record(format!("Ошибка адаптера: {e:#}"));plain(502,"OVF UI: upstream request failed")}};
                                Ok::<_,Infallible>(response)
                            }});
                            let connection=hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(tls),service);
                            tokio::select! {_=cancel.cancelled()=>{},_=connection=>{}}
                        });
                    }
                }
            }
            connections.abort_all();
        });
        Ok(Self {
            port,
            ticket,
            thumbprint,
            traffic,
            cancel: stop,
        })
    }
}

fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}
fn plain(status: u16, message: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(full(message.to_owned()))
        .unwrap()
}

fn forbidden_power_path(path: &str) -> bool {
    let path = percent_encoding::percent_decode_str(path)
        .decode_utf8_lossy()
        .to_ascii_lowercase();
    path.contains("/power/")
        || path.contains("/action/undeploy")
        || path.contains("/action/recompose")
        || path.contains("/action/remove")
}

async fn relay(state: Arc<State>, request: Request<Incoming>) -> Result<Response<Body>> {
    let path = request.uri().path();
    // OVF Tool discovers API versions before presenting its session ticket.
    // Keep this public operation outside every credential/capability path.
    if path == "/api/versions" && request.method() == hyper::Method::GET {
        return public_versions(&state).await;
    }
    let cap_prefix = format!("/_cap/{}/", state.capability);
    let mapped = if let Some(key) = path.strip_prefix("/_remote/") {
        state.mappings.lock().await.contains_key(key)
    } else {
        false
    };
    let cookie = request
        .headers()
        .get("cookie")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| {
            s.split(';')
                .any(|s| s.trim() == format!("vcloud_session_id={}", state.capability))
        });
    let authenticated = request
        .headers()
        .get("x-vcloud-authorization")
        .is_some_and(|h| h.as_bytes() == state.capability.as_bytes())
        || path.starts_with(&cap_prefix)
        || mapped
        || cookie;
    if !authenticated {
        state.limiter.traffic.record(format!(
            "Локальный вход отклонён: {} {}; заголовки: {}",
            request.method(),
            if path.starts_with("/api/") {
                path
            } else {
                "[path]"
            },
            request
                .headers()
                .keys()
                .map(|k| k.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        return Ok(Response::builder()
            .status(401)
            .header("WWW-Authenticate", "Basic realm=\"OVF Dock\"")
            .body(full("Local transfer session required"))?);
    }
    let mut remote = if let Some(key) = path.strip_prefix("/_remote/") {
        state
            .mappings
            .lock()
            .await
            .get(key)
            .cloned()
            .context("Unknown transfer URL")?
    } else {
        let path = path
            .strip_prefix(&cap_prefix)
            .map(|p| format!("/{p}"))
            .unwrap_or_else(|| path.to_owned());
        let mut remote = state.session.base.join(&path)?;
        remote.set_query(request.uri().query());
        remote
    };
    // Never let OVF Tool turn off a whole vApp or remove objects as a side effect.
    if forbidden_power_path(remote.path()) {
        return Ok(plain(
            403,
            "Power changes require explicit confirmation in OVF Dock",
        ));
    }
    let same_origin = remote.origin() == state.session.base.origin();
    if same_origin {
        state.session.ensure_fresh().await?;
    }
    // An opaque mapping itself is a capability, but normal API calls require the ticket.
    if !same_origin
        && request.method() != hyper::Method::GET
        && request.method() != hyper::Method::PUT
        && request.method() != hyper::Method::HEAD
    {
        return Ok(plain(405, "Unsupported transfer method"));
    }
    let is_login = remote.path() == "/api/sessions" && request.method() == hyper::Method::POST;
    if remote.path() == "/api/session" && request.method() == hyper::Method::DELETE {
        return Ok(plain(204, ""));
    }
    let method = if is_login {
        remote.set_path("/api/session");
        hyper::Method::GET
    } else {
        request.method().clone()
    };
    let (parts, body) = request.into_parts();
    let content_type = parts
        .headers
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let mut upstream = state.session.client.request(method.clone(), remote.clone());
    if matches!(method, hyper::Method::GET | hyper::Method::PUT)
        && remote.path().contains("/transfer/")
    {
        state
            .limiter
            .traffic
            .cloud
            .lock()
            .unwrap()
            .file(method.as_str(), remote.path());
    }
    let versioned_api =
        same_origin && remote.path().starts_with("/api/") && remote.path() != "/api/versions";
    for (name, value) in &parts.headers {
        if versioned_api && name == "accept" {
            continue;
        }
        if !matches!(
            name.as_str(),
            "host"
                | "authorization"
                | "x-vcloud-authorization"
                | "x-vmware-vcloud-access-token"
                | "cookie"
                | "connection"
                | "transfer-encoding"
                | "accept-encoding"
                | "content-length"
        ) {
            upstream = upstream.header(name, value);
        }
    }
    if same_origin {
        // OVF Tool 9.1 sends application/*+xml without a version. VCD 10.6
        // rejects it with 406, even though the session ticket is valid.
        if versioned_api {
            upstream = upstream.header(
                "accept",
                api_accept(
                    parts.headers.get("accept").and_then(|v| v.to_str().ok()),
                    &state.session.version,
                ),
            );
        }
    }
    // Clone only safe reads, before adding credentials: retain Range and conditionals on
    // a 401 retry, but never clone/buffer/replay a streaming upload or a mutation.
    let safe_retry = if same_origin && matches!(method, hyper::Method::GET | hyper::Method::HEAD) {
        upstream.try_clone()
    } else {
        None
    };
    let mut generation = 0;
    if same_origin {
        (upstream, generation) = state.session.authorize_tracked(upstream).await;
    }
    if method == hyper::Method::PUT || method == hyper::Method::POST {
        if content_type.contains("xml") || content_type.contains("json") {
            let data = http_body_util::Limited::new(body, 16 * 1024 * 1024)
                .collect()
                .await
                .map_err(|e| anyhow::anyhow!("Metadata request failed: {e}"))?
                .to_bytes();
            let text = String::from_utf8(data.to_vec())?;
            let text = text.replace(
                &format!("{}{cap_prefix}", state.origin),
                state.session.base.as_str(),
            );
            upstream = upstream.body(text);
        } else {
            if let Some(length) = parts.headers.get("content-length") {
                upstream = upstream.header("content-length", length);
            }
            let s = state.clone();
            let stream = body.into_data_stream().and_then(move |chunk| {
                let s = s.clone();
                async move {
                    s.limiter.consume(chunk.len()).await;
                    s.limiter
                        .traffic
                        .sent
                        .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    Ok(chunk)
                }
            });
            upstream = upstream.body(reqwest::Body::wrap_stream(stream));
        }
    }
    let mut response = upstream
        .send()
        .await
        .context("Cloud transfer request failed")?;
    // Retry only idempotent reads after refreshing a token, never replay streamed writes.
    if response.status().as_u16() == 401 && same_origin {
        if let Some(retry) = safe_retry {
            state.session.refresh_after_unauthorized(generation).await?;
            let (retry, retry_generation) = state.session.authorize_tracked(retry).await;
            generation = retry_generation;
            response = retry.send().await?;
        }
        if response.status().as_u16() == 401 {
            state.session.mark_unauthorized(generation).await;
            state.limiter.traffic.record(format!("{} Запрос {method} не повторяется автоматически; проверьте состояние объекта перед новой передачей.", state.session.authentication_error()));
        }
    }
    let status = response.status();
    state.limiter.traffic.record(format!(
        "{} {} → HTTP {}; Accept: {}",
        method,
        if same_origin {
            remote.path()
        } else {
            "[transfer]"
        },
        status.as_u16(),
        parts
            .headers
            .get("accept")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("—")
    ));
    let headers = response.headers().clone();
    let is_text = headers
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .is_some_and(|s| s.contains("xml") || s.contains("json"));
    let mut result = Response::builder().status(status);
    for (name, value) in &headers {
        if matches!(
            name.as_str(),
            "content-length"
                | "transfer-encoding"
                | "connection"
                | "set-cookie"
                | "x-vcloud-authorization"
                | "x-vmware-vcloud-access-token"
                | "x-vmware-vcloud-token-type"
        ) {
            continue;
        }
        if name == "location" {
            result = result.header(name, rewrite_urls(&state, value.to_str()?).await?);
        } else {
            result = result.header(name, value);
        }
    }
    if same_origin {
        result = result.header("x-vcloud-authorization", &state.capability);
    }
    if is_text {
        let mut data = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                data.len() + chunk.len() <= 16 * 1024 * 1024,
                "Oversized metadata response"
            );
            data.extend_from_slice(&chunk);
        }
        let text = String::from_utf8(data)?;
        if same_origin
            && status.is_success()
            && remote.path().starts_with("/api/")
            && text.trim_start().starts_with('<')
        {
            state.limiter.traffic.cloud.lock().unwrap().observe(
                &text,
                matches!(method, hyper::Method::POST | hyper::Method::PUT),
            )?;
        }
        if !status.is_success() {
            state.limiter.traffic.record(format!(
                "HTTP {}: {}",
                status.as_u16(),
                crate::api::api_error(&text)
            ));
        }
        let rewritten = rewrite_urls(&state, &text).await?;
        Ok(result.body(full(rewritten))?)
    } else {
        if let Some(length) = headers.get("content-length") {
            result = result.header("content-length", length);
        }
        let s = state.clone();
        let stream = response.bytes_stream().then(move |chunk| {
            let s = s.clone();
            async move {
                let chunk =
                    chunk.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
                s.limiter.consume(chunk.len()).await;
                s.limiter
                    .traffic
                    .received
                    .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(Frame::data(chunk))
            }
        });
        Ok(result.body(StreamBody::new(stream).boxed_unsync())?)
    }
}

async fn public_versions(state: &State) -> Result<Response<Body>> {
    // Fixed path, no caller headers/query/body, no CloudSession authorization,
    // refresh or authenticated retry. The client itself has no default credentials
    // and disables redirects (the same client used for anonymous initial discovery).
    let response = state
        .session
        .client
        .get(state.session.base.join("api/versions")?)
        .header("accept", "application/*+xml")
        .timeout(Duration::from_secs(30))
        .send()
        .await?;
    if !response.status().is_success() {
        // In particular, never relay a Location, cookie, token header or error
        // body through the authenticated response rewriter.
        return Ok(plain(502, "Public API version discovery failed"));
    }
    let text = crate::api::limited_text(response).await?;
    roxmltree::Document::parse(&text)?;
    let rewritten = https_urls().replace_all(&text, |captures: &regex::Captures<'_>| {
        let original = &captures[0];
        let raw = original.replace("&amp;", "&");
        match Url::parse(&raw) {
            Ok(url) if url.origin() == state.session.base.origin() => {
                let mut tail = url[url::Position::BeforePath..].to_owned();
                // The response is XML, so query separators must stay escaped.
                tail = tail.replace('&', "&amp;");
                format!("{}{tail}", state.origin)
            }
            _ => original.to_owned(),
        }
    });
    Ok(Response::builder()
        .header("content-type", "application/xml; charset=utf-8")
        .header("cache-control", "no-store")
        .body(full(rewritten.into_owned()))?)
}

fn api_accept(accept: Option<&str>, version: &str) -> String {
    let accept = accept.unwrap_or("application/*+xml");
    if accept.contains("version=") || (!accept.contains("xml") && accept != "*/*") {
        return accept.to_owned();
    }
    format!("application/*+xml;version={version}")
}

fn https_urls() -> &'static regex::Regex {
    static URL_RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    URL_RE.get_or_init(|| regex::Regex::new(r#"https://[^\s\"'<>]+"#).unwrap())
}

async fn rewrite_urls(state: &State, text: &str) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for m in https_urls().find_iter(text) {
        out.push_str(&text[last..m.start()]);
        last = m.end();
        let raw = m.as_str().replace("&amp;", "&");
        let Ok(url) = Url::parse(&raw) else {
            out.push_str(m.as_str());
            continue;
        };
        if url.origin() == state.session.base.origin() {
            let mut tail = url[url::Position::BeforePath..url::Position::AfterQuery].to_owned();
            if m.as_str().contains("&amp;") {
                tail = tail.replace('&', "&amp;");
            }
            out.push_str(&format!(
                "{}/_cap/{}{}",
                state.origin, state.capability, tail
            ));
        } else if url.path().contains("/transfer/") || url.query().is_some() {
            // External transfer hosts are trusted only when returned by the authenticated server.
            // No cloud Authorization header is sent to them, and their original TLS is verified.
            ensure!(
                url.username().is_empty() && url.password().is_none(),
                "Credentials in transfer URL"
            );
            let key = Uuid::new_v4().simple().to_string();
            state.mappings.lock().await.insert(key.clone(), url);
            out.push_str(&format!("{}/_remote/{key}", state.origin));
        } else {
            out.push_str(m.as_str());
        }
    }
    out.push_str(&text[last..]);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn power_actions_are_not_relayed() {
        assert!(forbidden_power_path("/api/vApp/1/power/action/powerOff"));
        assert!(forbidden_power_path("/api/vApp/1/action/undeploy"));
        assert!(!forbidden_power_path("/api/vApp/1/action/enableDownload"));
    }
    #[test]
    fn adds_api_version_to_real_ovftool_911_accept_header() {
        assert_eq!(
            api_accept(Some("application/*+xml"), "39.0"),
            "application/*+xml;version=39.0"
        );
        assert_eq!(
            api_accept(Some("application/*+xml;version=38.1"), "39.0"),
            "application/*+xml;version=38.1"
        );
        assert_eq!(
            api_accept(Some("application/octet-stream"), "39.0"),
            "application/octet-stream"
        );
    }
    #[tokio::test]
    async fn limiter_accounts_for_bytes() {
        let t = Arc::new(Traffic::default());
        t.limit.store(100_000, Ordering::Relaxed);
        let l = Limiter::new(t);
        let start = Instant::now();
        l.consume(10_000).await;
        assert!(start.elapsed() >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn token_bridge_preserves_tls_streams_and_blocks_unconfirmed_power() {
        bridge_contract(None).await;
    }

    #[tokio::test]
    #[ignore = "Set OVF_UI_TEST_TOOL to a locally installed OVF Tool executable; uses only synthetic loopback endpoints"]
    async fn real_ovftool_negotiates_public_versions_without_disclosing_ticket() {
        let tool = std::env::var_os("OVF_UI_TEST_TOOL").expect("OVF_UI_TEST_TOOL is required");
        bridge_contract(Some(std::path::PathBuf::from(tool))).await;
    }

    async fn bridge_contract(real_tool: Option<std::path::PathBuf>) {
        use crate::model::{AuthKind, Endpoint, Secret, Tenant};
        use std::sync::Mutex as StdMutex;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()])
            .unwrap();
        let root = reqwest::Certificate::from_der(cert.cert.der()).unwrap();
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
            )
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let received = Arc::new(StdMutex::new(Vec::<(String, String, usize)>::new()));
        let task_polls = Arc::new(AtomicU64::new(0));
        let versions_status = Arc::new(AtomicU64::new(200));
        let server_versions_status = versions_status.clone();
        let server_polls = task_polls.clone();
        let captured = received.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let acceptor = acceptor.clone();
                let captured = captured.clone();
                let task_polls = server_polls.clone();
                let versions_status = server_versions_status.clone();
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(stream).await else {
                        return;
                    };
                    let service = service_fn(move |req: Request<Incoming>| {
                        let captured = captured.clone();
                        let task_polls = task_polls.clone();
                        let versions_status = versions_status.clone();
                        async move {
                            let path = req.uri().path().to_owned();
                            if path == "/api/versions" {
                                assert!(req.uri().query().is_none());
                                for name in ["authorization", "x-vcloud-authorization", "cookie"] {
                                    assert!(
                                        req.headers().get(name).is_none(),
                                        "Discovery must not use session credentials"
                                    );
                                }
                                assert!(req.headers().get("x-discovery-probe").is_none());
                            }
                            if path == "/api/session" || path == "/api/vApp/vapp-1" {
                                assert_eq!(
                                    req.headers().get("accept").unwrap(),
                                    "application/*+xml;version=39.0",
                                    "Cloud Director 10.6 requires a version"
                                );
                            }
                            let auth = req
                                .headers()
                                .get("authorization")
                                .and_then(|h| h.to_str().ok())
                                .unwrap_or("")
                                .to_owned();
                            let data = req.into_body().collect().await.unwrap().to_bytes();
                            captured
                                .lock()
                                .unwrap()
                                .push((path.clone(), auth.clone(), data.len()));
                            let (kind, body) = match path.as_str() {
                                "/api/versions" => (
                                    "application/xml",
                                    format!(
                                        r#"<SupportedVersions xmlns="http://www.vmware.com/vcloud/versions"><VersionInfo deprecated="false"><Version>39.0</Version><LoginUrl>https://127.0.0.1:{port}/api/sessions</LoginUrl><MediaTypeMapping><SchemaLocation>https://localhost:{port}/transfer/schema?signature=public</SchemaLocation></MediaTypeMapping></VersionInfo></SupportedVersions>"#
                                    ),
                                ),
                                "/oauth/tenant/alpha/token" => {
                                    assert!(
                                        String::from_utf8_lossy(&data)
                                            .contains("refresh_token=test-refresh")
                                    );
                                    (
                                        "application/json",
                                        r#"{"access_token":"test-access","expires_in":86400}"#
                                            .into(),
                                    )
                                }
                                "/api/session" => {
                                    assert_eq!(auth, "Bearer test-access");
                                    (
                                        "application/xml",
                                        format!(
                                            "<Session href=\"https://127.0.0.1:{port}/api/session\" org=\"alpha\"/>"
                                        ),
                                    )
                                }
                                "/api/org" => {
                                    assert_eq!(auth, "Bearer test-access");
                                    (
                                        "application/xml",
                                        r#"<OrgList xmlns="http://www.vmware.com/vcloud/v1.5"/>"#
                                            .into(),
                                    )
                                }
                                "/api/vApp/vapp-1" => {
                                    assert_eq!(auth, "Bearer test-access");
                                    (
                                        "application/xml",
                                        format!(
                                            "<VApp><Link href=\"https://127.0.0.1:{port}/transfer/disk\"/><Link href=\"https://localhost:{port}/transfer/external?signature=secret\"/></VApp>"
                                        ),
                                    )
                                }
                                "/transfer/disk" => {
                                    ("application/octet-stream", "payload-from-cloud".into())
                                }
                                "/transfer/benchmark" => {
                                    ("application/octet-stream", "x".repeat(32 * 1024 * 1024))
                                }
                                "/transfer/benchmark-upload" => {
                                    assert_eq!(data.len(), 32 * 1024 * 1024);
                                    assert!(data.iter().all(|b| *b == b'x'));
                                    ("application/octet-stream", String::new())
                                }
                                "/transfer/external" => {
                                    assert!(
                                        auth.is_empty(),
                                        "Credentials must not reach external transfer hosts"
                                    );
                                    ("application/octet-stream", "external-payload".into())
                                }
                                "/transfer/upload" => {
                                    assert_eq!(data.as_ref(), b"upload-body");
                                    ("application/octet-stream", String::new())
                                }
                                "/api/create-test-template" => (
                                    "application/xml",
                                    format!(
                                        r#"<VAppTemplate href="https://127.0.0.1:{port}/api/vAppTemplate/upload" status="0"><Tasks><Task href="https://127.0.0.1:{port}/api/task/import" status="running"><Progress>5</Progress></Task></Tasks></VAppTemplate>"#
                                    ),
                                ),
                                "/api/task/import" => {
                                    let status = if task_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                                        "running"
                                    } else {
                                        "success"
                                    };
                                    (
                                        "application/xml",
                                        format!(
                                            r#"<Task href="https://127.0.0.1:{port}/api/task/import" status="{status}"><Progress>90</Progress></Task>"#
                                        ),
                                    )
                                }
                                "/api/vAppTemplate/upload" => (
                                    "application/xml",
                                    format!(
                                        r#"<VAppTemplate href="https://127.0.0.1:{port}/api/vAppTemplate/upload" status="8" ovfDescriptorUploaded="true"/>"#
                                    ),
                                ),
                                _ => panic!("Unexpected request: {path}"),
                            };
                            let mut response = Response::builder();
                            if path == "/api/versions" {
                                response = response
                                    .status(versions_status.load(Ordering::SeqCst) as u16)
                                    .header(
                                        "location",
                                        format!("https://127.0.0.1:{port}/api/session"),
                                    )
                                    .header("set-cookie", "upstream-cookie=never-forward")
                                    .header(
                                        "x-vcloud-authorization",
                                        "upstream-token-never-forward",
                                    );
                            }
                            Ok::<_, Infallible>(
                                response
                                    .header("content-type", kind)
                                    .body(Full::new(Bytes::from(body)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), service)
                        .await;
                });
            }
        });
        let client = reqwest::Client::builder()
            .add_root_certificate(root)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let tenant = Tenant {
            id: Uuid::new_v4(),
            organization: "alpha".into(),
            username: String::new(),
            auth: AuthKind::ApiToken,
        };
        let endpoint = Endpoint {
            id: Uuid::new_v4(),
            name: "test".into(),
            url: format!("https://127.0.0.1:{port}"),
            tenants: vec![tenant.clone()],
        };
        let session = CloudSession::login_with_client(
            &endpoint,
            &tenant,
            Secret {
                value: "test-refresh".into(),
                session_token: String::new(),
            },
            client,
        )
        .await
        .unwrap();
        let traffic = Arc::new(Traffic::default());
        let bridge = Bridge::start(session.clone(), traffic.clone(), CancellationToken::new())
            .await
            .unwrap();
        // Only this synthetic loopback client accepts the test bridge certificate without a pin.
        // Production OVF Tool receives the exact certificate fingerprint instead.
        let local = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let base = format!("https://127.0.0.1:{}", bridge.port);
        let requests_before_discovery = received.lock().unwrap().len();
        for status in [200, 302, 401, 500] {
            versions_status.store(status, Ordering::SeqCst);
            let discovery = local
                .get(format!("{base}/api/versions?ignored=1"))
                .header("x-discovery-probe", "must-not-forward")
                .header("authorization", "Basic untrusted")
                .header("cookie", "untrusted=value")
                .send()
                .await
                .unwrap();
            assert_eq!(discovery.status().is_success(), status == 200);
            for name in [
                "x-vcloud-authorization",
                "x-vmware-vcloud-access-token",
                "set-cookie",
                "location",
            ] {
                assert!(
                    discovery.headers().get(name).is_none(),
                    "Public response leaked {name}"
                );
            }
            let body = discovery.text().await.unwrap();
            for secret in [
                &bridge.ticket,
                "test-access",
                "test-refresh",
                "/_cap/",
                "/_remote/",
            ] {
                assert!(!body.contains(secret));
            }
            if status == 200 {
                let document = roxmltree::Document::parse(&body).unwrap();
                assert_eq!(crate::api::choose_version(&body).unwrap(), "39.0");
                let info = document
                    .descendants()
                    .find(|n| n.has_tag_name("VersionInfo"))
                    .unwrap();
                assert_eq!(info.attribute("deprecated"), Some("false"));
                let login_url = document
                    .descendants()
                    .find(|n| n.has_tag_name("LoginUrl"))
                    .unwrap()
                    .text()
                    .unwrap();
                assert_eq!(login_url, format!("{base}/api/sessions"));
                assert_eq!(local.post(login_url).send().await.unwrap().status(), 401);
                assert!(body.contains(&format!(
                    "https://localhost:{port}/transfer/schema?signature=public"
                )));
            }
        }
        versions_status.store(200, Ordering::SeqCst);
        let observed = received.lock().unwrap()[requests_before_discovery..].to_vec();
        assert_eq!(
            observed.len(),
            4,
            "Discovery must not trigger credential refresh or other API requests"
        );
        assert!(
            observed
                .iter()
                .all(|(path, auth, _)| path == "/api/versions" && auth.is_empty())
        );
        if let Some(tool) = real_tool {
            let before_probe = received.lock().unwrap().len();
            let mut command = tokio::process::Command::new(tool);
            command
                .arg("--machineOutput")
                .arg(format!("--I:sourceSessionTicket={}", bridge.ticket))
                .arg(format!("--sourceSSLThumbprint={}", bridge.thumbprint))
                .arg(format!(
                    "vcloud://127.0.0.1:{}/?org=alpha&vdc=test&vapp=probe",
                    bridge.port
                ))
                .stdin(std::process::Stdio::null())
                .kill_on_drop(true);
            crate::ovf::hide_console(&mut command);
            let output = tokio::time::timeout(Duration::from_secs(25), command.output())
                .await
                .expect("Real OVF Tool probe timed out")
                .expect("Could not start real OVF Tool");
            let probe_requests = received.lock().unwrap()[before_probe..].to_vec();
            assert!(
                probe_requests
                    .iter()
                    .any(|(p, a, _)| p == "/api/versions" && a.is_empty()),
                "OVF Tool must negotiate public versions"
            );
            assert!(
                probe_requests
                    .iter()
                    .any(|(p, a, _)| p == "/api/org" && a == "Bearer test-access"),
                "OVF Tool must then present its independently supplied ticket: {} / {}",
                crate::store::redact(
                    &String::from_utf8_lossy(&output.stdout).replace(&bridge.ticket, "[ticket]")
                ),
                crate::store::redact(
                    &String::from_utf8_lossy(&output.stderr).replace(&bridge.ticket, "[ticket]")
                )
            );
            // The mock has no complete inventory, so the final probe exit code is
            // not an export result. This checks the real discovery/auth handshake.
        }
        let cookie_session = local
            .post(format!("{base}/api/sessions"))
            .header("cookie", format!("vcloud_session_id={}", bridge.ticket))
            .send()
            .await
            .unwrap();
        assert_eq!(cookie_session.status(), 200);
        assert_eq!(
            cookie_session
                .headers()
                .get("x-vcloud-authorization")
                .unwrap(),
            &bridge.ticket
        );
        for path in [
            "/api/session",
            "/_cap/wrong/api/session",
            "/_remote/wrong",
            "/api/%76ersions",
        ] {
            assert_eq!(
                local
                    .get(format!("{base}{path}"))
                    .header("x-vcloud-authorization", "wrong")
                    .send()
                    .await
                    .unwrap()
                    .status(),
                401
            );
        }
        assert_eq!(
            local
                .get(format!("{base}/api/session"))
                .send()
                .await
                .unwrap()
                .status(),
            401
        );
        let session_xml = local
            .get(format!("{base}/api/session"))
            .header("x-vcloud-authorization", &bridge.ticket)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(session_xml.contains("/_cap/"));
        assert!(!session_xml.contains("test-access"));
        let xml = local
            .get(format!("{base}/api/vApp/vapp-1"))
            .header("accept", "application/*+xml")
            .header("x-vcloud-authorization", &bridge.ticket)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        let links = roxmltree::Document::parse(&xml)
            .unwrap()
            .descendants()
            .filter_map(|n| n.attribute("href").map(str::to_owned))
            .collect::<Vec<_>>();
        assert_eq!(
            local
                .get(&links[0])
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "payload-from-cloud"
        );
        assert_eq!(
            local
                .get(&links[1])
                .send()
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "external-payload"
        );
        let upload = local
            .put(format!("{base}/_cap/{}/transfer/upload", bridge.ticket))
            .header("content-type", "application/octet-stream")
            .body("upload-body")
            .send()
            .await
            .unwrap();
        assert!(upload.status().is_success());
        assert_eq!(traffic.sent.load(Ordering::Relaxed), 11);
        assert_eq!(traffic.received.load(Ordering::Relaxed), 34);
        let power = local
            .post(format!("{base}/api/vApp/vapp-1/power/action/powerOff"))
            .header("x-vcloud-authorization", &bridge.ticket)
            .send()
            .await
            .unwrap();
        assert_eq!(power.status(), 403);
        assert!(
            !received
                .lock()
                .unwrap()
                .iter()
                .any(|(p, _, _)| p.contains("powerOff"))
        );
        local
            .post(format!("{base}/api/create-test-template"))
            .header("x-vcloud-authorization", &bridge.ticket)
            .header("content-type", "application/xml")
            .body("<Create/>")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap();
        assert_eq!(traffic.cloud.lock().unwrap().progress(), Some(5));
        assert!(!traffic.cloud.lock().unwrap().upload_ready());
        let temp = tempfile::tempdir().unwrap();
        let manager = crate::jobs::JobManager::new(
            crate::store::Store::for_test(temp.path().into()),
            tokio::runtime::Handle::current(),
        );
        let start = std::time::Instant::now();
        manager
            .await_cloud(
                Uuid::new_v4(),
                &session,
                &traffic,
                &CancellationToken::new(),
                true,
                start,
            )
            .await
            .unwrap();
        assert!(
            task_polls.load(Ordering::SeqCst) >= 2,
            "Must poll pending import after OVF Tool exits"
        );
        assert!(
            start.elapsed() >= Duration::from_secs(6),
            "Must see stable resource readiness"
        );
        assert!(traffic.cloud.lock().unwrap().upload_ready());
        // Same local TLS endpoint, direct and through the production streaming adapter.
        // This is a proxy sanity benchmark, not a measurement of the user's cloud/disk.
        for (label, url, client) in [
            (
                "direct TLS",
                format!("https://127.0.0.1:{port}/transfer/benchmark"),
                &session.client,
            ),
            (
                "OVF UI TLS bridge",
                format!("{base}/_cap/{}/transfer/benchmark", bridge.ticket),
                &local,
            ),
        ] {
            let start = std::time::Instant::now();
            let mut response = client.get(url).send().await.unwrap();
            let mut bytes = 0;
            while let Some(chunk) = response.chunk().await.unwrap() {
                assert!(chunk.iter().all(|b| *b == b'x'));
                bytes += chunk.len();
            }
            assert_eq!(bytes, 32 * 1024 * 1024);
            eprintln!(
                "Local 32 MiB benchmark {label}: {:.1} MiB/s (test build, no disk)",
                32.0 / start.elapsed().as_secs_f64()
            );
        }
        let payload = Bytes::from(vec![b'x'; 32 * 1024 * 1024]);
        for (label, url, client) in [
            (
                "direct TLS upload",
                format!("https://127.0.0.1:{port}/transfer/benchmark-upload"),
                &session.client,
            ),
            (
                "OVF UI TLS bridge upload",
                format!("{base}/_cap/{}/transfer/benchmark-upload", bridge.ticket),
                &local,
            ),
        ] {
            let start = std::time::Instant::now();
            let response = client.put(url).body(payload.clone()).send().await.unwrap();
            assert!(response.status().is_success());
            response.bytes().await.unwrap();
            eprintln!(
                "Local 32 MiB benchmark {label}: {:.1} MiB/s (test build, no disk)",
                32.0 / start.elapsed().as_secs_f64()
            );
        }
        drop(bridge);
        server.abort();
    }
}
