//! Local TLS regression tests. No cloud credentials or external endpoints are used.
use super::*;
use crate::model::{AuthKind, Endpoint, Secret, Tenant};
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Default)]
struct Probe {
    exchanges: AtomicUsize,
    uploads: AtomicUsize,
    rejected_uploads: AtomicUsize,
    upload_bytes: AtomicUsize,
    fail_refresh: AtomicBool,
    legacy_login_status: AtomicUsize,
    modern_logins: AtomicUsize,
    oauth_status: AtomicUsize,
    disconnect_refresh: AtomicBool,
    api_calls: AtomicUsize,
    last_token: AtomicUsize,
    race: Option<tokio::sync::Barrier>,
}

struct Fixture {
    endpoint: Endpoint,
    client: reqwest::Client,
    probe: Arc<Probe>,
    server: tokio::task::JoinHandle<()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new(ttl: u64, concurrent: usize) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
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
        let probe = Arc::new(Probe {
            race: (concurrent > 0).then(|| tokio::sync::Barrier::new(concurrent)),
            ..Default::default()
        });
        let state = probe.clone();
        let server = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    Some(_) = connections.join_next(), if !connections.is_empty() => {},
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        let acceptor = acceptor.clone();
                        let state = state.clone();
                        connections.spawn(async move {
                            let tls = acceptor.accept(socket).await.unwrap();
                            let service = service_fn(move |request| {
                                let state = state.clone();
                                async move { Ok::<_, Infallible>(serve(request, state, ttl).await) }
                            });
                            let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(tls), service).await;
                        });
                    }
                }
            }
        });
        Self {
            endpoint: Endpoint {
                id: Uuid::new_v4(),
                name: "TLS fixture".into(),
                url: format!("https://127.0.0.1:{port}"),
                tenants: vec![],
            },
            client: reqwest::Client::builder()
                .add_root_certificate(root)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap(),
            probe,
            server,
        }
    }

    async fn login(&self, auth: AuthKind) -> CloudSession {
        CloudSession::login_with_client(
            &self.endpoint,
            &Tenant {
                id: Uuid::new_v4(),
                organization: "fixture".into(),
                username: String::new(),
                auth,
            },
            Secret {
                value: "fixture-1".into(),
                session_token: String::new(),
            },
            self.client.clone(),
        )
        .await
        .unwrap()
    }
}

async fn serve(request: Request<Incoming>, probe: Arc<Probe>, ttl: u64) -> Response<Body> {
    let path = request.uri().path().to_owned();
    let authorization = request
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_owned();
    match path.as_str() {
        "/api/sessions" => plain(
            probe.legacy_login_status.load(Ordering::SeqCst) as u16,
            "Legacy login unavailable",
        ),
        "/cloudapi/1.0.0/sessions" | "/cloudapi/1.0.0/sessions/provider" => {
            use base64::Engine;
            let organization = if path.ends_with("/provider") {
                "system"
            } else {
                "fixture"
            };
            let expected = format!(
                "Basic {}",
                base64::engine::general_purpose::STANDARD
                    .encode(format!("qa@{organization}:fixture-password"))
            );
            if authorization != expected
                || request.headers().get("accept").map(|h| h.as_bytes())
                    != Some(b"application/json;version=39.0".as_slice())
            {
                return plain(401, "Unexpected credentials or Accept");
            }
            probe.modern_logins.fetch_add(1, Ordering::SeqCst);
            Response::builder()
                .status(200)
                .header("X-VMWARE-VCLOUD-ACCESS-TOKEN", "fixture-modern")
                .header("content-type", "application/json")
                .body(full("{\"id\":\"fixture-session\"}"))
                .unwrap()
        }
        "/api/versions" => plain(
            200,
            "<SupportedVersions><VersionInfo><Version>39.0</Version></VersionInfo></SupportedVersions>",
        ),
        "/oauth/tenant/fixture/token" => {
            let generation = probe.exchanges.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::time::sleep(Duration::from_millis(40)).await;
            if probe.fail_refresh.load(Ordering::SeqCst) {
                return plain(401, r#"{"error_description":"fixture refresh revoked"}"#);
            }
            let status = probe.oauth_status.load(Ordering::SeqCst);
            if status != 0 {
                return plain(
                    status as u16,
                    r#"{"error_description":"fixture OAuth temporarily unavailable"}"#,
                );
            }
            if probe.disconnect_refresh.load(Ordering::SeqCst) {
                let stream = futures_util::stream::once(async {
                    Err::<Frame<Bytes>, _>(std::io::Error::new(
                        std::io::ErrorKind::ConnectionReset,
                        "fixture disconnected",
                    ))
                });
                return Response::builder()
                    .status(200)
                    .header("content-length", "128")
                    .body(
                        BodyExt::map_err(
                            StreamBody::new(stream),
                            |error| -> Box<dyn std::error::Error + Send + Sync> { Box::new(error) },
                        )
                        .boxed_unsync(),
                    )
                    .unwrap();
            }
            plain(
                200,
                &format!(r#"{{"access_token":"fixture-{generation}","expires_in":{ttl}}}"#),
            )
        }
        "/api/session" => {
            probe.api_calls.fetch_add(1, Ordering::SeqCst);
            probe.last_token.store(
                authorization
                    .strip_prefix("Bearer fixture-")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                Ordering::SeqCst,
            );
            plain(
                if authorization.starts_with("Bearer fixture-") {
                    200
                } else {
                    401
                },
                "<Session/>",
            )
        }
        "/api/race" => {
            if authorization == "Bearer fixture-1" {
                probe.race.as_ref().unwrap().wait().await;
                plain(401, "Expired")
            } else {
                plain(200, "<Success/>")
            }
        }
        "/transfer/range" => {
            if authorization == "Bearer fixture-1" {
                return plain(401, "Expired");
            }
            if request.headers().get("range").map(|h| h.as_bytes()) != Some(b"bytes=4-7".as_slice())
            {
                return plain(400, "Range header lost");
            }
            Response::builder()
                .status(206)
                .header("content-range", "bytes 4-7/8")
                .body(full("4567"))
                .unwrap()
        }
        "/transfer/upload" => {
            probe.uploads.fetch_add(1, Ordering::SeqCst);
            let bytes = request.into_body().collect().await.unwrap().to_bytes();
            probe.upload_bytes.store(bytes.len(), Ordering::SeqCst);
            if bytes.iter().any(|b| *b != 42) {
                return plain(400, "Corrupt stream");
            }
            plain(200, "Uploaded")
        }
        "/transfer/reject-upload" => {
            probe.rejected_uploads.fetch_add(1, Ordering::SeqCst);
            let _ = request.into_body().collect().await.unwrap();
            plain(401, "Expired")
        }
        "/transfer/download" => {
            let stream = futures_util::stream::unfold(0, |n| async move {
                if n == 6 {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(600)).await;
                Some((
                    Ok::<_, std::io::Error>(Frame::data(Bytes::from(vec![42; 65536]))),
                    n + 1,
                ))
            });
            Response::builder()
                .header("content-type", "application/octet-stream")
                .body(
                    BodyExt::map_err(
                        StreamBody::new(stream),
                        |e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) },
                    )
                    .boxed_unsync(),
                )
                .unwrap()
        }
        "/api/expired" => plain(401, "Expired"),
        _ => plain(404, "Unknown fixture path"),
    }
}

#[tokio::test]
async fn password_uses_modern_login_only_when_legacy_route_is_unavailable() {
    let fixture = Fixture::new(3600, 0).await;
    for organization in ["fixture", "system"] {
        let tenant = Tenant {
            id: Uuid::new_v4(),
            organization: organization.into(),
            username: "qa".into(),
            auth: AuthKind::Password,
        };
        for status in [404, 405, 401, 403, 429, 500] {
            fixture
                .probe
                .legacy_login_status
                .store(status, Ordering::SeqCst);
            let before = fixture.probe.modern_logins.load(Ordering::SeqCst);
            let result = CloudSession::login_with_client(
                &fixture.endpoint,
                &tenant,
                Secret {
                    value: "fixture-password".into(),
                    session_token: String::new(),
                },
                fixture.client.clone(),
            )
            .await;
            if matches!(status, 404 | 405) {
                let session =
                    result.expect("Modern login token must also authorize legacy GET api/session");
                assert_eq!(session.get("api/session").await.unwrap(), "<Session/>");
                assert_eq!(
                    fixture.probe.modern_logins.load(Ordering::SeqCst),
                    before + 1
                );
            } else {
                assert!(result.is_err());
                assert_eq!(
                    fixture.probe.modern_logins.load(Ordering::SeqCst),
                    before,
                    "Do not retry rejected credentials/rate limits/server errors"
                );
            }
        }
    }
}

fn bridge_client() -> reqwest::Client {
    // Synthetic loopback adapter only. Production still pins its generated certificate.
    reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap()
}

#[tokio::test]
async fn concurrent_401_refreshes_once_and_read_retry_keeps_range() {
    let fixture = Fixture::new(3600, 8).await;
    let session = fixture.login(AuthKind::ApiToken).await;
    let requests = (0..8).map(|_| session.get("api/race"));
    let results = tokio::time::timeout(
        Duration::from_secs(10),
        futures_util::future::join_all(requests),
    )
    .await
    .unwrap();
    assert!(
        results
            .iter()
            .all(|result| result.as_ref().is_ok_and(|s| s == "<Success/>"))
    );
    assert_eq!(
        fixture.probe.exchanges.load(Ordering::SeqCst),
        2,
        "Only one renewal for eight simultaneous stale-token responses"
    );

    let fixture = Fixture::new(3600, 0).await;
    let bridge = Bridge::start(
        fixture.login(AuthKind::ApiToken).await,
        Arc::new(Traffic::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let response = bridge_client()
        .get(format!(
            "https://127.0.0.1:{}/_cap/{}/transfer/range",
            bridge.port, bridge.ticket
        ))
        .header("range", "bytes=4-7")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(response.bytes().await.unwrap(), "4567");
    assert_eq!(fixture.probe.exchanges.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn short_ttl_renews_during_streams_and_never_replays_upload() {
    let fixture = Fixture::new(2, 0).await;
    let bridge = Bridge::start(
        fixture.login(AuthKind::ApiToken).await,
        Arc::new(Traffic::default()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let local = bridge_client();
    let base = format!(
        "https://127.0.0.1:{}/_cap/{}/transfer",
        bridge.port, bridge.ticket
    );
    let chunks = futures_util::stream::unfold(0, |n| async move {
        if n == 6 {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
        Some((Ok::<_, std::io::Error>(Bytes::from(vec![42; 65536])), n + 1))
    });
    let upload = local
        .put(format!("{base}/upload"))
        .header("content-type", "application/octet-stream")
        .body(reqwest::Body::wrap_stream(chunks))
        .send();
    let download = async {
        local
            .get(format!("{base}/download"))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
    };
    let (upload, download) = tokio::join!(upload, download);
    assert!(upload.unwrap().status().is_success());
    assert_eq!(download.len(), 6 * 65536);
    assert!(download.iter().all(|b| *b == 42));
    assert_eq!(fixture.probe.uploads.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.probe.upload_bytes.load(Ordering::SeqCst), 6 * 65536);
    assert!(
        fixture.probe.exchanges.load(Ordering::SeqCst) >= 3,
        "Refresh must run while both streams are in flight without API polls"
    );
    let rejected = local
        .put(format!("{base}/reject-upload"))
        .header("content-type", "application/octet-stream")
        .body(vec![42; 1024])
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), 401);
    assert_eq!(fixture.probe.rejected_uploads.load(Ordering::SeqCst), 1);
    assert!(
        bridge
            .traffic
            .diagnostics()
            .iter()
            .any(|line| line.contains("не повторяется автоматически"))
    );
    drop(bridge);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let count = fixture.probe.exchanges.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(
        fixture.probe.exchanges.load(Ordering::SeqCst),
        count,
        "Closing bridge stops renewal"
    );
}

#[tokio::test]
async fn revoked_refresh_and_expired_bearer_are_actionable() {
    let fixture = Fixture::new(3600, 0).await;
    let session = fixture.login(AuthKind::ApiToken).await;
    fixture.probe.fail_refresh.store(true, Ordering::SeqCst);
    let results = futures_util::future::join_all((0..8).map(|_| session.get("api/expired"))).await;
    assert!(results.iter().all(|r| {
        r.as_ref()
            .is_err_and(|e| e.to_string().contains("Не удалось продлить API-сессию"))
    }));
    assert_eq!(
        fixture.probe.exchanges.load(Ordering::SeqCst),
        2,
        "Failed refresh is shared during cooldown"
    );
    fixture.probe.fail_refresh.store(false, Ordering::SeqCst);
    let bearer = fixture.login(AuthKind::BearerToken).await;
    let error = bearer.get("api/expired").await.unwrap_err().to_string();
    assert!(error.contains("Bearer token истёк"));
    assert!(error.contains("refresh token"));
    assert_eq!(
        fixture.probe.exchanges.load(Ordering::SeqCst),
        2,
        "Raw Bearer must not pretend to refresh"
    );
}

#[tokio::test]
async fn proactive_transient_failure_uses_valid_token_then_recovers() {
    for disconnect in [false, true] {
        let fixture = Fixture::new(4, 0).await;
        let session = fixture.login(AuthKind::ApiToken).await;
        if disconnect {
            fixture
                .probe
                .disconnect_refresh
                .store(true, Ordering::SeqCst);
        } else {
            fixture.probe.oauth_status.store(503, Ordering::SeqCst);
        }
        tokio::time::sleep(session.renewal_delay().await.unwrap()).await;
        // Only a proactive transport/5xx failure may retain the old token, while its
        // conservative safe lifetime remains. The TLS server records the token used.
        assert_eq!(session.get("api/session").await.unwrap(), "<Session/>");
        assert_eq!(fixture.probe.last_token.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.probe.exchanges.load(Ordering::SeqCst), 2);
        fixture
            .probe
            .disconnect_refresh
            .store(false, Ordering::SeqCst);
        fixture.probe.oauth_status.store(0, Ordering::SeqCst);
        tokio::time::sleep(session.renewal_delay().await.unwrap()).await;
        assert_eq!(session.get("api/session").await.unwrap(), "<Session/>");
        assert_eq!(fixture.probe.last_token.load(Ordering::SeqCst), 3);
    }
}

#[tokio::test]
async fn expired_token_never_falls_back_after_transient_failure() {
    let fixture = Fixture::new(2, 0).await;
    let session = fixture.login(AuthKind::ApiToken).await;
    fixture.probe.oauth_status.store(503, Ordering::SeqCst);
    tokio::time::sleep(session.renewal_delay().await.unwrap()).await;
    assert!(session.get("api/session").await.is_ok());
    let calls = fixture.probe.api_calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(450)).await;
    assert!(session.get("api/session").await.is_err());
    assert_eq!(
        fixture.probe.api_calls.load(Ordering::SeqCst),
        calls,
        "No API request with expired old token, even though fixture would accept it"
    );
}

#[tokio::test]
async fn oauth_zero_lifetime_is_not_extended_artificially() {
    let fixture = Fixture::new(0, 0).await;
    let result = CloudSession::login_with_client(
        &fixture.endpoint,
        &Tenant {
            id: Uuid::new_v4(),
            organization: "fixture".into(),
            username: String::new(),
            auth: AuthKind::ApiToken,
        },
        Secret {
            value: "fixture-1".into(),
            session_token: String::new(),
        },
        fixture.client.clone(),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(
        fixture.probe.api_calls.load(Ordering::SeqCst),
        0,
        "Already expired token must never authorize an API call"
    );
}

#[tokio::test]
async fn revoked_proactive_refresh_never_uses_still_valid_old_token() {
    let fixture = Fixture::new(3, 0).await;
    let session = fixture.login(AuthKind::ApiToken).await;
    fixture.probe.fail_refresh.store(true, Ordering::SeqCst);
    let calls = fixture.probe.api_calls.load(Ordering::SeqCst);
    tokio::time::sleep(session.renewal_delay().await.unwrap()).await;
    assert!(
        session
            .get("api/session")
            .await
            .unwrap_err()
            .to_string()
            .contains("Не удалось продлить")
    );
    assert_eq!(
        fixture.probe.api_calls.load(Ordering::SeqCst),
        calls,
        "4xx revocation forbids fallback before expiry too"
    );
}

#[tokio::test]
async fn reactive_401_failure_never_reuses_old_token_on_following_requests() {
    let fixture = Fixture::new(3600, 0).await;
    let session = fixture.login(AuthKind::ApiToken).await;
    fixture.probe.oauth_status.store(503, Ordering::SeqCst);
    let calls = fixture.probe.api_calls.load(Ordering::SeqCst);
    assert!(session.get("api/expired").await.is_err());
    assert!(session.get("api/session").await.is_err());
    assert_eq!(fixture.probe.api_calls.load(Ordering::SeqCst), calls);
    assert_eq!(
        fixture.probe.exchanges.load(Ordering::SeqCst),
        2,
        "Reactive failure is shared during backoff"
    );

    // Mutations are not replayed; their 401 also invalidates subsequent token fallback.
    let fixture = Fixture::new(3600, 0).await;
    let session = fixture.login(AuthKind::ApiToken).await;
    assert!(
        session
            .post_xml("api/expired", "application/xml", "<Test/>".into())
            .await
            .is_err()
    );
    fixture.probe.oauth_status.store(503, Ordering::SeqCst);
    let calls = fixture.probe.api_calls.load(Ordering::SeqCst);
    assert!(session.get("api/session").await.is_err());
    assert_eq!(fixture.probe.api_calls.load(Ordering::SeqCst), calls);

    // An older response must not reuse a newer generation that has also been rejected.
    let fixture = Fixture::new(3600, 0).await;
    let session = fixture.login(AuthKind::ApiToken).await;
    assert!(session.get("api/expired").await.is_err()); // Both generations return 401.
    assert_eq!(fixture.probe.exchanges.load(Ordering::SeqCst), 2);
    fixture.probe.oauth_status.store(503, Ordering::SeqCst);
    assert!(session.refresh_after_unauthorized(0).await.is_err());
}
