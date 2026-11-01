use crate::model::*;
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Method, RequestBuilder, Response};
use roxmltree::Document;
use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};
use url::Url;

const MAX_XML: usize = 16 * 1024 * 1024;

struct TokenTiming {
    renew_at: Instant,
    expires_at: Instant,
    margin: Duration,
    rejected: bool,
}
impl TokenTiming {
    fn new(issued_at: Instant, ttl: Duration) -> Self {
        Self {
            renew_at: issued_at + ttl.mul_f64(0.8),
            expires_at: issued_at + ttl,
            margin: ttl.div_f64(10.0).min(Duration::from_secs(30)),
            rejected: false,
        }
    }
    fn usable_until(&self) -> Instant {
        self.expires_at - self.margin
    }
    fn usable(&self) -> bool {
        !self.rejected && Instant::now() < self.usable_until()
    }
}

struct RenewError {
    retry_at: Instant,
    message: String,
    transient: bool,
}

#[derive(Debug)]
struct ExchangeFailure {
    message: String,
    transient: bool,
}
impl std::fmt::Display for ExchangeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for ExchangeFailure {}

fn transport_failure(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(|e| e.is_timeout() || e.is_connect() || e.is_body() || e.is_request())
}

#[derive(Clone)]
pub struct CloudSession {
    pub client: Client,
    pub base: Url,
    pub version: String,
    pub tenant: Tenant,
    // Header and generation are read together, so a late 401 cannot invalidate a newer token.
    auth: Arc<RwLock<(String, String, u64)>>,
    refresh_secret: Option<Secret>,
    timing: Arc<Mutex<TokenTiming>>,
    renew_gate: Arc<Mutex<()>>,
    renew_error: Arc<Mutex<Option<RenewError>>>,
}

pub fn endpoint_url(input: &str) -> Result<Url> {
    let mut url = Url::parse(input.trim()).context("Укажите полный HTTPS-адрес endpoint")?;
    ensure!(
        url.scheme() == "https" && url.host_str().is_some(),
        "Endpoint должен использовать HTTPS"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "Не указывайте пароль в адресе endpoint"
    );
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "Укажите адрес сервера без параметров"
    );
    // Portal URLs are accepted, API and OVF locators are always based on the origin.
    url.set_path("/");
    Ok(url)
}

pub async fn limited_text(mut response: Response) -> Result<String> {
    let status = response.status();
    let mut data = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("Ошибка чтения ответа сервера")?
    {
        ensure!(
            data.len() + chunk.len() <= MAX_XML,
            "Ответ API превышает 16 МиБ"
        );
        data.extend_from_slice(&chunk);
    }
    let text = String::from_utf8(data).context("Ответ API содержит некорректный UTF-8")?;
    ensure!(
        status.is_success(),
        "Cloud Director: HTTP {}. {}",
        status.as_u16(),
        api_error(&text)
    );
    Ok(text)
}

pub fn api_error(text: &str) -> String {
    let message = Document::parse(text)
        .ok()
        .and_then(|d| {
            d.descendants()
                .find(|n| n.has_tag_name("Error"))
                .and_then(|n| n.attribute("message"))
                .map(str::to_owned)
        })
        .or_else(|| {
            serde_json::from_str::<serde_json::Value>(text)
                .ok()
                .and_then(|v| {
                    v.get("message")
                        .or(v.get("error_description"))
                        .and_then(|v| v.as_str())
                        .map(str::to_owned)
                })
        })
        .unwrap_or_else(|| "Сервер не передал описание ошибки.".into());
    crate::store::redact(&message).chars().take(1000).collect()
}

pub fn choose_version(xml: &str) -> Result<String> {
    let doc = Document::parse(xml)?;
    let mut versions: Vec<(u32, u32)> = doc
        .descendants()
        .filter(|n| n.has_tag_name("VersionInfo"))
        .filter_map(|n| n.children().find(|c| c.has_tag_name("Version"))?.text())
        .filter_map(|s| {
            let (a, b) = s.split_once('.')?;
            Some((a.parse().ok()?, b.parse().ok()?))
        })
        .filter(|v| *v >= (33, 0) && *v <= (39, 0))
        .collect();
    versions.sort();
    let (a, b) = versions
        .last()
        .context("Нет поддерживаемой API-версии 33.0–39.0")?;
    Ok(format!("{a}.{b}"))
}

impl CloudSession {
    pub async fn login(endpoint: &Endpoint, tenant: &Tenant, secret: Secret) -> Result<Self> {
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(20))
            .tcp_nodelay(true)
            .pool_max_idle_per_host(16)
            .user_agent(concat!("OVF-UI/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Self::login_with_client(endpoint, tenant, secret, client).await
    }

    pub(crate) async fn login_with_client(
        endpoint: &Endpoint,
        tenant: &Tenant,
        secret: Secret,
        client: Client,
    ) -> Result<Self> {
        let base = endpoint_url(&endpoint.url)?;
        let versions = limited_text(
            client
                .get(base.join("api/versions")?)
                .timeout(Duration::from_secs(30))
                .send()
                .await
                .context("Endpoint недоступен или TLS-сертификат не прошёл проверку")?,
        )
        .await?;
        let version = choose_version(&versions)?;
        let mut timing = TokenTiming::new(Instant::now(), Duration::from_secs(3600));
        let auth: (String, String) = match tenant.auth {
            AuthKind::Password => {
                let suffix = format!("@{}", tenant.organization);
                let user = if tenant.username.ends_with(&suffix) {
                    tenant.username.clone()
                } else {
                    format!("{}{}", tenant.username, suffix)
                };
                let mut response = client
                    .post(base.join("api/sessions")?)
                    .timeout(Duration::from_secs(30))
                    .header("Accept", format!("application/*+xml;version={version}"))
                    .basic_auth(&user, Some(&secret.value))
                    .send()
                    .await
                    .context("Не удалось выполнить вход")?;
                // Some vCD deployments retire the legacy login route while keeping the
                // XML inventory/export APIs. Never retry rejected credentials (401/403)
                // or transient/server failures against a different authentication route.
                let mut login_path = "api/sessions";
                if matches!(response.status().as_u16(), 404 | 405) {
                    login_path = if tenant.organization.eq_ignore_ascii_case("system") {
                        "cloudapi/1.0.0/sessions/provider"
                    } else {
                        "cloudapi/1.0.0/sessions"
                    };
                    response = client
                        .post(base.join(login_path)?)
                        .timeout(Duration::from_secs(30))
                        .header("Accept", format!("application/json;version={version}"))
                        .basic_auth(&user, Some(&secret.value))
                        .send()
                        .await
                        .context("Не удалось выполнить вход через Cloud Director OpenAPI")?;
                }
                ensure!(
                    response.status().is_success(),
                    "Вход отклонён ({login_path}): HTTP {}. Проверьте логин, организацию и пароль.",
                    response.status().as_u16()
                );
                let headers = response.headers();
                if let Some(t) = headers.get("x-vmware-vcloud-access-token") {
                    ("Authorization".into(), format!("Bearer {}", t.to_str()?))
                } else if let Some(t) = headers.get("x-vcloud-authorization") {
                    ("x-vcloud-authorization".into(), t.to_str()?.into())
                } else {
                    bail!("Сервер не выдал токен сессии")
                }
            }
            AuthKind::ApiToken => {
                let (token, token_timing) =
                    Self::exchange(&client, &base, &tenant.organization, &secret.value).await?;
                timing = token_timing;
                ("Authorization".into(), format!("Bearer {token}"))
            }
            AuthKind::BearerToken => (
                "Authorization".into(),
                format!("Bearer {}", secret.value.trim()),
            ),
        };
        let session = Self {
            client,
            base,
            version,
            tenant: tenant.clone(),
            auth: Arc::new(RwLock::new((auth.0, auth.1, 0))),
            refresh_secret: (tenant.auth == AuthKind::ApiToken).then_some(secret),
            timing: Arc::new(Mutex::new(timing)),
            renew_gate: Arc::new(Mutex::new(())),
            renew_error: Arc::new(Mutex::new(None)),
        };
        session
            .get("api/session")
            .await
            .context("Проверка сессии не удалась")?;
        Ok(session)
    }

    async fn exchange(
        client: &Client,
        base: &Url,
        org: &str,
        token: &str,
    ) -> Result<(String, TokenTiming)> {
        let mut url = base.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("Некорректный endpoint"))?
            .extend(["oauth", "tenant", org, "token"]);
        // Starting the clock before the exchange is conservative: a slow OAuth reply
        // must not extend our estimate of how long its access token remains valid.
        let issued_at = Instant::now();
        let r = client
            .post(url)
            .timeout(Duration::from_secs(30))
            .header("Accept", "application/json")
            .form(&[("grant_type", "refresh_token"), ("refresh_token", token)])
            .send()
            .await
            .map_err(|error| {
                let error = anyhow::Error::new(error);
                ExchangeFailure {
                    transient: transport_failure(&error),
                    message: crate::store::redact(&format!("Обмен API token не выполнен: {error}")),
                }
            })?;
        let status = r.status();
        let text = limited_text(r).await.map_err(|error| ExchangeFailure {
            // A failed/incomplete 4xx response must never fall back to an old token.
            transient: status.is_server_error()
                || (status.is_success() && transport_failure(&error)),
            message: crate::store::redact(&format!("{error:#}")),
        })?;
        let value: serde_json::Value = serde_json::from_str(&text)?;
        let token = value
            .get("access_token")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .context("Сервер не вернул access_token")?
            .to_owned();
        let ttl = value
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .filter(|ttl| *ttl > 0)
            .context("OAuth не вернул положительный expires_in; безопасный срок действия access token неизвестен")?
            .min(86400);
        let timing = TokenTiming::new(issued_at, Duration::from_secs(ttl));
        ensure!(
            timing.usable(),
            "OAuth выдал токен, срок безопасного использования которого уже истёк"
        );
        Ok((token, timing))
    }

    pub(crate) fn authentication_error(&self) -> &'static str {
        match self.tenant.auth {
            AuthKind::BearerToken => {
                "Bearer token истёк или отозван. Подключитесь заново; для длительных передач выберите API token (refresh token). Bearer без refresh token автоматически продлить невозможно."
            }
            AuthKind::ApiToken => {
                "Cloud Director отклонил обновлённую сессию. Проверьте срок действия и права API token и подключитесь заново."
            }
            AuthKind::Password => {
                "Сессия входа истекла или отозвана. Подключитесь заново с логином и паролем."
            }
        }
    }

    // Caller holds renew_gate. Failed exchanges are briefly shared with queued callers,
    // instead of letting dozens of parallel transfer requests hammer the OAuth endpoint.
    async fn refresh_locked(&self) -> Result<()> {
        let secret = self
            .refresh_secret
            .as_ref()
            .context(self.authentication_error())?;
        if let Some(error) = &*self.renew_error.lock().await
            && Instant::now() < error.retry_at
        {
            bail!("{}", error.message);
        }
        let result = Self::exchange(
            &self.client,
            &self.base,
            &self.tenant.organization,
            &secret.value,
        )
        .await;
        let (token, timing) = match result {
            Ok(value) => value,
            Err(error) => {
                let transient = error
                    .downcast_ref::<ExchangeFailure>()
                    .is_some_and(|failure| failure.transient);
                let message = crate::store::redact(&format!(
                    "Не удалось продлить API-сессию: {error:#}. Проверьте соединение и срок действия API token. Поток загрузки автоматически не повторяется."
                ));
                let mut timing = self.timing.lock().await;
                if !transient {
                    timing.rejected = true;
                }
                let retry = if transient && timing.usable() {
                    timing
                        .usable_until()
                        .saturating_duration_since(Instant::now())
                        .div_f64(2.0)
                        .clamp(Duration::from_millis(50), Duration::from_secs(5))
                } else {
                    Duration::from_secs(5)
                };
                drop(timing);
                *self.renew_error.lock().await = Some(RenewError {
                    retry_at: Instant::now() + retry,
                    message: message.clone(),
                    transient,
                });
                bail!("{message}");
            }
        };
        let mut auth = self.auth.write().await;
        let generation = auth.2.wrapping_add(1);
        *auth = (
            "Authorization".into(),
            format!("Bearer {token}"),
            generation,
        );
        *self.timing.lock().await = timing;
        *self.renew_error.lock().await = None;
        Ok(())
    }

    pub(crate) async fn refresh_after_unauthorized(&self, generation: u64) -> Result<()> {
        let _gate = self.renew_gate.lock().await;
        let same_generation = self.auth.read().await.2 == generation;
        if !same_generation && self.timing.lock().await.usable() {
            return Ok(());
        }
        // A server-side 401 overrides local TTL estimates. This remains sticky even
        // if the reactive refresh fails transiently; only a new token clears it.
        if same_generation {
            self.timing.lock().await.rejected = true;
        }
        self.refresh_locked().await
    }

    pub(crate) async fn mark_unauthorized(&self, generation: u64) {
        let _gate = self.renew_gate.lock().await;
        if self.auth.read().await.2 == generation {
            self.timing.lock().await.rejected = true;
        }
    }

    pub(crate) async fn renewal_delay(&self) -> Option<Duration> {
        self.refresh_secret.as_ref()?;
        let mut deadline = self.timing.lock().await.renew_at;
        if let Some(error) = &*self.renew_error.lock().await {
            deadline = error.retry_at;
        }
        Some(
            deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(100)),
        )
    }

    pub async fn ensure_fresh(&self) -> Result<()> {
        self.renew_if_due(true).await
    }

    // Background failures are reported even when foreground API calls can still use
    // the existing token during its remaining safe lifetime.
    pub(crate) async fn renew_in_background(&self) -> Result<()> {
        self.renew_if_due(false).await
    }

    async fn renew_if_due(&self, allow_valid_fallback: bool) -> Result<()> {
        if self.refresh_secret.is_none() {
            return Ok(());
        }
        let _gate = self.renew_gate.lock().await;
        let timing = self.timing.lock().await;
        let due = Instant::now() >= timing.renew_at || timing.rejected;
        drop(timing);
        if due && let Err(error) = self.refresh_locked().await {
            let transient = self
                .renew_error
                .lock()
                .await
                .as_ref()
                .is_some_and(|failure| failure.transient);
            if !(allow_valid_fallback && transient && self.timing.lock().await.usable()) {
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn checked_url(&self, href: &str) -> Result<Url> {
        let resolved = if let Some(id) = href.strip_prefix("urn:vcloud:vapp:") {
            format!("api/vApp/vapp-{id}")
        } else {
            href.to_owned()
        };
        let u = self.base.join(&resolved)?;
        ensure!(
            u.origin() == self.base.origin() && u.username().is_empty() && u.password().is_none(),
            "API-ссылка ведёт на другой сервер"
        );
        Ok(u)
    }

    pub async fn authorize(&self, builder: RequestBuilder) -> RequestBuilder {
        self.authorize_tracked(builder).await.0
    }

    pub(crate) async fn authorize_tracked(&self, builder: RequestBuilder) -> (RequestBuilder, u64) {
        let auth = self.auth.read().await;
        (builder.header(&auth.0, &auth.1), auth.2)
    }

    pub async fn get(&self, href: &str) -> Result<String> {
        self.ensure_fresh().await?;
        let u = self.checked_url(href)?;
        for attempt in 0..2 {
            let req = self
                .client
                .get(u.clone())
                .timeout(Duration::from_secs(60))
                .header(
                    "Accept",
                    format!("application/*+xml;version={}", self.version),
                );
            let (req, generation) = self.authorize_tracked(req).await;
            let r = req.send().await.context("Ошибка запроса API")?;
            if r.status().as_u16() == 401 && attempt == 0 && self.refresh_secret.is_some() {
                self.refresh_after_unauthorized(generation).await?;
                continue;
            }
            if r.status().as_u16() == 401 {
                self.mark_unauthorized(generation).await;
            }
            ensure!(
                r.status().as_u16() != 401,
                "{}",
                self.authentication_error()
            );
            return limited_text(r).await;
        }
        bail!("Сессия недействительна")
    }

    pub async fn post_xml(&self, href: &str, content_type: &str, body: String) -> Result<String> {
        self.ensure_fresh().await?;
        let req = self
            .client
            .request(Method::POST, self.checked_url(href)?)
            .timeout(Duration::from_secs(60))
            .header(
                "Accept",
                format!("application/*+xml;version={}", self.version),
            )
            .header("Content-Type", content_type)
            .body(body);
        let (request, generation) = self.authorize_tracked(req).await;
        let response = request.send().await.context("Операция API не выполнена")?;
        if response.status().as_u16() == 401 {
            self.mark_unauthorized(generation).await;
        }
        ensure!(
            response.status().as_u16() != 401,
            "{} Операция не повторена автоматически; проверьте её состояние в Cloud Director.",
            self.authentication_error()
        );
        limited_text(response).await
    }

    pub(crate) async fn query(&self, kind: &str) -> Result<Vec<CloudObject>> {
        let mut next = Some(format!("api/query?type={kind}&format=records&pageSize=128"));
        let mut visited = HashSet::new();
        let mut objects = Vec::new();
        while let Some(href) = next.take() {
            ensure!(
                visited.len() < 10000 && visited.insert(href.clone()),
                "API вернул зацикленную пагинацию"
            );
            let xml = self.get(&href).await?;
            let (mut page, link) = parse_records(&xml)?;
            objects.append(&mut page);
            next = link;
        }
        Ok(objects)
    }

    pub async fn inventory(&self) -> Result<Inventory> {
        let xml = self.get("api/org").await?;
        let org_href = {
            let doc = Document::parse(&xml)?;
            doc.descendants()
                .find(|n| {
                    n.has_tag_name("Org") && n.attribute("name") == Some(&self.tenant.organization)
                })
                .and_then(|n| n.attribute("href"))
                .context("Организация не найдена в доступных пользователю")?
                .to_owned()
        };
        let mut result = Inventory {
            org_href,
            api_version: self.version.clone(),
            ..Default::default()
        };
        for kind in ["orgVdc", "catalog", "vApp", "vm", "vAppTemplate", "media"] {
            match self.query(kind).await {
                Ok(mut items) => result.objects.append(&mut items),
                Err(e) => result.warnings.push(format!("{kind}: {e}")),
            }
        }
        result
            .objects
            .retain(|o| o.kind != ObjectKind::Catalog || !o.owner.trim().is_empty());
        // VMRecord storage includes swap/overhead. Read provisioned disk capacity
        // from each VM, without querying/exporting its sibling VMs.
        use futures_util::{StreamExt, stream};
        let vm_hrefs = result
            .objects
            .iter()
            .enumerate()
            .filter(|(_, o)| o.kind == ObjectKind::Vm)
            .map(|(index, o)| (index, o.href.clone()))
            .collect::<Vec<_>>();
        let sizes = stream::iter(vm_hrefs.into_iter().map(|(index, href)| async move {
            (
                index,
                self.get(&href).await.and_then(|xml| vm_resources(&xml)),
            )
        }))
        .buffer_unordered(6)
        .collect::<Vec<_>>()
        .await;
        for (index, size) in sizes {
            match size {
                Ok((bytes, cpu, memory)) => {
                    let object = &mut result.objects[index];
                    object.bytes = bytes.or(object.bytes);
                    object.cpu = cpu.or(object.cpu);
                    object.memory_mb = memory.or(object.memory_mb);
                }
                Err(_) => result.warnings.push(format!(
                    "{}: показан выделенный объём API, размер дисков уточнить не удалось",
                    result.objects[index].name
                )),
            }
        }
        result.objects.sort_by(|a, b| {
            let group = |o: &CloudObject| {
                if o.kind == ObjectKind::Vm {
                    o.parent_name.clone()
                } else {
                    o.name.clone()
                }
            };
            (&a.vdc, group(a), a.kind == ObjectKind::Vm, &a.name).cmp(&(
                &b.vdc,
                group(b),
                b.kind == ObjectKind::Vm,
                &b.name,
            ))
        });
        Ok(result)
    }

    pub async fn catalogs(&self) -> Result<Vec<CloudObject>> {
        Ok(self
            .query("catalog")
            .await?
            .into_iter()
            .filter(|o| !o.owner.trim().is_empty())
            .collect())
    }

    pub async fn inspect_export(&self, object: &CloudObject) -> Result<ExportInspection> {
        let href = if object.kind == ObjectKind::Vm {
            &object.parent_href
        } else {
            &object.href
        };
        ensure!(!href.is_empty(), "Не удалось определить родительский vApp");
        let xml = self.get(href).await?;
        let mut inspection = inspect_xml(object, &xml)?;
        // Read each VM directly: embedded Children may omit VmSpecSection.
        for vm in &mut inspection.vms {
            let detail = self
                .get(&vm.href)
                .await
                .context("Не удалось проверить носители ВМ")?;
            vm.media = mounted_media(&detail, &vm.href, &vm.name)?;
            vm.status = Document::parse(&detail)?
                .root_element()
                .attribute("status")
                .unwrap_or("unknown")
                .into();
        }
        Ok(inspection)
    }

    pub async fn power_off_vm(&self, vm_href: &str) -> Result<()> {
        let xml = self.get(vm_href).await?;
        let action = {
            let doc = Document::parse(&xml)?;
            let root = doc.root_element();
            ensure!(
                root.has_tag_name("Vm"),
                "Выключение разрешено только для конкретной ВМ"
            );
            if root.attribute("status") == Some("8") {
                return Ok(());
            }
            root.children()
                .find(|n| n.has_tag_name("Link") && n.attribute("rel") == Some("power:powerOff"))
                .and_then(|n| n.attribute("href"))
                .context("Нет права или доступной операции выключения ВМ")?
                .to_owned()
        };
        let xml = self
            .post_xml(
                &action,
                "application/vnd.vmware.vcloud.task+xml",
                String::new(),
            )
            .await?;
        let task = Document::parse(&xml)?
            .root_element()
            .attribute("href")
            .map(str::to_owned);
        if let Some(task) = task {
            self.wait_task(&task).await?;
        }
        Ok(())
    }

    async fn wait_task(&self, href: &str) -> Result<()> {
        for _ in 0..120 {
            let xml = self.get(href).await?;
            let status = Document::parse(&xml)?
                .root_element()
                .attribute("status")
                .unwrap_or("")
                .to_owned();
            match status.as_str() {
                "success" => return Ok(()),
                "error" | "aborted" | "canceled" | "cancelled" => {
                    bail!(
                        "Задача Cloud Director завершилась: {status}. {}",
                        api_error(&xml)
                    )
                }
                _ => {}
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        bail!("Истекло время ожидания задачи. Проверьте её состояние в Cloud Director.")
    }

    pub async fn create_catalog(
        &self,
        org_href: &str,
        name: &str,
        description: &str,
    ) -> Result<()> {
        ensure!(!name.trim().is_empty(), "Введите имя каталога");
        let org = self.checked_url(org_href)?;
        let admin_path = org.path().replacen("/api/org/", "/api/admin/org/", 1);
        let xml = self
            .get(&admin_path)
            .await
            .context("Нет доступа к административным настройкам организации")?;
        let link = {
            let doc = Document::parse(&xml)?;
            doc.descendants()
                .find(|n| {
                    n.has_tag_name("Link")
                        && n.attribute("rel") == Some("add")
                        && n.attribute("type")
                            .is_some_and(|t| t.contains("admin.catalog"))
                })
                .and_then(|n| n.attribute("href"))
                .context("Создание каталога недоступно для этой учётной записи")?
                .to_owned()
        };
        let created = self.post_xml(&link, "application/vnd.vmware.admin.catalog+xml", format!("<AdminCatalog xmlns=\"http://www.vmware.com/vcloud/v1.5\" name=\"{}\"><Description>{}</Description><IsPublished>false</IsPublished></AdminCatalog>", xml_escape(name), xml_escape(description))).await?;
        let tasks = Document::parse(&created)?
            .descendants()
            .filter(|n| n.has_tag_name("Task") && !matches!(n.attribute("status"), Some("success")))
            .filter_map(|n| n.attribute("href").map(str::to_owned))
            .collect::<Vec<_>>();
        for task in tasks {
            self.wait_task(&task).await?;
        }
        Ok(())
    }
}

pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub fn parse_records(xml: &str) -> Result<(Vec<CloudObject>, Option<String>)> {
    let doc = Document::parse(xml)?;
    let mut records = Vec::new();
    for node in doc.root_element().children().filter(|n| n.is_element()) {
        let kind = match node.tag_name().name() {
            "VAppRecord" => ObjectKind::Vapp,
            "VMRecord" => ObjectKind::Vm,
            "VAppTemplateRecord" => ObjectKind::Template,
            "MediaRecord" => ObjectKind::Media,
            "CatalogRecord" => ObjectKind::Catalog,
            "OrgVdcRecord" | "VdcRecord" => ObjectKind::Vdc,
            _ => continue,
        };
        if kind == ObjectKind::Vm && node.attribute("isVAppTemplate") == Some("true") {
            continue;
        }
        let a = |key| node.attribute(key).unwrap_or("").to_owned();
        let num = |key| node.attribute(key).and_then(|s| s.parse::<u64>().ok());
        let bytes = num("storageKB")
            .and_then(|n| n.checked_mul(1024))
            .or_else(|| num("storageB"))
            .or_else(|| num("totalStorageAllocatedMb").and_then(|n| n.checked_mul(1024 * 1024)));
        records.push(CloudObject {
            href: a("href"),
            name: a("name"),
            kind,
            vdc: a("vdcName"),
            catalog: a("catalogName"),
            parent_href: a("container"),
            parent_name: a("containerName"),
            status: a("status"),
            bytes,
            cpu: num("numberOfCpus").or_else(|| num("cpuCount")),
            memory_mb: num("memoryMB"),
            owner: node
                .attribute("ownerName")
                .filter(|s| !s.trim().is_empty())
                .or_else(|| node.attribute("owner"))
                .unwrap_or("")
                .into(),
        });
    }
    let next = doc
        .descendants()
        .find(|n| n.has_tag_name("Link") && n.attribute("rel") == Some("nextPage"))
        .and_then(|n| n.attribute("href"))
        .map(str::to_owned);
    Ok((records, next))
}

pub fn inspect_xml(object: &CloudObject, xml: &str) -> Result<ExportInspection> {
    let doc = Document::parse(xml)?;
    let mut object = object.clone();
    let vms: Vec<_> = doc
        .descendants()
        .filter(|n| {
            matches!(object.kind, ObjectKind::Vm | ObjectKind::Vapp) && n.has_tag_name("Vm")
        })
        .map(|n| VmState {
            href: n.attribute("href").unwrap_or("").into(),
            name: n.attribute("name").unwrap_or("").into(),
            status: n.attribute("status").unwrap_or("unknown").into(),
            media: Vec::new(),
        })
        .collect();
    if object.kind == ObjectKind::Vm {
        ensure!(
            vms.len() == 1 && vms[0].href == object.href,
            "В vApp несколько ВМ. OVF Tool экспортирует vApp целиком. Выберите родительский vApp явно либо предварительно переместите нужную ВМ в отдельный vApp. Соседние ВМ не изменены."
        );
        object.kind = ObjectKind::Vapp;
        object.href = object.parent_href.clone();
        object.name = object.parent_name.clone();
    }
    if object.kind == ObjectKind::Vapp {
        ensure!(
            !vms.is_empty(),
            "Не удалось прочитать состав vApp. Экспорт остановлен."
        );
    }
    // Provisioned capacity is deliberately conservative, unlike allocated storageKB.
    let capacity = disk_capacity(xml)?;
    let media_size = if object.kind == ObjectKind::Media {
        doc.root_element()
            .attribute("size")
            .and_then(|s| s.parse::<u64>().ok())
    } else {
        None
    };
    let estimated_bytes = capacity.or(media_size).or(object.bytes);
    Ok(ExportInspection {
        object,
        vms,
        estimated_bytes,
    })
}

pub(crate) fn mounted_media(xml: &str, vm_href: &str, vm_name: &str) -> Result<Vec<MountedMedia>> {
    let doc = Document::parse(xml)?;
    ensure!(
        doc.root_element().has_tag_name("Vm"),
        "API вернул не ВМ при проверке ISO"
    );
    ensure!(
        doc.descendants()
            .any(|n| n.has_tag_name("VmSpecSection") || n.has_tag_name("VirtualHardwareSection")),
        "API не вернул конфигурацию носителей ВМ «{vm_name}»; отсутствие ISO не подтверждено"
    );
    let mut media = Vec::new();
    for n in doc.descendants().filter(|n| n.has_tag_name("MediaImage")) {
        let href = n.attribute("href").unwrap_or("");
        let name = n.attribute("name").unwrap_or("Подключённый ISO / Media");
        if href.is_empty()
            && n.attributes()
                .any(|a| a.name() == "nil" && a.value() == "true")
        {
            continue;
        }
        media.push(MountedMedia {
            vm_href: vm_href.into(),
            vm_name: vm_name.into(),
            href: href.into(),
            name: name.into(),
        });
    }
    // Legacy hardware sections may expose a mounted CD without MediaImage.
    // An unknown reference is visible but cannot be ejected automatically.
    for item in doc.descendants().filter(|n| n.has_tag_name("Item")) {
        let kind = item
            .children()
            .find(|n| n.has_tag_name("ResourceType"))
            .and_then(|n| n.text());
        if !matches!(kind, Some("15" | "16")) {
            continue;
        }
        let id = item
            .children()
            .find(|n| n.has_tag_name("InstanceID"))
            .and_then(|n| n.text());
        let covered = id.is_some_and(|id| {
            doc.descendants()
                .filter(|n| n.has_tag_name("MediaSettings"))
                .any(|settings| {
                    settings
                        .children()
                        .any(|n| n.has_tag_name("DeviceId") && n.text() == Some(id))
                })
        });
        if covered {
            continue;
        }
        for host in item.children().filter(|n| n.has_tag_name("HostResource")) {
            let backing = host.text().unwrap_or("").trim();
            if backing.is_empty() || media.iter().any(|m| m.href == backing || m.name == backing) {
                continue;
            }
            let href = if backing.starts_with("https://") && backing.contains("/api/media/") {
                backing
            } else {
                ""
            };
            media.push(MountedMedia {
                vm_href: vm_href.into(),
                vm_name: vm_name.into(),
                href: href.into(),
                name: backing.into(),
            });
        }
    }
    Ok(media)
}

fn vm_resources(xml: &str) -> Result<(Option<u64>, Option<u64>, Option<u64>)> {
    let doc = Document::parse(xml)?;
    let cpu = doc
        .descendants()
        .find(|n| n.has_tag_name("NumCpus"))
        .and_then(|n| n.text()?.parse().ok());
    let memory = doc
        .descendants()
        .find(|n| n.has_tag_name("MemoryResourceMb"))
        .and_then(|n| n.children().find(|n| n.has_tag_name("Configured")))
        .and_then(|n| n.text()?.parse().ok());
    Ok((disk_capacity(xml)?, cpu, memory))
}

fn disk_capacity(xml: &str) -> Result<Option<u64>> {
    let doc = Document::parse(xml)?;
    let capacity_mb: u64 = doc
        .descendants()
        .filter(|n| n.has_tag_name("DiskSettings"))
        .filter_map(|n| n.children().find(|c| c.has_tag_name("SizeMb")))
        .filter_map(|n| n.text()?.parse::<u64>().ok())
        .try_fold(0u64, |sum, n| sum.checked_add(n))
        .context("Слишком большой объём дисков")?;
    Ok(capacity_mb.checked_mul(1024 * 1024).filter(|n| *n > 0))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reads_resources_and_multiple_media_without_empty_drive_false_positive() {
        let xml = r#"<Vm><VmSpecSection><NumCpus>4</NumCpus><MemoryResourceMb><Configured>8192</Configured></MemoryResourceMb><DiskSection><DiskSettings><SizeMb>10240</SizeMb></DiskSettings></DiskSection><MediaSection><MediaSettings><DeviceId>1</DeviceId><MediaImage href="https://c/api/media/a" name="a.iso"/></MediaSettings><MediaSettings><DeviceId>2</DeviceId><MediaImage href="https://c/api/media/b" name="b.iso"/></MediaSettings><MediaSettings><DeviceId>3</DeviceId></MediaSettings></MediaSection></VmSpecSection><VirtualHardwareSection><Item><ResourceType>15</ResourceType><InstanceID>3</InstanceID><HostResource/></Item></VirtualHardwareSection></Vm>"#;
        let media = mounted_media(xml, "vm", "name").unwrap();
        assert_eq!(media.len(), 2);
        assert_eq!(
            vm_resources(xml).unwrap(),
            (Some(10 * 1024 * 1024 * 1024), Some(4), Some(8192))
        );
        assert!(mounted_media("<Vm/>", "vm", "name").is_err());
    }
    #[test]
    fn unknown_legacy_iso_is_not_silently_ignored() {
        let media = mounted_media(r#"<Vm><VirtualHardwareSection><Item><ResourceType>15</ResourceType><HostResource>[datastore] boot.iso</HostResource></Item></VirtualHardwareSection></Vm>"#,"vm","name").unwrap();
        assert_eq!(media.len(), 1);
        assert!(media[0].href.is_empty());
    }
    #[test]
    fn negotiates_known_version() {
        assert_eq!(choose_version("<SupportedVersions><VersionInfo><Version>40.0</Version></VersionInfo><VersionInfo><Version>39.0</Version></VersionInfo></SupportedVersions>").unwrap(), "39.0");
    }
    #[test]
    fn rejects_http_and_url_credentials() {
        assert!(endpoint_url("http://cloud.test").is_err());
        assert!(endpoint_url("https://u:p@cloud.test").is_err());
    }
    #[test]
    fn names_and_pagination_are_decoded() {
        let (items,next)=parse_records("<QueryResultRecords><VMRecord name=\"A &amp; B\" href=\"https://c/api/vApp/vm-1\" containerName=\"prod\" container=\"https://c/api/vApp/1\"/><Link rel=\"nextPage\" href=\"https://c/api/query?page=2&amp;pageSize=2\"/></QueryResultRecords>").unwrap();
        assert_eq!(items[0].name, "A & B");
        assert_eq!(next.unwrap(), "https://c/api/query?page=2&pageSize=2");
    }
    #[test]
    fn single_vm_export_cannot_touch_siblings() {
        let (items, _) =
            parse_records("<R><VMRecord name=\"a\" href=\"vm-a\" container=\"vapp\"/></R>")
                .unwrap();
        assert!(inspect_xml(&items[0],"<VApp><Children><Vm href=\"vm-a\" status=\"4\"/><Vm href=\"vm-b\" status=\"4\"/></Children></VApp>").is_err());
    }
    #[test]
    fn vcd_106_vm_storage_and_catalog_owners() {
        let (items,_)=parse_records(r#"<QueryResultRecords><VMRecord name="vm" totalStorageAllocatedMb="10240"/><CatalogRecord name="private" ownerName="owner" owner="urn:vcloud:user:1"/><CatalogRecord name="public" isPublished="true"/><CatalogRecord name="id-only" owner="urn:vcloud:user:2"/></QueryResultRecords>"#).unwrap();
        assert_eq!(items[0].bytes, Some(10 * 1024 * 1024 * 1024));
        let owned = items
            .iter()
            .filter(|o| o.kind == ObjectKind::Catalog && !o.owner.is_empty())
            .map(|o| o.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(owned, vec!["private", "id-only"]);
    }
    #[test]
    fn error_message_includes_server_reason_without_credentials() {
        assert_eq!(
            api_error(
                r#"<Error message="Access denied" minorErrorCode="ACCESS_TO_RESOURCE_IS_FORBIDDEN"/>"#
            ),
            "Access denied"
        );
    }
}
