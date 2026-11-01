//! Explicit, bounded cloud mutations initiated by a reviewed transfer request.
use crate::{
    api::{CloudSession, xml_escape},
    model::*,
    transfer_state::{CloudTrace, resource_ready},
};
use anyhow::{Context, Result, bail, ensure};
use roxmltree::Document;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub fn ensure_reviewed(
    inspection: &ExportInspection,
    expected: &[String],
    allowed: &[MountedMedia],
) -> Result<()> {
    ensure!(
        inspection.vms.len() == expected.len()
            && inspection.vms.iter().all(|v| expected.contains(&v.href)),
        "Состав vApp изменился. Откройте скачивание заново и проверьте список ВМ."
    );
    ensure!(
        inspection.vms.iter().all(VmState::is_off),
        "Есть включённые ВМ. Выключение требуется подтвердить заново."
    );
    for media in inspection.vms.iter().flat_map(|v| &v.media) {
        ensure!(
            allowed.contains(media),
            "У ВМ «{}» подключён носитель «{}». Подтвердите извлечение ISO в окне скачивания.",
            media.vm_name,
            media.name
        );
        ensure!(
            !media.href.is_empty(),
            "API не сообщил адрес носителя «{}». Извлеките его в Cloud Director и повторите проверку.",
            media.name
        );
    }
    Ok(())
}

pub fn capture_body(source: &CloudObject, name: &str) -> Result<String> {
    ensure!(
        source.kind == ObjectKind::Vapp,
        "Захват поддерживает только явно выбранный vApp"
    );
    ensure!(!name.trim().is_empty(), "Введите имя шаблона");
    Ok(format!(
        r#"<CaptureVAppParams xmlns="http://www.vmware.com/vcloud/v1.5" name="{}"><Description>Создан OVF UI для скачивания. Сохраняется в каталоге.</Description><Source href="{}" type="application/vnd.vmware.vcloud.vApp+xml"/></CaptureVAppParams>"#,
        xml_escape(name),
        xml_escape(&source.href)
    ))
}

fn capture_action_from_xml(xml: &str) -> Result<String> {
    let doc = Document::parse(xml)?;
    ensure!(
        doc.root_element().has_tag_name("Catalog"),
        "API вернул не каталог"
    );
    doc.root_element()
        .children()
        .find(|n| {
            n.has_tag_name("Link")
                && n.attribute("rel") == Some("add")
                && n.attribute("type")
                    == Some("application/vnd.vmware.vcloud.captureVAppParams+xml")
        })
        .and_then(|n| n.attribute("href"))
        .map(str::to_owned)
        .context("Нет права захвата vApp в выбранный каталог")
}

pub fn eject_body(media: &MountedMedia) -> String {
    format!(
        r#"<MediaInsertOrEjectParams xmlns="http://www.vmware.com/vcloud/v1.5"><Media href="{}" type="application/vnd.vmware.vcloud.media+xml"/></MediaInsertOrEjectParams>"#,
        xml_escape(&media.href)
    )
}

impl CloudSession {
    async fn await_export_task(
        &self,
        initial: &str,
        cancel: &CancellationToken,
        progress: &impl Fn(&str, Option<u8>),
    ) -> Result<CloudTrace> {
        let mut trace = CloudTrace::default();
        trace.observe(initial, true)?;
        ensure!(
            !trace.tasks.is_empty(),
            "Cloud Director принял запрос, но не вернул задачу. Проверьте каталог перед повтором."
        );
        let work = async {
            loop {
                trace.check_errors()?;
                for line in trace.drain_events() {
                    progress(&line, None);
                }
                if !trace.pending() {
                    return Ok(trace);
                }
                let tasks = trace
                    .tasks
                    .iter()
                    .filter(|(_, t)| t.pending())
                    .map(|(h, _)| h.clone())
                    .collect::<Vec<_>>();
                for href in tasks {
                    let xml = self.get(&href).await?;
                    trace.observe(&xml, true)?;
                }
                let percent = trace.progress();
                progress("", percent);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        };
        tokio::select! {
            _=cancel.cancelled()=>bail!("Ожидание отменено. Уже отправленная операция в Cloud Director может продолжаться. Проверьте её перед повтором."),
            result=tokio::time::timeout(Duration::from_secs(7200),work)=>result.context("Cloud Director не завершил задачу за 2 часа")?,
        }
    }

    pub async fn eject_reviewed_media(
        &self,
        media: &MountedMedia,
        cancel: &CancellationToken,
        progress: &impl Fn(&str, Option<u8>),
    ) -> Result<()> {
        ensure!(!cancel.is_cancelled(), "Передача отменена");
        self.checked_url(&media.href)?;
        let xml = self.get(&media.vm_href).await?;
        let current = crate::api::mounted_media(&xml, &media.vm_href, &media.vm_name)?;
        ensure!(
            current.contains(media),
            "Носитель ВМ изменился. Повторите проверку перед извлечением."
        );
        let action = {
            let doc = Document::parse(&xml)?;
            ensure!(
                doc.root_element().has_tag_name("Vm")
                    && matches!(
                        doc.root_element().attribute("status"),
                        Some("8" | "POWERED_OFF")
                    ),
                "ВМ должна оставаться выключенной перед извлечением ISO"
            );
            doc.root_element()
                .children()
                .find(|n| n.has_tag_name("Link") && n.attribute("rel") == Some("media:ejectMedia"))
                .and_then(|n| n.attribute("href"))
                .context("API не разрешил извлечение носителя ВМ")?
                .to_owned()
        };
        ensure!(!cancel.is_cancelled(), "Передача отменена");
        let reply = self
            .post_xml(
                &action,
                "application/vnd.vmware.vcloud.mediaInsertOrEjectParams+xml",
                eject_body(media),
            )
            .await?;
        self.await_export_task(&reply, cancel, progress).await?;
        let xml = self.get(&media.vm_href).await?;
        ensure!(
            !crate::api::mounted_media(&xml, &media.vm_href, &media.vm_name)?.contains(media),
            "Cloud Director не подтвердил извлечение ISO"
        );
        Ok(())
    }

    pub(crate) async fn capture_action(&self, catalog_href: &str) -> Result<String> {
        let action = capture_action_from_xml(&self.get(catalog_href).await?)?;
        self.checked_url(&action)?;
        Ok(action)
    }

    pub async fn capture_for_download(
        &self,
        source: &CloudObject,
        target: &CaptureTarget,
        cancel: &CancellationToken,
        progress: &impl Fn(&str, Option<u8>),
    ) -> Result<CloudObject> {
        let preflight = self
            .catalog_preflight(
                &target.catalog_href,
                &target.name,
                &source.vdc,
                source.bytes.unwrap_or(0),
            )
            .await?;
        ensure!(
            preflight.catalog_name == target.catalog_name,
            "Выбранный каталог переименован. Откройте скачивание заново."
        );
        progress(&preflight.summary(), None);
        let existing = self.query("vAppTemplate").await?;
        ensure!(
            !existing.iter().any(|t| t.catalog == target.catalog_name
                && crate::catalog_preflight::normalized_name(&t.name)
                    == crate::catalog_preflight::normalized_name(&target.name)),
            "Шаблон с таким именем уже существует. Выберите другое имя; перезапись отключена."
        );
        let action = self.capture_action(&target.catalog_href).await?;
        ensure!(!cancel.is_cancelled(), "Передача отменена");
        let reply = self
            .post_xml(
                &action,
                "application/vnd.vmware.vcloud.captureVAppParams+xml",
                capture_body(source, &target.name)?,
            )
            .await?;
        let trace = self.await_export_task(&reply, cancel, progress).await?;
        ensure!(
            trace.targets.len() == 1,
            "API не вернул однозначный адрес созданного шаблона. Проверьте каталог перед повтором."
        );
        let resolve = async {
            for _ in 0..60 {
                let found = self
                    .query("vAppTemplate")
                    .await?
                    .into_iter()
                    .filter(|t| t.name == target.name && t.catalog == target.catalog_name)
                    .collect::<Vec<_>>();
                ensure!(
                    found.len() <= 1,
                    "API вернул несколько шаблонов с одинаковым именем; скачивание остановлено"
                );
                if let Some(template) = found.into_iter().next() {
                    ensure!(
                        trace.targets.contains_key(&template.href),
                        "Адрес шаблона не совпал с результатом захвата"
                    );
                    let xml = self.get(&template.href).await?;
                    if resource_ready(&xml)? {
                        return Ok(template);
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            bail!(
                "Захват завершён, но готовый шаблон ещё не появился в каталоге. Обновите каталог; повторный захват не выполнялся."
            )
        };
        tokio::select! { _=cancel.cancelled()=>bail!("Скачивание отменено; созданный шаблон остаётся в каталоге"), result=resolve=>result }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn capture_permission_requires_the_exact_action_not_generic_catalog_write() {
        assert!(capture_action_from_xml(r#"<Catalog><Link rel="add" type="application/vnd.vmware.vcloud.media+xml" href="/api/catalog/1/media"/></Catalog>"#).is_err());
        assert_eq!(capture_action_from_xml(r#"<Catalog><Link rel="add" type="application/vnd.vmware.vcloud.captureVAppParams+xml" href="/api/catalog/1/action/captureVApp"/></Catalog>"#).unwrap(), "/api/catalog/1/action/captureVApp");
    }
    #[test]
    fn capture_never_overwrites_or_changes_source() {
        let (objects, _) = crate::api::parse_records(
            r#"<R><VAppRecord name="a" href="https://c/api/vApp/a"/></R>"#,
        )
        .unwrap();
        let body = capture_body(&objects[0], "a & b").unwrap();
        assert!(body.contains("a &amp; b"));
        assert!(!body.contains("TargetCatalogItem"));
        assert!(!body.contains("Delete"));
    }
    #[test]
    fn changed_vm_set_or_new_iso_needs_new_review() {
        let (objects, _) =
            crate::api::parse_records(r#"<R><VAppRecord name="a" href="vapp"/></R>"#).unwrap();
        let mut i = crate::api::inspect_xml(
            &objects[0],
            r#"<VApp><Children><Vm href="vm1" status="8"/></Children></VApp>"#,
        )
        .unwrap();
        assert!(ensure_reviewed(&i, &["vm1".into()], &[]).is_ok());
        assert!(ensure_reviewed(&i, &["vm2".into()], &[]).is_err());
        let media = MountedMedia {
            vm_href: "vm1".into(),
            vm_name: "vm".into(),
            href: "media".into(),
            name: "a.iso".into(),
        };
        i.vms[0].media.push(media.clone());
        assert!(ensure_reviewed(&i, &["vm1".into()], &[]).is_err());
        assert!(ensure_reviewed(&i, &["vm1".into()], &[media]).is_ok());
    }

    #[tokio::test]
    async fn tls_capture_and_eject_wait_for_tasks_and_reject_duplicate_capture() {
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};
        use hyper::{Request, Response, body::Incoming, service::service_fn};
        use hyper_util::rt::TokioIo;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio_rustls::{TlsAcceptor, rustls};
        let _ = rustls::crypto::ring::default_provider().install_default();
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let root = reqwest::Certificate::from_der(cert.cert.der()).unwrap();
        let tls = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.cert.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
            )
            .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let base = format!("https://127.0.0.1:{port}");
        let posts = Arc::new(AtomicUsize::new(0));
        let task_reads = Arc::new(AtomicUsize::new(0));
        let ejected = Arc::new(AtomicUsize::new(0));
        let server_posts = posts.clone();
        let reads = task_reads.clone();
        let media_state = ejected.clone();
        let acceptor = TlsAcceptor::from(Arc::new(tls));
        let server = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let acceptor = acceptor.clone();
                let posts = server_posts.clone();
                let reads = reads.clone();
                let ejected = media_state.clone();
                tokio::spawn(async move {
                    let tls = acceptor.accept(stream).await.unwrap();
                    let service = service_fn(move |req: Request<Incoming>| {
                        let posts = posts.clone();
                        let reads = reads.clone();
                        let ejected = ejected.clone();
                        async move {
                            let path = req.uri().path().to_owned();
                            let query = req.uri().query().unwrap_or("").to_owned();
                            let method = req.method().clone();
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let b = format!("https://127.0.0.1:{port}");
                            let xml=match path.as_str() {
                                "/api/versions"=>"<SupportedVersions><VersionInfo><Version>39.0</Version></VersionInfo></SupportedVersions>".into(),
                                "/api/session"=>"<Session/>".into(),
                                "/api/query" if query.contains("type=catalog")=>format!(r#"<R><CatalogRecord name="mine" href="{b}/api/catalog/1" ownerName="me"/></R>"#),
                                "/api/query" if query.contains("type=vAppTemplate")=>if posts.load(Ordering::SeqCst)>0 {format!(r#"<R><VAppTemplateRecord name="export" catalogName="mine" href="{b}/api/vAppTemplate/1"/></R>"#)} else {"<R/>".into()},
                                "/api/catalog/1"=>format!(r#"<Catalog><Link rel="add" type="application/vnd.vmware.vcloud.captureVAppParams+xml" href="{b}/api/catalog/1/action/captureVApp"/></Catalog>"#),
                                "/api/admin/catalog/1"=>format!(r#"<AdminCatalog><CatalogStorageProfiles><VdcStorageProfile name="fast" href="{b}/api/vdcStorageProfile/1"/></CatalogStorageProfiles></AdminCatalog>"#),
                                "/api/vdcStorageProfile/1"=>"<VdcStorageProfile name=\"fast\"><Enabled>true</Enabled><Units>MB</Units><Limit>10000</Limit><StorageUsedMB>100</StorageUsedMB></VdcStorageProfile>".into(),
                                "/api/catalog/1/action/captureVApp"=>{
                                    assert_eq!(method,hyper::Method::POST);posts.fetch_add(1,Ordering::SeqCst);
                                    let body=std::str::from_utf8(&body).unwrap();assert!(body.contains("Source href="));assert!(!body.contains("TargetCatalogItem"));
                                    format!(r#"<Task href="{b}/api/task/capture" status="running"><Owner type="application/vnd.vmware.vcloud.vAppTemplate+xml" href="{b}/api/vAppTemplate/1"/><Progress>5</Progress></Task>"#)
                                },
                                "/api/task/capture"=>{let status=if reads.fetch_add(1,Ordering::SeqCst)==0 {"running"}else{"success"};format!(r#"<Task href="{b}/api/task/capture" status="{status}"/>"#)},
                                "/api/vAppTemplate/1"=>format!(r#"<VAppTemplate href="{b}/api/vAppTemplate/1" status="8" ovfDescriptorUploaded="true"/>"#),
                                "/api/vApp/vm-1"=>{let media=if ejected.load(Ordering::SeqCst)==0 {format!(r#"<MediaImage href="{b}/api/media/1" name="boot.iso"/>"#)}else{String::new()};format!(r#"<Vm status="8"><Link rel="media:ejectMedia" href="{b}/api/vApp/vm-1/media/action/ejectMedia"/><VmSpecSection><MediaSection><MediaSettings>{media}</MediaSettings></MediaSection></VmSpecSection></Vm>"#)},
                                "/api/vApp/vm-1/media/action/ejectMedia"=>{assert_eq!(method,hyper::Method::POST);assert!(std::str::from_utf8(&body).unwrap().contains("/api/media/1"));format!(r#"<Task href="{b}/api/task/eject" status="running"/>"#)},
                                "/api/task/eject"=>{ejected.store(1,Ordering::SeqCst);format!(r#"<Task href="{b}/api/task/eject" status="success"/>"#)},
                                _=>panic!("Unexpected test request {path}"),
                            };
                            Ok::<_, std::convert::Infallible>(
                                Response::builder()
                                    .header("Content-Type", "application/xml")
                                    .body(Full::new(Bytes::from(xml)))
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
        let tenant = Tenant {
            id: uuid::Uuid::new_v4(),
            organization: "test".into(),
            username: String::new(),
            auth: AuthKind::BearerToken,
        };
        let endpoint = Endpoint {
            id: uuid::Uuid::new_v4(),
            name: "mock".into(),
            url: base.clone(),
            tenants: vec![],
        };
        let client = reqwest::Client::builder()
            .add_root_certificate(root)
            .build()
            .unwrap();
        let session = CloudSession::login_with_client(
            &endpoint,
            &tenant,
            Secret {
                value: "test-token".into(),
                session_token: String::new(),
            },
            client,
        )
        .await
        .unwrap();
        let (objects, _) = crate::api::parse_records(&format!(
            r#"<R><VAppRecord name="source" href="{base}/api/vApp/source"/></R>"#
        ))
        .unwrap();
        let target = CaptureTarget {
            catalog_href: format!("{base}/api/catalog/1"),
            catalog_name: "mine".into(),
            name: "export".into(),
        };
        let cancel = CancellationToken::new();
        let created = session
            .capture_for_download(&objects[0], &target, &cancel, &|_, _| {})
            .await
            .unwrap();
        assert_eq!(created.href, format!("{base}/api/vAppTemplate/1"));
        assert!(task_reads.load(Ordering::SeqCst) >= 2);
        assert!(
            session
                .capture_for_download(&objects[0], &target, &cancel, &|_, _| {})
                .await
                .unwrap_err()
                .to_string()
                .contains("уже существует")
        );
        assert_eq!(posts.load(Ordering::SeqCst), 1);
        let media = MountedMedia {
            vm_href: format!("{base}/api/vApp/vm-1"),
            vm_name: "vm".into(),
            href: format!("{base}/api/media/1"),
            name: "boot.iso".into(),
        };
        session
            .eject_reviewed_media(&media, &cancel, &|_, _| {})
            .await
            .unwrap();
        assert_eq!(ejected.load(Ordering::SeqCst), 1);
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        assert!(
            session
                .eject_reviewed_media(&media, &cancelled, &|_, _| {})
                .await
                .is_err()
        );
        assert!(
            session
                .await_export_task(
                    r#"<Task href="x" status="error"><Error message="Quota exceeded"/></Task>"#,
                    &cancel,
                    &|_, _| {}
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("Quota exceeded")
        );
        server.abort();
    }
}
