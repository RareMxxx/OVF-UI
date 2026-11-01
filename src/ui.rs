use crate::{
    api::CloudSession,
    jobs::{JobManager, JobSnapshot, JobStatus},
    model::*,
    ovf,
    store::Store,
};
use eframe::egui::{self, Color32, RichText, Vec2};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::mpsc::{self, Receiver, Sender},
    time::{Duration, Instant},
};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const RED: Color32 = Color32::from_rgb(211, 32, 54);
const CONTROL_RADIUS: u8 = 18;
const SURFACE_RADIUS: u8 = 12;
const CONTROL_HEIGHT: f32 = 36.0;
fn error_color(ui: &egui::Ui) -> Color32 {
    if ui.visuals().dark_mode {
        Color32::from_rgb(255, 112, 127)
    } else {
        RED
    }
}
fn muted(ui: &egui::Ui) -> Color32 {
    if ui.visuals().dark_mode {
        Color32::from_rgb(163, 170, 185)
    } else {
        Color32::from_rgb(77, 87, 105)
    }
}
fn success_color(ui: &egui::Ui) -> Color32 {
    if ui.visuals().dark_mode {
        Color32::from_rgb(76, 185, 133)
    } else {
        Color32::from_rgb(20, 112, 69)
    }
}
fn warning_color(ui: &egui::Ui) -> Color32 {
    if ui.visuals().dark_mode {
        Color32::from_rgb(224, 172, 79)
    } else {
        Color32::from_rgb(128, 83, 13)
    }
}

type Outcome<T> = Result<T, String>;
enum Event {
    AppReleaseStarted(chrono::NaiveDate),
    AppRelease(Outcome<crate::app_updates::AppRelease>),
    DestinationCheck(Uuid, String, Outcome<DestinationPreview>),
    TransferInventory(Uuid, Outcome<Inventory>),
    UpdateStarted(chrono::NaiveDate),
    UpdateCheck(Outcome<crate::updates::Version>),
    Tool(PathBuf, Outcome<String>),
    Inventory(Uuid, Outcome<(CloudSession, Inventory)>),
    Inspection(Uuid, Outcome<ExportInspection>),
    Credential(Outcome<(Endpoint, Tenant)>),
    S3Saved(Outcome<S3Profile>),
    Catalog(Uuid, String, Outcome<()>),
    Catalogs(Uuid, Option<String>, Outcome<Vec<CloudObject>>),
    Validation(PathBuf, Outcome<ovf::ValidationReport>),
    Backup(Outcome<Option<Settings>>),
    Picked(Pick, Option<PathBuf>),
}
#[derive(Clone, Copy)]
enum Pick {
    Tool,
    Download,
    Upload,
    Validate,
    Export,
    Import,
}
#[derive(Clone, Copy, PartialEq)]
enum Page {
    Machines,
    Catalogs,
    Validate,
    Settings,
}

#[derive(Default)]
struct ConnectionForm {
    open: bool,
    endpoint_id: Option<Uuid>,
    tenant_id: Option<Uuid>,
    name: String,
    url: String,
    organization: String,
    username: String,
    auth: AuthKind,
    password: String,
}
#[derive(Default)]
struct S3Form {
    open: bool,
    id: Option<Uuid>,
    name: String,
    endpoint: String,
    region: String,
    bucket: String,
    prefix: String,
    access_key: String,
    secret_key: String,
    session_token: String,
}
#[derive(Default)]
struct CatalogForm {
    open: bool,
    name: String,
    description: String,
}
#[derive(Default)]
struct BackupForm {
    open: bool,
    import: bool,
    path: PathBuf,
    password: String,
    repeat: String,
}

struct TransferForm {
    id: Uuid,
    endpoint: Endpoint,
    tenant: Tenant,
    inspection: Option<ExportInspection>,
    direction: Direction,
    object: CloudObject,
    path: PathBuf,
    format: String,
    s3: Option<Uuid>,
    limit: u64,
    estimate_gib: f64,
    free: Option<u64>,
    power_confirm: bool,
    eulas: bool,
    error: String,
    via_template: bool,
    capture_catalog: String,
    capture_name: String,
    eject_confirm: bool,
    parallel_threads: u8,
    destination_key: String,
    destination_busy: bool,
    destination_cancel: Option<CancellationToken>,
    destination_preview: Option<DestinationPreview>,
    destination_error: String,
    allow_unknown_capacity: bool,
}

struct DestinationPreview {
    summary: String,
    capacity_verified: bool,
}

impl Drop for TransferForm {
    fn drop(&mut self) {
        if let Some(cancel) = &self.destination_cancel {
            cancel.cancel();
        }
    }
}

impl TransferForm {
    fn destination_fingerprint(&self) -> String {
        let metadata = std::fs::metadata(&self.path).ok();
        format!(
            "{:?}",
            (
                self.tenant.id,
                self.via_template,
                &self.capture_catalog,
                &self.capture_name,
                &self.object.catalog,
                &self.object.name,
                &self.object.vdc,
                &self.path,
                metadata.as_ref().map(|m| m.len()),
                metadata.and_then(|m| m.modified().ok()),
                self.estimate_gib.to_bits(),
            )
        )
    }
    fn reset_destination(&mut self) {
        if let Some(cancel) = self.destination_cancel.take() {
            cancel.cancel();
        }
        self.id = Uuid::new_v4();
        self.destination_key = self.destination_fingerprint();
        self.destination_busy = false;
        self.destination_preview = None;
        self.destination_error.clear();
        self.allow_unknown_capacity = false;
    }
}

pub struct DockApp {
    settings: Settings,
    store: Store,
    runtime: Handle,
    sender: Sender<Event>,
    receiver: Receiver<Event>,
    jobs: JobManager,
    transfer_statuses: HashMap<Uuid, JobStatus>,
    transfer_tenants: HashMap<Uuid, (Uuid, bool)>,
    refresh_after_transfer: HashSet<Uuid>,
    deferred_transfer_inventory: HashMap<Uuid, Inventory>,
    transfer_watch_at: Instant,
    sessions: HashMap<Uuid, CloudSession>,
    inventories: HashMap<Uuid, Inventory>,
    selected: Option<(Uuid, Uuid)>,
    page: Page,
    filter: String,
    catalog_filter: String,
    tool_version: String,
    latest_version: Option<crate::updates::Version>,
    update_check_day: Option<chrono::NaiveDate>,
    update_check_busy: bool,
    update_check_error: String,
    app_release: Option<crate::app_updates::AppRelease>,
    app_check_busy: bool,
    app_check_error: String,
    app_check_day: Option<chrono::NaiveDate>,
    setup: bool,
    busy: usize,
    notice: String,
    error: bool,
    connection: ConnectionForm,
    s3_form: S3Form,
    catalog_form: CatalogForm,
    backup: BackupForm,
    transfer: Option<TransferForm>,
    queue_choice: Option<TransferRequest>,
    show_transfers: bool,
    validate_path: PathBuf,
    validation: Option<ovf::ValidationReport>,
    validation_cancel: Option<CancellationToken>,
    show_logs: bool,
    log_text: String,
    close_confirm: bool,
    closing: bool,
    export_error: Option<String>,
}

impl DockApp {
    #[cfg(feature = "test-fixtures")]
    fn load_visual_fixture(&mut self, ctx: &egui::Context) {
        self.setup = false;
        self.settings.light_theme = std::env::args().any(|s| s == "--light");
        apply_theme(ctx, self.settings.light_theme);
        let tenant = Tenant {
            id: Uuid::from_u128(2),
            organization: "demo-tenant".into(),
            username: String::new(),
            auth: AuthKind::Password,
        };
        let endpoint = Endpoint {
            id: Uuid::from_u128(1),
            name: "Демонстрационный ЦОД".into(),
            url: "https://offline.invalid".into(),
            tenants: vec![tenant.clone()],
        };
        self.settings.endpoints = vec![endpoint.clone()];
        self.selected = Some((endpoint.id, tenant.id));
        self.tool_version = "VMware ovftool 9.1.1 (build-25678992)".into();
        self.latest_version = crate::updates::Version::parse(&self.tool_version);
        self.notice = "Автономная проверка UI · все данные демонстрационные".into();
        let xml = r#"<R><CatalogRecord href="catalog1" name="Production" ownerName="admin@example.test"/><CatalogRecord href="catalog2" name="Образы Linux" ownerName="linux@example.test"/><CatalogRecord href="catalog3" name="Windows Server — инфраструктура и приложения" ownerName="infrastructure@example.test"/><CatalogRecord href="catalog4" name="Test" ownerName="qa@example.test"/><CatalogRecord href="catalog5" name="Сетевое оборудование" ownerName="network@example.test"/><CatalogRecord href="catalog6" name="Архив" ownerName="backup@example.test"/><VAppRecord name="Application cluster" href="vapp" vdcName="DC-01" status="8" ownerName="me"/><VMRecord name="Ubuntu Server" href="vm" container="vapp" containerName="Application cluster" vdcName="DC-01" numberOfCpus="4" memoryMB="8192" totalStorageAllocatedMb="102400" status="8" ownerName="me"/><VAppTemplateRecord name="Ubuntu 24.04 LTS" href="template" catalogName="Production" status="8" storageKB="5242880"/><MediaRecord name="installer.iso" href="media" catalogName="Образы Linux" storageKB="524288"/></R>"#;
        let (objects, _) = crate::api::parse_records(xml).unwrap();
        let source = objects
            .iter()
            .find(|o| o.kind == ObjectKind::Vapp)
            .unwrap()
            .clone();
        self.inventories.insert(
            tenant.id,
            Inventory {
                objects,
                api_version: "39.0".into(),
                ..Default::default()
            },
        );
        self.page = if std::env::args().any(|s| s == "--machines") {
            Page::Machines
        } else {
            Page::Catalogs
        };
        if std::env::args().any(|s| s == "--settings") {
            self.page = Page::Settings;
        }
        if std::env::args().any(|s| s == "--download") {
            let mut inspection=crate::api::inspect_xml(&source,r#"<VApp><Children><Vm href="vm" name="Ubuntu Server" status="8"/></Children><DiskSettings><SizeMb>102400</SizeMb></DiskSettings></VApp>"#).unwrap();
            inspection.vms[0].media.push(MountedMedia {
                vm_href: "vm".into(),
                vm_name: "Ubuntu Server".into(),
                href: "media".into(),
                name: "installer.iso".into(),
            });
            self.transfer = Some(TransferForm {
                id: Uuid::new_v4(),
                destination_key: String::new(),
                destination_busy: false,
                destination_cancel: None,
                destination_preview: None,
                destination_error: String::new(),
                allow_unknown_capacity: false,
                endpoint,
                tenant,
                inspection: Some(inspection),
                direction: Direction::Download,
                object: source,
                path: "D:\\".into(),
                format: "OVA".into(),
                s3: None,
                limit: 0,
                estimate_gib: 110.0,
                free: Some(500 * 1024 * 1024 * 1024),
                power_confirm: false,
                eulas: false,
                error: String::new(),
                via_template: true,
                capture_catalog: "catalog1".into(),
                capture_name: "export-Ubuntu".into(),
                eject_confirm: false,
                parallel_threads: 4,
            });
        }
        if std::env::args().any(|s| s == "--multi-vm") {
            self.export_error=Some("В vApp несколько ВМ. OVF Tool экспортирует vApp целиком. Выберите родительский vApp явно либо предварительно переместите нужную ВМ в отдельный vApp. Соседние ВМ не изменены.".into());
        }
    }
    pub fn new(cc: &eframe::CreationContext<'_>, runtime: Handle) -> Self {
        let store = Store::open().expect("Не удалось открыть папку данных OVF UI");
        let (settings, notice) = match store.load() {
            Ok(s) => (s, String::new()),
            Err(e) => (Settings::default(), e.to_string()),
        };
        let jobs = JobManager::new(store.clone(), runtime.clone());
        // Seed history so old completed jobs do not trigger network refreshes.
        let transfer_statuses = jobs
            .snapshots()
            .into_iter()
            .map(|j| (j.id, j.status))
            .collect();
        let (sender, receiver) = mpsc::channel();
        let setup = settings.ovf_directory.as_os_str().is_empty();
        let error = !notice.is_empty();
        let mut app = Self {
            settings,
            store,
            runtime,
            sender,
            receiver,
            jobs,
            transfer_statuses,
            transfer_tenants: HashMap::new(),
            refresh_after_transfer: HashSet::new(),
            deferred_transfer_inventory: HashMap::new(),
            transfer_watch_at: Instant::now(),
            sessions: HashMap::new(),
            inventories: HashMap::new(),
            selected: None,
            page: Page::Machines,
            filter: String::new(),
            catalog_filter: String::new(),
            tool_version: String::new(),
            latest_version: None,
            update_check_day: None,
            update_check_busy: false,
            update_check_error: String::new(),
            app_release: None,
            app_check_busy: false,
            app_check_error: String::new(),
            app_check_day: None,
            setup,
            busy: 0,
            notice,
            error,
            connection: Default::default(),
            s3_form: Default::default(),
            catalog_form: Default::default(),
            backup: Default::default(),
            transfer: None,
            queue_choice: None,
            show_transfers: false,
            validate_path: PathBuf::new(),
            validation: None,
            validation_cancel: None,
            show_logs: false,
            log_text: String::new(),
            close_confirm: false,
            closing: false,
            export_error: None,
        };
        apply_theme(&cc.egui_ctx, app.settings.light_theme);
        #[cfg(feature = "test-fixtures")]
        if std::env::args().any(|s| s == "--workspace-preview") {
            app.load_visual_fixture(&cc.egui_ctx);
            return app;
        }
        if !setup {
            app.probe(app.settings.ovf_directory.clone());
        }
        app.check_updates();
        app.check_application_updates();
        // The timer lives in Tokio, so midnight checks also run while minimized.
        let tx = app.sender.clone();
        let ctx = cc.egui_ctx.clone();
        app.runtime.spawn(async move {
            let mut day = chrono::Local::now().date_naive();
            loop {
                use chrono::Timelike;
                let now = chrono::Local::now();
                let seconds = 86400 - now.time().num_seconds_from_midnight();
                tokio::time::sleep(Duration::from_secs(u64::from(seconds.min(30)))).await;
                let today = chrono::Local::now().date_naive();
                if crate::updates::due(Some(day), today) {
                    day = today;
                    if tx.send(Event::UpdateStarted(today)).is_err() {
                        break;
                    }
                    ctx.request_repaint();
                    {
                        let tx = tx.clone();
                        let ctx = ctx.clone();
                        tokio::spawn(async move {
                            if tx.send(Event::AppReleaseStarted(today)).is_err() {
                                return;
                            }
                            let result = crate::app_updates::check_app_release()
                                .await
                                .map_err(|e| format!("{e:#}"));
                            let _ = tx.send(Event::AppRelease(result));
                            ctx.request_repaint();
                        });
                    }
                    if tx
                        .send(Event::UpdateCheck(
                            crate::updates::check_latest()
                                .await
                                .map_err(|e| format!("{e:#}")),
                        ))
                        .is_err()
                    {
                        break;
                    }
                    ctx.request_repaint();
                }
            }
        });
        app
    }
    fn say(&mut self, message: impl Into<String>, error: bool) {
        self.notice = message.into();
        self.error = error;
        self.store
            .log(if error { "ERROR" } else { "INFO" }, &self.notice);
    }
    fn check_updates(&mut self) {
        if self.update_check_busy {
            return;
        }
        self.update_check_busy = true;
        self.update_check_error.clear();
        self.update_check_day = Some(chrono::Local::now().date_naive());
        let tx = self.sender.clone();
        self.runtime.spawn(async move {
            let _ = tx.send(Event::UpdateCheck(
                crate::updates::check_latest()
                    .await
                    .map_err(|e| format!("{e:#}")),
            ));
        });
    }
    fn check_application_updates(&mut self) {
        if self.app_check_busy {
            return;
        }
        self.app_check_busy = true;
        self.app_check_error.clear();
        self.app_check_day = Some(chrono::Local::now().date_naive());
        let tx = self.sender.clone();
        self.runtime.spawn(async move {
            let result = crate::app_updates::check_app_release()
                .await
                .map_err(|e| format!("{e:#}"));
            let _ = tx.send(Event::AppRelease(result));
        });
    }
    fn save(&mut self) {
        if let Err(e) = self.store.save(&self.settings) {
            self.say(e.to_string(), true);
        }
    }
    fn selected_connection(&self) -> Option<(Endpoint, Tenant)> {
        let (e, t) = self.selected?;
        let endpoint = self.settings.endpoints.iter().find(|x| x.id == e)?;
        Some((
            endpoint.clone(),
            endpoint.tenants.iter().find(|x| x.id == t)?.clone(),
        ))
    }
    fn inventory(&self) -> Option<&Inventory> {
        self.selected.and_then(|(_, t)| self.inventories.get(&t))
    }
    fn probe(&mut self, path: PathBuf) {
        self.busy += 1;
        let tx = self.sender.clone();
        self.runtime.spawn(async move {
            let r = ovf::probe(&path).await.map_err(|e| e.to_string());
            let _ = tx.send(Event::Tool(path, r));
        });
    }
    fn pick(&self, pick: Pick) {
        let tx = self.sender.clone();
        self.runtime.spawn(async move {
            let dialog = rfd::AsyncFileDialog::new();
            let selected = match pick {
                Pick::Tool | Pick::Download => dialog.pick_folder().await,
                Pick::Upload | Pick::Validate => {
                    dialog
                        .add_filter("Образы виртуальных машин", &["ova", "ovf", "iso"])
                        .pick_file()
                        .await
                }
                Pick::Export => {
                    dialog
                        .add_filter("Зашифрованный экспорт OVF UI", &["ovfdock"])
                        .set_file_name("connections.ovfdock")
                        .save_file()
                        .await
                }
                Pick::Import => {
                    dialog
                        .add_filter("Зашифрованный экспорт OVF UI", &["ovfdock"])
                        .pick_file()
                        .await
                }
            };
            let _ = tx.send(Event::Picked(pick, selected.map(|p| p.path().to_owned())));
        });
    }
    fn connect(&mut self) {
        let Some((endpoint, tenant)) = self.selected_connection() else {
            return;
        };
        let store = self.store.clone();
        let tx = self.sender.clone();
        self.busy += 1;
        self.runtime.spawn(async move {
            let id = tenant.id;
            let r = async {
                let secret = tokio::task::spawn_blocking(move || store.secret(id)).await??;
                let session = CloudSession::login(&endpoint, &tenant, secret).await?;
                let inventory = session.inventory().await?;
                Ok::<_, anyhow::Error>((session, inventory))
            }
            .await
            .map_err(|e| format!("{e:#}"));
            let _ = tx.send(Event::Inventory(id, r));
        });
    }
    fn inspect(&mut self, object: CloudObject) {
        let Some((_, t)) = self.selected else { return };
        let Some(session) = self.sessions.get(&t).cloned() else {
            return;
        };
        self.busy += 1;
        let tx = self.sender.clone();
        self.runtime.spawn(async move {
            let r = session
                .inspect_export(&object)
                .await
                .map_err(|e| format!("{e:#}"));
            let _ = tx.send(Event::Inspection(t, r));
        });
    }
    fn refresh_catalogs(&mut self, id: Uuid, preferred: Option<String>) {
        let Some(session) = self.sessions.get(&id).cloned() else {
            return;
        };
        let tx = self.sender.clone();
        self.busy += 1;
        self.runtime.spawn(async move {
            let r=async {
                for attempt in 0..15 {
                    let items=session.catalogs().await?;
                    if preferred.as_ref().is_none_or(|name|items.iter().any(|o|&o.name==name)) {return Ok(items)}
                    if attempt <14 {tokio::time::sleep(Duration::from_secs(2)).await;}
                }
                anyhow::bail!("Каталог создан, но ещё не появился в списке API. Нажмите «Обновить каталоги» через несколько секунд.")
            }.await.map_err(|e:anyhow::Error|format!("{e:#}"));
            let _=tx.send(Event::Catalogs(id,preferred,r));
        });
    }
    fn enqueue_transfer(&mut self, request: TransferRequest, parallel: bool) {
        let tenant = request.tenant.id;
        let changes_catalog = request.direction == Direction::Upload || request.capture.is_some();
        let id = self.jobs.enqueue(request, parallel);
        self.transfer_tenants.insert(id, (tenant, changes_catalog));
    }
    fn refresh_completed_transfers(&mut self, force: bool) {
        // Snapshots contain logs; polling once per second avoids copying them on every frame.
        if force || self.transfer_watch_at.elapsed() >= Duration::from_secs(1) {
            self.transfer_watch_at = Instant::now();
            let snapshots = self.jobs.snapshots();
            let present: HashSet<_> = snapshots.iter().map(|j| j.id).collect();
            self.transfer_statuses.retain(|id, _| present.contains(id));
            self.transfer_tenants.retain(|id, _| present.contains(id));
            for job in snapshots {
                self.observe_transfer_status(&job);
            }
        }
        if self.transfer.is_some()
            || self.queue_choice.is_some()
            || self.connection.open
            || self.catalog_form.open
            || self.backup.open
            || self.s3_form.open
            || self.busy > 0
        {
            return;
        }
        for (id, inventory) in self.deferred_transfer_inventory.drain() {
            self.inventories.insert(id, inventory);
        }
        let Some((_, tenant)) = self.selected else {
            return;
        };
        let Some(session) = self.sessions.get(&tenant).cloned() else {
            return;
        };
        if !self.refresh_after_transfer.remove(&tenant) {
            return;
        }
        let tx = self.sender.clone();
        self.busy += 1;
        self.runtime.spawn(async move {
            let result = session.inventory().await.map_err(|e| format!("{e:#}"));
            let _ = tx.send(Event::TransferInventory(tenant, result));
        });
    }
    fn observe_transfer_status(&mut self, job: &JobSnapshot) {
        let previous = self.transfer_statuses.insert(job.id, job.status);
        if previous == Some(job.status)
            || !matches!(
                job.status,
                JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
            )
        {
            return;
        }
        // Snapshot identity survives retry. Downloads may also capture a template
        // or change power state, so their final inventory is refreshed as well.
        let target = job
            .tenant_id
            .map(|tenant| (tenant, true))
            .or_else(|| self.transfer_tenants.get(&job.id).copied());
        if let Some((tenant, true)) = target {
            self.refresh_after_transfer.insert(tenant);
        }
    }
    fn handle_events(&mut self) {
        while let Ok(event) = self.receiver.try_recv() {
            if !matches!(
                event,
                Event::Picked(..)
                    | Event::UpdateCheck(..)
                    | Event::UpdateStarted(..)
                    | Event::AppRelease(..)
                    | Event::AppReleaseStarted(..)
                    | Event::DestinationCheck(..)
            ) {
                self.busy = self.busy.saturating_sub(1);
            }
            match event {
                Event::AppReleaseStarted(day) => {
                        self.app_check_busy = true;
                        self.app_check_day = Some(day);
                }
                Event::TransferInventory(id, result) => match result {
                    Ok(inventory) => { self.deferred_transfer_inventory.insert(id, inventory); }
                    Err(error) => self.say(format!("Передача завершилась, но список объектов обновить не удалось: {error}. Нажмите «Обновить»."), true),
                },
                Event::DestinationCheck(id, key, result) => {
                    if let Some(form) = &mut self.transfer
                        && form.id == id
                        && form.destination_fingerprint() == key
                    {
                        form.destination_busy = false;
                        form.destination_cancel = None;
                        match result {
                            Ok(preview) => {
                                form.destination_preview = Some(preview);
                                form.destination_error.clear();
                            }
                            Err(error) => {
                                form.destination_preview = None;
                                form.destination_error = error;
                            }
                        }
                    }
                }
                Event::AppRelease(result) => {
                    self.app_check_busy = false;
                        match result {
                            Ok(release) => {
                                self.app_release = Some(release);
                                self.app_check_error.clear();
                            }
                            Err(error) => {
                                self.app_release = None;
                                self.app_check_error = error;
                            }
                        }
                }
                Event::UpdateStarted(day) => {
                    self.update_check_day = Some(day);
                    self.update_check_busy = true;
                }
                Event::UpdateCheck(result) => {
                    self.update_check_busy = false;
                    match result {
                        Ok(version) => {
                            self.latest_version = Some(version);
                            self.update_check_error.clear();
                        }
                        Err(e) => {
                            self.latest_version = None;
                            self.update_check_error = e;
                        }
                    }
                }
                Event::Tool(path, result) => match result {
                    Ok(v) => {
                        self.settings.ovf_directory = path;
                        self.tool_version = v;
                        self.setup = false;
                        self.save();
                        self.say("OVF Tool готов к работе", false);
                    }
                    Err(e) => {
                        self.setup = true;
                        self.say(e, true);
                    }
                },
                Event::Inventory(id, result) => match result {
                    Ok((session, inv)) => {
                        self.deferred_transfer_inventory.remove(&id);
                        let count = inv.user_object_count();
                        let warnings = inv.warnings.len();
                        self.sessions.insert(id, session);
                        self.inventories.insert(id, inv);
                        self.say(
                            format!(
                                "Подключено. Пользовательских объектов: {count}{}",
                                if warnings > 0 {
                                    format!(". Недоступных разделов: {warnings}")
                                } else {
                                    String::new()
                                }
                            ),
                            false,
                        );
                    }
                    Err(e) => self.say(e, true),
                },
                Event::Inspection(id, result) => match result {
                    Ok(inspection) => {
                        if self.selected.is_some_and(|(_, t)| t == id) {
                            if let Some(form) = &mut self.transfer {
                                form.object = inspection.object.clone();
                                form.inspection = Some(inspection);
                                form.power_confirm = false;
                                form.eject_confirm = false;
                            } else if let Some((endpoint, tenant)) = self.selected_connection() {
                                let format = if inspection.object.kind == ObjectKind::Media {
                                    "ISO"
                                } else {
                                    "OVA"
                                }
                                .into();
                                let path = self.settings.download_directory.clone();
                                let free = fs2::available_space(&path).ok();
                                let estimate = inspection
                                    .estimated_bytes
                                    .map(|b| b as f64 / (1024.0 * 1024.0 * 1024.0) * 1.1)
                                    .unwrap_or(0.0);
                                self.transfer = Some(TransferForm {
                                    id: Uuid::new_v4(),
                                    destination_key: String::new(),
                                    destination_busy: false,
                                    destination_cancel: None,
                                    destination_preview: None,
                                    destination_error: String::new(),
                                    allow_unknown_capacity: false,
                                    endpoint,
                                    tenant,
                                    object: inspection.object.clone(),
                                    inspection: Some(inspection),
                                    direction: Direction::Download,
                                    path,
                                    format,
                                    s3: None,
                                    limit: 0,
                                    estimate_gib: estimate,
                                    free,
                                    power_confirm: false,
                                    eulas: false,
                                    error: String::new(),
                                    via_template: false,
                                    capture_catalog: String::new(),
                                    capture_name: format!(
                                        "export-{}",
                                        &Uuid::new_v4().simple().to_string()[..12]
                                    ),
                                    eject_confirm: false,
                                    parallel_threads: 4,
                                });
                            }
                        }
                    }
                    Err(e) => {
                        self.export_error = Some(e.clone());
                        if let Some(f) = &mut self.transfer {
                            f.error = e.clone();
                        }
                        self.store.log("ERROR", &e);
                        self.say(
                            "Скачивание недоступно — подробности в диалоговом окне",
                            true,
                        );
                    }
                },
                Event::Credential(result) => match result {
                    Ok((mut endpoint, tenant)) => {
                        let id = (endpoint.id, tenant.id);
                        if let Some(existing) = self
                            .settings
                            .endpoints
                            .iter_mut()
                            .find(|e| e.id == endpoint.id)
                        {
                            existing.name = endpoint.name;
                            existing.url = endpoint.url;
                            if let Some(t) = existing.tenants.iter_mut().find(|t| t.id == tenant.id)
                            {
                                *t = tenant;
                            } else {
                                existing.tenants.push(tenant);
                            }
                        } else {
                            endpoint.tenants.push(tenant);
                            self.settings.endpoints.push(endpoint);
                        }
                        self.selected = Some(id);
                        self.connection = Default::default();
                        self.save();
                        self.connect();
                    }
                    Err(e) => self.say(e, true),
                },
                Event::S3Saved(result) => match result {
                    Ok(profile) => {
                        if let Some(s) = self
                            .settings
                            .s3_profiles
                            .iter_mut()
                            .find(|s| s.id == profile.id)
                        {
                            *s = profile;
                        } else {
                            self.settings.s3_profiles.push(profile);
                        }
                        self.s3_form = Default::default();
                        self.save();
                        self.say("Профиль S3 сохранён", false);
                    }
                    Err(e) => self.say(e, true),
                },
                Event::Catalog(id, name, result) => match result {
                    Ok(()) => {
                        self.catalog_form = Default::default();
                        self.say("Каталог создан", false);
                        self.refresh_catalogs(id, Some(name));
                    }
                    Err(e) => self.say(e, true),
                },
                Event::Catalogs(id, preferred, result) => match result {
                    Ok(items) => {
                        if let Some(form) = self
                            .transfer
                            .as_mut()
                            .filter(|f| f.tenant.id == id && f.via_template)
                        {
                            if let Some(name) = preferred.as_ref() {
                                form.capture_catalog = items
                                    .iter()
                                    .find(|c| &c.name == name)
                                    .map(|c| c.href.clone())
                                    .unwrap_or_default();
                            }
                            if !items.iter().any(|c| c.href == form.capture_catalog) {
                                form.capture_catalog.clear();
                            }
                        }
                        if let Some(form) = self
                            .transfer
                            .as_mut()
                            .filter(|f| f.tenant.id == id && f.direction == Direction::Upload)
                        {
                            if let Some(name) = preferred {
                                form.object.catalog = name;
                            }
                            if !items.iter().any(|o| o.name == form.object.catalog) {
                                form.object.catalog =
                                    items.first().map(|o| o.name.clone()).unwrap_or_default();
                            }
                        }
                        let count = items.len();
                        if let Some(inv) = self.inventories.get_mut(&id) {
                            inv.objects.retain(|o| o.kind != ObjectKind::Catalog);
                            inv.objects.extend(items);
                        }
                        self.say(format!("Каталоги обновлены: {count}"), false);
                    }
                    Err(e) => {
                        if let Some(f) = self.transfer.as_mut().filter(|f| f.tenant.id == id) {
                            f.error = e.clone();
                        }
                        self.say(e, true);
                    }
                },
                Event::Validation(path, result) => {
                    self.validation_cancel = None;
                    if path != self.validate_path {
                        continue;
                    }
                    match result {
                        Ok(report) => {
                            self.validation = Some(report);
                            self.say("Проверка завершена", false);
                        }
                        Err(e) => self.say(e, true),
                    }
                }
                Event::Backup(result) => match result {
                    Ok(settings) => {
                        if let Some(s) = settings {
                            self.settings = s;
                            self.app_release = None;
                            self.app_check_error.clear();
                            self.app_check_day = None;
                        }
                        self.backup = Default::default();
                        self.say("Операция с зашифрованным экспортом завершена", false);
                    }
                    Err(e) => self.say(e, true),
                },
                Event::Picked(pick, Some(path)) => match pick {
                    Pick::Tool => self.probe(path),
                    Pick::Download => {
                        self.settings.download_directory = path.clone();
                        if let Some(f) = &mut self.transfer {
                            f.free = fs2::available_space(&path).ok();
                            f.path = path;
                        }
                        self.save();
                    }
                    Pick::Upload => {
                        if let Some(f) = &mut self.transfer {
                            f.object.name = path
                                .file_stem()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .into_owned();
                            f.object.kind = if path
                                .extension()
                                .is_some_and(|e| e.eq_ignore_ascii_case("iso"))
                            {
                                ObjectKind::Media
                            } else {
                                ObjectKind::Template
                            };
                            f.path = path;
                        }
                    }
                    Pick::Validate => {
                        self.validate_path = path;
                        self.validation = None;
                    }
                    Pick::Export | Pick::Import => {
                        self.backup = BackupForm {
                            open: true,
                            import: matches!(pick, Pick::Import),
                            path,
                            password: String::new(),
                            repeat: String::new(),
                        };
                    }
                },
                Event::Picked(_, None) => {}
            }
        }
    }

    fn sidebar(&mut self, ctx: &egui::Context) {
        egui::SidePanel::left("navigation")
            .exact_width(252.0)
            .frame(
                egui::Frame::new()
                    .fill(if self.settings.light_theme {
                        Color32::from_rgb(245, 246, 249)
                    } else {
                        Color32::from_rgb(20, 21, 26)
                    })
                    .inner_margin(20),
            )
            .show(ctx, |ui| {
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    logo(ui);
                    ui.vertical(|ui| {
                        ui.label(RichText::new("OVF UI").size(24.0).strong());
                        ui.label(
                            RichText::new("VMware transfer desk")
                                .size(11.0)
                                .color(muted(ui)),
                        );
                    });
                });
                ui.add_space(30.0);
                ui.label(
                    RichText::new("РАБОЧЕЕ ПРОСТРАНСТВО")
                        .size(10.0)
                        .color(muted(ui)),
                );
                ui.add_space(10.0);
                for (page, label) in [
                    (Page::Machines, "Виртуальные машины"),
                    (Page::Catalogs, "Каталоги и образы"),
                    (Page::Validate, "Проверка пакета"),
                ] {
                    if nav(ui, label, self.page == page) {
                        self.page = page;
                    }
                }
                ui.add_space(25.0);
                ui.horizontal(|ui| {
                    ui.label(RichText::new("ПОДКЛЮЧЕНИЯ").size(10.0).color(muted(ui)));
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if ui.button("+").on_hover_text("Добавить endpoint").clicked() {
                            self.connection = ConnectionForm {
                                open: true,
                                url: "https://".into(),
                                ..Default::default()
                            };
                        }
                    });
                });
                ui.add_space(8.0);
                egui::ScrollArea::vertical()
                    .id_salt("endpoints")
                    .max_height((ui.available_height() - 180.0).max(100.0))
                    .show(ui, |ui| {
                        if self.settings.endpoints.is_empty() {
                            ui.label(
                                RichText::new(
                                    "Добавьте первый endpoint,\nзатем подключите тенант.",
                                )
                                .size(12.0)
                                .color(muted(ui)),
                            );
                        }
                        for endpoint in self.settings.endpoints.clone() {
                            egui::CollapsingHeader::new(RichText::new(&endpoint.name).strong())
                                .id_salt(endpoint.id)
                                .default_open(true)
                                .show(ui, |ui| {
                                    for tenant in &endpoint.tenants {
                                        let selected =
                                            self.selected == Some((endpoint.id, tenant.id));
                                        let connected = self.sessions.contains_key(&tenant.id);
                                        ui.horizontal(|ui| {
                                            ui.colored_label(
                                                if connected {
                                                    success_color(ui)
                                                } else {
                                                    muted(ui)
                                                },
                                                "•",
                                            );
                                            if ui
                                                .selectable_label(selected, &tenant.organization)
                                                .clicked()
                                            {
                                                self.selected = Some((endpoint.id, tenant.id));
                                                self.catalog_filter.clear();
                                                self.filter.clear();
                                                if !connected {
                                                    self.connect();
                                                }
                                            }
                                        });
                                    }
                                    ui.horizontal(|ui| {
                                        ui.add_space(18.0);
                                        if ui.button("+ Тенант").clicked() {
                                            self.connection = ConnectionForm {
                                                open: true,
                                                endpoint_id: Some(endpoint.id),
                                                name: endpoint.name.clone(),
                                                url: endpoint.url.clone(),
                                                ..Default::default()
                                            };
                                        }
                                    });
                                });
                            ui.add_space(4.0);
                        }
                    });
                ui.with_layout(egui::Layout::bottom_up(egui::Align::LEFT), |ui| {
                    ui.label(
                        RichText::new(concat!("OVF UI  /  ", env!("CARGO_PKG_VERSION")))
                            .size(10.0)
                            .color(muted(ui)),
                    );
                    ui.add_space(12.0);
                    if nav(ui, "Настройки", self.page == Page::Settings) {
                        self.page = Page::Settings;
                    }
                    if nav(
                        ui,
                        &format!("Передачи    {}", self.jobs.pending_count()),
                        self.show_transfers,
                    ) {
                        self.show_transfers = true;
                    }
                    ui.add_space(10.0);
                    ui.separator();
                    ui.horizontal(|ui| {
                        ui.spacing_mut().item_spacing.x = 5.0;
                        ui.spacing_mut().button_padding.x = 4.0;
                        ui.label(RichText::new("OVF Tool").size(12.0).strong());
                        ui.label(
                            RichText::new(short_version(&self.tool_version))
                                .size(11.0)
                                .color(muted(ui)),
                        )
                        .on_hover_text(&self.tool_version);
                        if self.update_check_busy {
                            ui.label(RichText::new("…").size(10.0).color(muted(ui)))
                                .on_hover_text("Проверяется версия OVF Tool");
                        } else if let (Some(installed), Some(latest)) = (
                            crate::updates::Version::parse(&self.tool_version),
                            self.latest_version,
                        ) {
                            if latest > installed {
                                if ui
                                    .link(
                                        RichText::new("+upgrade").size(10.0).color(error_color(ui)),
                                    )
                                    .on_hover_text(format!(
                                        "Доступна версия {latest}. Открыть Broadcom"
                                    ))
                                    .clicked()
                                {
                                    ui.ctx().open_url(egui::OpenUrl::new_tab(
                                        crate::updates::DOWNLOAD_URL,
                                    ));
                                }
                            } else {
                                ui.label(
                                    RichText::new("latest").size(10.0).color(success_color(ui)),
                                )
                                .on_hover_text(format!("На Broadcom: {latest}"));
                            }
                        }
                        if !self.update_check_error.is_empty() {
                            ui.label(RichText::new("?").size(10.0).color(muted(ui)))
                                .on_hover_text(&self.update_check_error);
                        }
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let ready = !self.tool_version.is_empty();
                            let color = if ready {
                                success_color(ui)
                            } else {
                                error_color(ui)
                            };
                            ui.colored_label(
                                color,
                                RichText::new(if ready { "OK" } else { "OFF" })
                                    .size(11.0)
                                    .strong(),
                            );
                            let (r, _) =
                                ui.allocate_exact_size(Vec2::splat(8.0), egui::Sense::hover());
                            ui.painter().circle_filled(r.center(), 3.0, color);
                        });
                    });
                });
            });
    }

    fn header(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let crumb = self
                .selected_connection()
                .map(|(e, t)| format!("{}  /  {}", e.name, t.organization))
                .unwrap_or_else(|| "Локальное рабочее пространство".into());
            ui.label(RichText::new(crumb).size(12.0).color(muted(ui)));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .button(if self.settings.light_theme {
                        "Тёмная тема"
                    } else {
                        "Светлая тема"
                    })
                    .clicked()
                {
                    self.settings.light_theme = !self.settings.light_theme;
                    apply_theme(ui.ctx(), self.settings.light_theme);
                    self.save();
                }
                if self.busy > 0 {
                    ui.spinner();
                    ui.label(RichText::new("Выполняется…").color(muted(ui)).size(12.0));
                }
            });
        });
        ui.add_space(16.0);
        ui.horizontal(|ui| {
            ui.heading(
                RichText::new(match self.page {
                    Page::Machines => "Виртуальные машины",
                    Page::Catalogs => "Каталоги и образы",
                    Page::Validate => "Проверка пакета",
                    Page::Settings => "Настройки",
                })
                .size(28.0),
            );
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if matches!(self.page, Page::Machines | Page::Catalogs) {
                    if accent(ui, "Загрузить в каталог").clicked() {
                        self.open_upload();
                    }
                    if ui
                        .add_enabled(
                            self.selected.is_some() && self.busy == 0,
                            egui::Button::new("Обновить"),
                        )
                        .clicked()
                    {
                        self.connect();
                    }
                }
            });
        });
        ui.add_space(6.0);
        ui.label(
            RichText::new(match self.page {
                Page::Machines => "Экспортируйте vApp и ВМ, контролируя каждый этап передачи.",
                Page::Catalogs => "Шаблоны, ISO и каталоги выбранной организации.",
                Page::Validate => "Проверьте структуру, файлы и контрольные суммы перед импортом.",
                Page::Settings => "Инструменты, подключения и локальное хранение данных.",
            })
            .size(13.0)
            .color(muted(ui)),
        );
        ui.add_space(22.0);
    }

    fn inventory_page(&mut self, ui: &mut egui::Ui) {
        let Some(inventory) = self.inventory().cloned() else {
            ui.add_space(45.0);
            card(ui, |ui| {
                ui.add_space(24.0);
                ui.label(
                    RichText::new("Всё начинается с подключения")
                        .size(23.0)
                        .strong(),
                );
                ui.add_space(10.0);
                ui.label("Добавьте адрес Cloud Director и тенант. Сохранённые подключения появятся слева.");
                ui.label("Пароли и токены сохраняются в системном хранилище этого компьютера.");
                ui.add_space(22.0);
                if accent(ui, "+ Добавить подключение").clicked() {
                    self.connection = ConnectionForm {
                        open: true,
                        url: "https://".into(),
                        ..Default::default()
                    };
                }
                ui.add_space(18.0);
            });
            return;
        };
        let vms = inventory
            .objects
            .iter()
            .filter(|o| o.kind == ObjectKind::Vm)
            .count();
        let catalogs = inventory
            .objects
            .iter()
            .filter(|o| o.kind == ObjectKind::Catalog)
            .count();
        let owned_catalogs = inventory
            .objects
            .iter()
            .filter(|o| o.kind == ObjectKind::Catalog)
            .map(|o| o.name.as_str())
            .collect::<std::collections::HashSet<_>>();
        let images = inventory
            .objects
            .iter()
            .filter(|o| {
                matches!(o.kind, ObjectKind::Template | ObjectKind::Media)
                    && owned_catalogs.contains(o.catalog.as_str())
            })
            .count();
        ui.columns(3, |cols| {
            stat(
                &mut cols[0],
                "ВИРТУАЛЬНЫЕ МАШИНЫ",
                &vms.to_string(),
                "в выбранном тенанте",
            );
            stat(
                &mut cols[1],
                "КАТАЛОГИ",
                &catalogs.to_string(),
                &format!("{images} шаблонов и ISO"),
            );
            stat(
                &mut cols[2],
                "АКТИВНЫЕ ПЕРЕДАЧИ",
                &self.jobs.active_count().to_string(),
                "OVF Tool + защищённая сессия",
            );
        });
        ui.add_space(20.0);
        ui.horizontal(|ui| {
            ui.add(
                egui::TextEdit::singleline(&mut self.filter)
                    .hint_text("Поиск по имени, каталогу или ЦОДу")
                    .margin(egui::Margin::symmetric(12, 10))
                    .desired_width(330.0),
            );
            if self.page == Page::Catalogs {
                egui::ComboBox::from_id_salt("catalog-filter")
                    .selected_text(if self.catalog_filter.is_empty() {
                        "Все каталоги"
                    } else {
                        &self.catalog_filter
                    })
                    .show_ui(ui, |ui| {
                        ui.selectable_value(
                            &mut self.catalog_filter,
                            String::new(),
                            "Все каталоги",
                        );
                        for c in inventory
                            .objects
                            .iter()
                            .filter(|o| o.kind == ObjectKind::Catalog)
                        {
                            ui.selectable_value(&mut self.catalog_filter, c.name.clone(), &c.name);
                        }
                    });
                if ui.button("+ Каталог").clicked() {
                    self.catalog_form = CatalogForm {
                        open: true,
                        ..Default::default()
                    };
                }
            }
        });
        ui.add_space(14.0);
        if self.page == Page::Catalogs {
            catalog_grid(ui, &inventory.objects, &mut self.catalog_filter);
            ui.add_space(12.0);
        }
        for warning in &inventory.warnings {
            ui.colored_label(warning_color(ui), warning);
        }
        let query = self.filter.to_lowercase();
        let filtered = inventory
            .objects
            .iter()
            .filter(|o| {
                let kind = if self.page == Page::Machines {
                    matches!(o.kind, ObjectKind::Vm | ObjectKind::Vapp)
                } else {
                    matches!(o.kind, ObjectKind::Template | ObjectKind::Media)
                        && owned_catalogs.contains(o.catalog.as_str())
                };
                kind && (self.catalog_filter.is_empty()
                    || o.catalog == self.catalog_filter
                    || self.page != Page::Catalogs)
                    && (query.is_empty()
                        || format!("{} {} {}", o.name, o.catalog, o.vdc)
                            .to_lowercase()
                            .contains(&query))
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut download = None;
        card(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new(format!("ОБЪЕКТЫ  /  {}", filtered.len()))
                        .size(11.0)
                        .color(muted(ui)),
                );
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(
                        RichText::new(format!("API {}", inventory.api_version))
                            .size(11.0)
                            .color(muted(ui)),
                    );
                });
            });
            ui.add_space(8.0);
            ui.separator();
            egui::ScrollArea::vertical()
                .id_salt("objects")
                .auto_shrink([false, false])
                .max_height((ui.available_height() - 40.0).max(160.0))
                .show(ui, |ui| {
                    if filtered.is_empty() {
                        ui.add_space(32.0);
                        ui.label(RichText::new("Объектов пока нет").size(18.0));
                        ui.label(
                            RichText::new(
                                "Измените фильтр, обновите список или загрузите образ в каталог.",
                            )
                            .color(muted(ui)),
                        );
                        ui.add_space(32.0);
                    }
                    for object in filtered {
                        ui.push_id(&object.href, |ui| {
                            ui.add_space(9.0);
                            ui.horizontal(|ui| {
                                let is_vm = object.kind == ObjectKind::Vm;
                                if is_vm {
                                    ui.add_space(16.0);
                                }
                                let (rect, _) = ui.allocate_exact_size(
                                    Vec2::new(34.0, 34.0),
                                    egui::Sense::hover(),
                                );
                                ui.painter()
                                    .rect_filled(rect, 7.0, RED.gamma_multiply(0.14));
                                ui.painter().text(
                                    rect.center(),
                                    egui::Align2::CENTER_CENTER,
                                    match object.kind {
                                        ObjectKind::Vm => "VM",
                                        ObjectKind::Vapp => "VA",
                                        ObjectKind::Template => "OV",
                                        _ => "ISO",
                                    },
                                    egui::FontId::proportional(11.0),
                                    RED,
                                );
                                ui.vertical(|ui| {
                                    ui.label(RichText::new(&object.name).strong().size(14.0));
                                    let location = if is_vm {
                                        format!("{}  /  {}", object.vdc, object.parent_name)
                                    } else if object.catalog.is_empty() {
                                        object.vdc.clone()
                                    } else {
                                        object.catalog.clone()
                                    };
                                    ui.label(
                                        RichText::new(format!(
                                            "{}  ·  {location}",
                                            object.kind.label()
                                        ))
                                        .size(11.0)
                                        .color(muted(ui)),
                                    );
                                    if is_vm {
                                        ui.label(
                                            RichText::new(format!(
                                                "{} vCPU  ·  RAM {}  ·  Диски {}",
                                                object
                                                    .cpu
                                                    .map(|n| n.to_string())
                                                    .unwrap_or_else(|| "—".into()),
                                                object
                                                    .memory_mb
                                                    .map(|n| human_bytes(
                                                        n.saturating_mul(1024 * 1024)
                                                    ))
                                                    .unwrap_or_else(|| "—".into()),
                                                object
                                                    .bytes
                                                    .map(human_bytes)
                                                    .unwrap_or_else(|| "—".into())
                                            ))
                                            .size(12.0)
                                            .color(muted(ui)),
                                        );
                                    }
                                });
                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if ui
                                            .add_enabled(
                                                self.busy == 0 && !self.tool_version.is_empty(),
                                                egui::Button::new("Скачать"),
                                            )
                                            .clicked()
                                        {
                                            download = Some(object.clone());
                                        }
                                        ui.add_space(12.0);
                                        ui.label(
                                            RichText::new(
                                                object
                                                    .bytes
                                                    .map(human_bytes)
                                                    .unwrap_or_else(|| "Размер уточняется".into()),
                                            )
                                            .size(12.0)
                                            .color(muted(ui)),
                                        );
                                        let status = status_label(&object.status);
                                        if !status.is_empty() {
                                            ui.label(RichText::new(status).size(11.0).color(
                                                if matches!(
                                                    object.status.as_str(),
                                                    "POWERED_ON" | "4"
                                                ) {
                                                    success_color(ui)
                                                } else {
                                                    muted(ui)
                                                },
                                            ));
                                        }
                                    },
                                );
                            });
                            ui.add_space(9.0);
                            ui.separator();
                        });
                    }
                });
        });
        if let Some(object) = download {
            self.inspect(object);
        }
    }

    fn open_upload(&mut self) {
        let Some((endpoint, tenant)) = self.selected_connection() else {
            self.say("Сначала выберите и подключите тенант", true);
            return;
        };
        if !self.sessions.contains_key(&tenant.id) {
            self.say("Сначала подключитесь к тенанту", true);
            return;
        }
        let catalog = self
            .inventory()
            .and_then(|i| i.objects.iter().find(|o| o.kind == ObjectKind::Catalog))
            .map(|o| o.name.clone())
            .unwrap_or_default();
        let vdc = self
            .inventory()
            .and_then(|i| i.objects.iter().find(|o| o.kind == ObjectKind::Vdc))
            .map(|o| o.name.clone())
            .unwrap_or_default();
        self.refresh_catalogs(tenant.id, None);
        self.transfer = Some(TransferForm {
            id: Uuid::new_v4(),
            destination_key: String::new(),
            destination_busy: false,
            destination_cancel: None,
            destination_preview: None,
            destination_error: String::new(),
            allow_unknown_capacity: false,
            endpoint,
            tenant,
            inspection: None,
            direction: Direction::Upload,
            object: CloudObject {
                href: String::new(),
                name: String::new(),
                kind: ObjectKind::Template,
                vdc,
                catalog,
                parent_href: String::new(),
                parent_name: String::new(),
                status: String::new(),
                bytes: None,
                cpu: None,
                memory_mb: None,
                owner: String::new(),
            },
            path: PathBuf::new(),
            format: "OVA".into(),
            s3: None,
            limit: 0,
            estimate_gib: 0.0,
            free: None,
            power_confirm: false,
            eulas: false,
            error: String::new(),
            via_template: false,
            capture_catalog: String::new(),
            capture_name: String::new(),
            eject_confirm: false,
            parallel_threads: 4,
        });
    }

    fn validation_page(&mut self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical()
            .id_salt("validation-page")
            .show(ui, |ui| {
                card(ui, |ui| {
                    ui.label(RichText::new("Локальный пакет").size(18.0).strong());
                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        if ui.button("Выбрать OVA / OVF / ISO").clicked() {
                            self.pick(Pick::Validate);
                        }
                        ui.label(self.validate_path.display().to_string());
                    });
                    ui.add_space(16.0);
                    ui.label(
                "OVA читается без распаковки. Для OVF диски должны находиться рядом с описанием.",
            );
                    ui.label(
                        RichText::new(
                            "Проверка совместимости с конкретным ЦОДом не заменяет пробный импорт.",
                        )
                        .color(muted(ui)),
                    );
                    ui.add_space(18.0);
                    if let Some(cancel) = &self.validation_cancel {
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label("Чтение файлов и проверка контрольных сумм…");
                            if ui.button("Отменить").clicked() {
                                cancel.cancel();
                            }
                        });
                    } else if ui
                        .add_enabled(
                            self.validate_path.is_file(),
                            egui::Button::new(
                                RichText::new("Проверить пакет").color(Color32::WHITE),
                            )
                            .fill(RED),
                        )
                        .clicked()
                    {
                        self.start_validation();
                    }
                });
                if let Some(report) = &self.validation {
                    ui.add_space(18.0);
                    card(ui, |ui| {
                        ui.colored_label(
                            success_color(ui),
                            RichText::new("Проверка пройдена").size(20.0).strong(),
                        );
                        ui.label(format!("Прочитано: {}", human_bytes(report.bytes)));
                        for line in &report.lines {
                            ui.label(line);
                        }
                        for warning in &report.warnings {
                            ui.add_space(8.0);
                            ui.colored_label(warning_color(ui), warning);
                        }
                    });
                }
            });
    }
    fn start_validation(&mut self) {
        let path = self.validate_path.clone();
        let dir = self.settings.ovf_directory.clone();
        let cancel = CancellationToken::new();
        self.validation_cancel = Some(cancel.clone());
        self.validation = None;
        self.busy += 1;
        let tx = self.sender.clone();
        self.runtime.spawn(async move {
            let r = async {
                let p = path.clone();
                let c = cancel.clone();
                let mut report =
                    tokio::task::spawn_blocking(move || ovf::validate_package(&p, &c)).await??;
                if !path
                    .extension()
                    .is_some_and(|x| x.eq_ignore_ascii_case("iso"))
                {
                    if dir.as_os_str().is_empty() {
                        report.warnings.push(
                            "OVF Tool не выбран: выполнена только локальная проверка.".into(),
                        );
                    } else {
                        let details = ovf::verify_with_tool(&dir, &path, cancel).await?;
                        report.lines.push(format!("OVF Tool: {details}"));
                    }
                }
                Ok::<_, anyhow::Error>(report)
            }
            .await
            .map_err(|e| format!("{e:#}"));
            let _ = tx.send(Event::Validation(path, r));
        });
    }

    fn settings_page(&mut self, ui: &mut egui::Ui) {
        egui::ScrollArea::vertical().show(ui,|ui|{
        card(ui, |ui| {
            ui.label(RichText::new(format!("OVF UI · {}", env!("CARGO_PKG_VERSION"))).size(18.0).strong());
            ui.add_space(8.0);
            ui.label(RichText::new("Публичные стабильные выпуски. Проверка при запуске и раз в день; установка вручную.").color(muted(ui)));
            ui.label(RichText::new("Без входа в GitHub. Пароли, токены и данные подключений не передаются.").color(muted(ui)));
            if ui.add_enabled(!self.app_check_busy, egui::Button::new("Проверить обновления")).clicked() {
                self.check_application_updates();
            }
            if self.app_check_busy { ui.label("Проверяется версия OVF UI…"); }
            if let Some(release) = &self.app_release {
                if release.newer { ui.hyperlink_to(format!("Доступна {} — открыть выпуск", release.version), &release.url); }
                else { ui.colored_label(success_color(ui), format!("Новых стабильных выпусков нет · GitHub {}", release.version)); }
            }
            if let Some(day) = self.app_check_day { ui.label(RichText::new(format!("Последняя проверка: {day}")).size(12.0).color(muted(ui))); }
            if !self.app_check_error.is_empty() { ui.colored_label(error_color(ui), &self.app_check_error); }
        });ui.add_space(14.0);
        card(ui,|ui|{ui.label(RichText::new("OVF Tool").size(18.0).strong());ui.add_space(8.0);ui.label(self.settings.ovf_directory.display().to_string());ui.label(RichText::new(&self.tool_version).color(muted(ui)));ui.add_space(10.0);if ui.button("Изменить папку OVF Tool").clicked(){self.pick(Pick::Tool);}ui.add_space(6.0);if ui.add_enabled(!self.update_check_busy,egui::Button::new("Проверить версию на Broadcom")).clicked(){self.check_updates();}
if self.update_check_busy {ui.label("Проверяется версия…");}
if !self.update_check_error.is_empty(){ui.colored_label(warning_color(ui),&self.update_check_error);}});ui.add_space(14.0);
        card(ui,|ui|{ui.label(RichText::new("Файлы и свободное место").size(18.0).strong());ui.add_space(10.0);ui.horizontal(|ui|{ui.label(self.settings.download_directory.display().to_string());if ui.button("Выбрать папку").clicked(){self.pick(Pick::Download);}});ui.horizontal(|ui|{ui.label("Оставлять свободными, ГиБ:");if ui.add(egui::DragValue::new(&mut self.settings.reserve_gib).range(1..=1024)).changed(){self.save();}});ui.label(RichText::new("S3: образ сначала скачивается сюда. Локальная копия сохраняется после отправки.").color(muted(ui)));});ui.add_space(14.0);
        card(ui,|ui|{ui.horizontal(|ui|{ui.label(RichText::new("Хранилища S3").size(18.0).strong());if ui.button("+ Добавить").clicked(){self.s3_form=S3Form{open:true,endpoint:"https://".into(),region:"us-east-1".into(),..Default::default()};}});ui.add_space(10.0);if self.settings.s3_profiles.is_empty(){ui.label(RichText::new("AWS S3 и совместимые хранилища. HTTPS, адресация path-style.").color(muted(ui)));}for s in self.settings.s3_profiles.clone(){ui.horizontal(|ui|{ui.label(format!("{}  /  {}",s.name,s.bucket));if ui.button("Изменить").clicked(){self.s3_form=S3Form{open:true,id:Some(s.id),name:s.name,endpoint:s.endpoint,region:s.region,bucket:s.bucket,prefix:s.prefix,access_key:s.access_key,secret_key:String::new(),session_token:String::new()};}});}});ui.add_space(14.0);
        card(ui,|ui|{ui.label(RichText::new("Подключения и секреты").size(18.0).strong());ui.add_space(10.0);ui.label("Windows Credential Manager / Linux Secret Service. В settings.json секретов нет.");ui.label("Экспорт защищается паролем: Argon2id + AES-256-GCM.");ui.add_space(10.0);ui.horizontal(|ui|{if ui.button("Экспортировать подключения").clicked(){self.pick(Pick::Export);}
if ui.button("Импортировать").clicked(){self.pick(Pick::Import);}
if ui.add_enabled(self.selected.is_some(),egui::Button::new("Изменить выбранный тенант")).clicked()&& let Some((e,t))=self.selected_connection(){self.connection=ConnectionForm{open:true,endpoint_id:Some(e.id),tenant_id:Some(t.id),name:e.name,url:e.url,organization:t.organization,username:t.username,auth:t.auth,password:String::new()};}});});ui.add_space(14.0);
        card(ui,|ui|{ui.label(RichText::new("Диагностика").size(18.0).strong());ui.label(self.store.directory.display().to_string());ui.add_space(10.0);if ui.button("Открыть журнал").clicked(){self.log_text=std::fs::read_to_string(self.store.directory.join("ovf-dock.log")).unwrap_or_else(|_|"Журнал пока пуст".into());self.show_logs=true;}ui.label(RichText::new("Адреса с параметрами и токены удаляются из журнала. До 4 МиБ на файл.").color(muted(ui)));});
    });
    }

    fn connection_dialog(&mut self, ctx: &egui::Context) {
        if !self.connection.open {
            return;
        }
        let mut open = true;
        let mut save = false;
        dialog(ctx, "connection", 520.0, |ui| {
            dialog_header(ui, "Подключение", "VMware Cloud Director", &mut open);
            ui.add_space(16.0);
            let f = &mut self.connection;
            field(ui, "Название endpoint", &mut f.name, false);
            field(ui, "Endpoint HTTPS", &mut f.url, false);
            field(ui, "Тенант / организация", &mut f.organization, false);
            ui.add_space(8.0);
            ui.label(RichText::new("Тип входа:").size(12.0).strong());
            egui::ComboBox::from_id_salt("login-kind")
                .width(ui.available_width())
                .selected_text(f.auth.label())
                .show_ui(ui, |ui| {
                    for kind in [
                        AuthKind::Password,
                        AuthKind::ApiToken,
                        AuthKind::BearerToken,
                    ] {
                        ui.selectable_value(&mut f.auth, kind, kind.label());
                    }
                });
            if f.auth == AuthKind::Password {
                field(ui, "Логин", &mut f.username, false);
            }
            field(
                ui,
                if f.auth == AuthKind::Password {
                    "Пароль"
                } else {
                    "Токен"
                },
                &mut f.password,
                true,
            );
            if f.tenant_id.is_some() {
                ui.label(
                    RichText::new("Оставьте секрет пустым, чтобы использовать сохранённый.")
                        .size(11.0)
                        .color(muted(ui)),
                );
            }
            ui.add_space(12.0);
            ui.label(
                RichText::new("Секрет хранится локально в хранилище операционной системы.")
                    .size(12.0)
                    .color(muted(ui)),
            );
            ui.add_space(16.0);
            ui.separator();
            if self.error {
                ui.colored_label(error_color(ui), &self.notice);
            }
            ui.add_space(8.0);
            ui.horizontal(|ui| {
                if pill(ui, "Отмена", false).clicked() {
                    open = false;
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    save = ui
                        .add_enabled_ui(self.busy == 0, |ui| accent(ui, "Сохранить и подключиться"))
                        .inner
                        .clicked();
                    if self.busy > 0 {
                        ui.spinner();
                    }
                });
            });
        });
        self.connection.open = open;
        if save {
            let f = &self.connection;
            let validation = crate::api::endpoint_url(&f.url);
            if let Err(e) = validation {
                self.say(e.to_string(), true);
                return;
            }
            if f.name.trim().is_empty()
                || f.organization.trim().is_empty()
                || (f.auth == AuthKind::Password && f.username.trim().is_empty())
            {
                self.say("Заполните название, организацию и логин", true);
                return;
            }
            let endpoint = Endpoint {
                id: f.endpoint_id.unwrap_or_else(Uuid::new_v4),
                name: f.name.trim().into(),
                url: f.url.trim().into(),
                tenants: Vec::new(),
            };
            let tenant = Tenant {
                id: f.tenant_id.unwrap_or_else(Uuid::new_v4),
                organization: f.organization.trim().into(),
                username: f.username.trim().into(),
                auth: f.auth,
            };
            let password = std::mem::take(&mut self.connection.password);
            let store = self.store.clone();
            let tx = self.sender.clone();
            self.busy += 1;
            self.runtime.spawn_blocking(move || {
                let result = (|| {
                    if !password.is_empty() {
                        store.set_secret(
                            tenant.id,
                            &Secret {
                                value: password,
                                session_token: String::new(),
                            },
                        )?;
                    } else {
                        store.secret(tenant.id)?;
                    }
                    Ok::<_, anyhow::Error>((endpoint, tenant))
                })()
                .map_err(|e| format!("{e:#}"));
                let _ = tx.send(Event::Credential(result));
            });
        }
    }

    fn s3_dialog(&mut self, ctx: &egui::Context) {
        if !self.s3_form.open {
            return;
        }
        let mut open = true;
        let save = form_dialog(
            ctx,
            "s3-profile",
            520.0,
            ("Хранилище S3", "S3 и совместимые хранилища"),
            &mut open,
            |ui| {
                let f = &mut self.s3_form;
                field(ui, "Название", &mut f.name, false);
                field(ui, "Endpoint HTTPS", &mut f.endpoint, false);
                field(ui, "Регион", &mut f.region, false);
                field(ui, "Бакет", &mut f.bucket, false);
                field(ui, "Префикс / папка в бакете", &mut f.prefix, false);
                field(ui, "Access key", &mut f.access_key, false);
                field(ui, "Secret key", &mut f.secret_key, true);
                field(
                    ui,
                    "Session token (необязательно)",
                    &mut f.session_token,
                    true,
                );
                ui.add_space(10.0);
                ui.add(egui::Label::new(RichText::new("Новый секрет сохраняется вместе с session token. Пустой secret key сохраняет прежние ключи.").size(12.0).color(muted(ui))).wrap());
            },
            ("Сохранить S3", self.busy == 0),
        );
        self.s3_form.open = open;
        if save {
            let f = &self.s3_form;
            if let Err(e) = crate::api::endpoint_url(&f.endpoint) {
                self.say(e.to_string(), true);
                return;
            }
            if [
                f.name.as_str(),
                f.bucket.as_str(),
                f.region.as_str(),
                f.access_key.as_str(),
            ]
            .iter()
            .any(|s| s.trim().is_empty())
            {
                self.say("Заполните название, регион, бакет и access key", true);
                return;
            }
            let profile = S3Profile {
                id: f.id.unwrap_or_else(Uuid::new_v4),
                name: f.name.clone(),
                endpoint: f.endpoint.clone(),
                region: f.region.clone(),
                bucket: f.bucket.clone(),
                prefix: f.prefix.clone(),
                access_key: f.access_key.clone(),
            };
            let secret = Secret {
                value: std::mem::take(&mut self.s3_form.secret_key),
                session_token: std::mem::take(&mut self.s3_form.session_token),
            };
            let store = self.store.clone();
            let tx = self.sender.clone();
            self.busy += 1;
            self.runtime.spawn_blocking(move || {
                let r = (|| {
                    if secret.value.is_empty() {
                        store.secret(profile.id)?;
                    } else {
                        store.set_secret(profile.id, &secret)?;
                    }
                    Ok::<_, anyhow::Error>(profile)
                })()
                .map_err(|e| format!("{e:#}"));
                let _ = tx.send(Event::S3Saved(r));
            });
        }
    }

    fn catalog_dialog(&mut self, ctx: &egui::Context) {
        if !self.catalog_form.open {
            return;
        }
        let mut open = true;
        let mut create = false;
        dialog(ctx, "catalog", 460.0, |ui| {
            dialog_header(ui, "Новый каталог", "В выбранном тенанте", &mut open);
            ui.add_space(16.0);
            field(ui, "Название каталога", &mut self.catalog_form.name, false);
            field(ui, "Описание", &mut self.catalog_form.description, false);
            ui.add_space(12.0);
            ui.label("Каталог будет создан в выбранном тенанте и не будет опубликован.");
            create = ui
                .add_enabled(
                    self.busy == 0,
                    egui::Button::new(RichText::new("Создать каталог").color(Color32::WHITE))
                        .fill(RED),
                )
                .clicked();
        });
        self.catalog_form.open = open;
        if create {
            let Some(t) = self
                .transfer
                .as_ref()
                .map(|f| f.tenant.id)
                .or(self.selected.map(|(_, t)| t))
            else {
                return;
            };
            let Some(session) = self.sessions.get(&t).cloned() else {
                return;
            };
            let Some(inv) = self.inventories.get(&t) else {
                return;
            };
            let org = inv.org_href.clone();
            let name = self.catalog_form.name.clone();
            let description = self.catalog_form.description.clone();
            let tx = self.sender.clone();
            self.busy += 1;
            self.runtime.spawn(async move {
                let r = session
                    .create_catalog(&org, &name, &description)
                    .await
                    .map_err(|e| format!("{e:#}"));
                let _ = tx.send(Event::Catalog(t, name, r));
            });
        }
    }

    fn backup_dialog(&mut self, ctx: &egui::Context) {
        if !self.backup.open {
            return;
        }
        let mut open = true;
        let importing = self.backup.import;
        let submit = form_dialog(
            ctx,
            "connection-backup",
            520.0,
            (
                if importing {
                    "Импорт подключений"
                } else {
                    "Зашифрованный экспорт"
                },
                "Архив настроек и секретов",
            ),
            &mut open,
            |ui| {
                ui.label(RichText::new("Файл архива").size(12.0).strong());
                ui.add(egui::Label::new(self.backup.path.display().to_string()).wrap());
                ui.add_space(8.0);
                field(ui, "Пароль архива", &mut self.backup.password, true);
                if !importing {
                    field(ui, "Повторите пароль", &mut self.backup.repeat, true);
                    ui.add(egui::Label::new(RichText::new("Минимум 12 символов. В экспорт входят настройки и секреты всех подключений.").color(muted(ui))).wrap());
                } else {
                    ui.add(egui::Label::new("Подключения будут добавлены к существующим.").wrap());
                }
            },
            (
                if importing {
                    "Импортировать"
                } else {
                    "Экспортировать"
                },
                self.busy == 0,
            ),
        );
        self.backup.open = open;
        if submit {
            if !self.backup.import && self.backup.password != self.backup.repeat {
                self.say("Пароли не совпадают", true);
                return;
            }
            let password = zeroize::Zeroizing::new(std::mem::take(&mut self.backup.password));
            self.backup.repeat.clear();
            let path = self.backup.path.clone();
            let import = self.backup.import;
            let settings = self.settings.clone();
            let store = self.store.clone();
            let tx = self.sender.clone();
            self.busy += 1;
            self.runtime.spawn_blocking(move || {
                let r = if import {
                    store.import(&settings, &path, &password).map(Some)
                } else {
                    store.export(&settings, &path, &password).map(|_| None)
                }
                .map_err(|e| format!("{e:#}"));
                let _ = tx.send(Event::Backup(r));
            });
        }
    }

    fn transfer_dialog(&mut self, ctx: &egui::Context) {
        let Some(mut form) = self.transfer.take() else {
            return;
        };
        let mut open = true;
        let mut start = false;
        let mut pick = None;
        let mut shutdown = false;
        let mut validate = false;
        let download = form.direction == Direction::Download;
        let inventory = self
            .inventories
            .get(&form.tenant.id)
            .cloned()
            .unwrap_or_default();
        let mut refresh = false;
        let mut check_destination = false;
        egui::Modal::new(egui::Id::new("transfer"))
        .frame(egui::Frame::window(&ctx.style()).inner_margin(24).corner_radius(18))
        .show(ctx, |ui| {
            ui.set_width(600.0_f32.min(ctx.screen_rect().width() - 80.0));
            dialog_header(
                ui,
                if download {
                    "Скачать образ"
                } else {
                    "Загрузить в каталог"
                },
                &format!("{} / {}", form.endpoint.name, form.tenant.organization),
                &mut open,
            );
            ui.add_space(16.0);
            egui::ScrollArea::vertical().id_salt("transfer-body")
                .max_height((ctx.screen_rect().height() - 330.0).max(100.0))
                .show(ui, |ui| {
            if download {
                card(ui, |ui| {
                    ui.label(RichText::new(&form.object.name).size(17.0).strong());
                    if let Some(inspection) = &form.inspection {
                        ui.label(
                            RichText::new(format!(
                                "{} · {}",
                                form.object.kind.label(),
                                inspection
                                    .estimated_bytes
                                    .map(human_bytes)
                                    .unwrap_or_else(|| "Размер неизвестен".into())
                            ))
                            .color(muted(ui))
                            .size(12.0),
                        );
                        if !inspection.vms.is_empty() {
                            ui.add_space(4.0);
                            egui::ScrollArea::vertical()
                                .id_salt("export-vms")
                                .max_height(95.0)
                                .show(ui, |ui| {
                                    for vm in &inspection.vms {
                                        ui.horizontal(|ui| {
                                            ui.label(&vm.name);
                                            ui.with_layout(
                                                egui::Layout::right_to_left(egui::Align::Center),
                                                |ui| {
                                                    ui.colored_label(
                                                        if vm.is_off() {
                                                            success_color(ui)
                                                        } else {
                                                            warning_color(ui)
                                                        },
                                                        status_label(&vm.status),
                                                    );
                                                },
                                            );
                                        });
                                    }
                                });
                            let running = inspection.vms.iter().filter(|v| !v.is_off()).count();
                            if running > 0 {
                                ui.colored_label(
                                    warning_color(ui),
                                    "Для экспорта необходимо выключить ВМ.",
                                );
                                checkbox(ui, &mut form.power_confirm,format!("Подтверждаю Power Off перечисленных ВМ ({running}) без завершения гостевой ОС"));
                                shutdown = ui
                                    .add_enabled(
                                        form.power_confirm && self.busy == 0,
                                        egui::Button::new("Выключить и проверить состав"),
                                    )
                                    .clicked();
                            }
                        }
                    }
                });
                ui.add_space(10.0);
                if let Some(inspection) = &form.inspection {
                    let media = inspection
                        .vms
                        .iter()
                        .flat_map(|v| &v.media)
                        .collect::<Vec<_>>();
                    if !media.is_empty() {
                        ui.label(RichText::new("Подключённые ISO / Media").strong());
                        for m in &media {
                            ui.label(format!("{} · {}", m.vm_name, m.name));
                        }
                        if media.iter().all(|m| !m.href.is_empty()) {
                            checkbox(ui,
                                &mut form.eject_confirm,
                                "Извлечь перечисленные ISO из ВМ перед скачиванием",
                            );
                            ui.label(RichText::new("Это изменит подключение носителей ВМ. ISO останутся в каталоге; обратно автоматически не подключаются.").size(12.0).color(muted(ui)));
                        } else {
                            ui.colored_label(warning_color(ui), "Неизвестен адрес носителя. Извлеките его в vCD и откройте скачивание заново.");
                        }
                    }
                }
                if form.object.kind == ObjectKind::Vapp {
                    ui.add_space(6.0);
                    ui.label(RichText::new("Способ скачивания").strong());
                    ui.horizontal_wrapped(|ui| {
                        if pill(ui, "Напрямую", !form.via_template).clicked() {
                            form.via_template = false;
                        }
                        if pill(ui, "Через шаблон в каталоге", form.via_template).clicked()
                        {
                            form.via_template = true;
                        }
                    });
                    if form.via_template {
                        ui.label(RichText::new("Будет создан шаблон всего указанного vApp, затем скачан через OVF Tool. Шаблон останется в каталоге и займёт место в ЦОДе.").size(12.0).color(muted(ui)));
                        field(ui, "Имя нового шаблона", &mut form.capture_name, false);
                        egui::ComboBox::from_id_salt("capture-catalog")
                            .width(ui.available_width())
                            .selected_text(
                                inventory
                                    .objects
                                    .iter()
                                    .find(|c| c.href == form.capture_catalog)
                                    .map(|c| c.name.as_str())
                                    .unwrap_or("Выберите каталог для шаблона"),
                            )
                            .show_ui(ui, |ui| {
                                for c in inventory.objects.iter().filter(|c| {
                                    c.kind == ObjectKind::Catalog && !c.owner.is_empty()
                                }) {
                                    ui.selectable_value(
                                        &mut form.capture_catalog,
                                        c.href.clone(),
                                        &c.name,
                                    );
                                }
                            });
                        ui.horizontal(|ui| {
                            if ui.button("Обновить каталоги").clicked() {
                                refresh = true;
                            }
                            if ui.button("+ Создать каталог").clicked() {
                                self.catalog_form = CatalogForm {
                                    open: true,
                                    ..Default::default()
                                };
                            }
                        });
                    }
                }
                ui.add_space(8.0);
                ui.label(RichText::new("Куда сохранить").size(12.0).strong());
                ui.horizontal(|ui| {
                    if pill(ui, "На компьютер", form.s3.is_none()).clicked() {
                        form.s3 = None;
                    }
                    egui::ComboBox::from_id_salt("target-s3")
                        .width(210.0)
                        .selected_text(
                            form.s3
                                .and_then(|id| {
                                    self.settings.s3_profiles.iter().find(|p| p.id == id)
                                })
                                .map(|p| p.name.as_str())
                                .unwrap_or("Хранилище S3"),
                        )
                        .show_ui(ui, |ui| {
                            for p in &self.settings.s3_profiles {
                                ui.selectable_value(&mut form.s3, Some(p.id), &p.name);
                            }
                            if self.settings.s3_profiles.is_empty() {
                                ui.label("Добавьте S3 в настройках");
                            }
                        });
                });
                if form.s3.is_some() {
                    ui.label(
                        RichText::new(
                            "Сначала образ сохраняется на диск, затем отправляется в S3.",
                        )
                        .size(12.0)
                        .color(muted(ui)),
                    );
                    if form.format == "OVF" {
                        form.format = "OVA".into();
                    }
                }
                ui.add_space(4.0);
                ui.label(
                    RichText::new(if form.s3.is_some() {
                        "Промежуточная папка на диске"
                    } else {
                        "Папка скачивания"
                    })
                    .size(12.0)
                    .strong(),
                );
                ui.horizontal(|ui| {
                    let mut path = form.path.display().to_string();
                    ui.add(
                        egui::TextEdit::singleline(&mut path)
                            .interactive(false)
                            .desired_width(ui.available_width() - 120.0)
                            .margin(egui::Margin::symmetric(12, 10)),
                    );
                    if pill(ui, "Выбрать…", false).clicked() {
                        pick = Some(Pick::Download);
                    }
                });
                if form.object.kind != ObjectKind::Media {
                    ui.add_space(4.0);
                    ui.label(RichText::new("Формат образа").size(12.0).strong());
                    ui.horizontal(|ui| {
                        if pill(ui, "OVA · один файл", form.format == "OVA").clicked() {
                            form.format = "OVA".into();
                        }
                        if form.s3.is_none()
                            && pill(ui, "OVF · папка с дисками", form.format == "OVF").clicked()
                        {
                            form.format = "OVF".into();
                        }
                    });
                }
                ui.add_space(8.0);
                card(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label("Место под образ, ГиБ");
                        ui.add(
                            egui::DragValue::new(&mut form.estimate_gib)
                                .speed(1.0)
                                .range(0.0..=1_000_000.0),
                        );
                    });
                    if let Some(free) = form.free {
                        let need = (form.estimate_gib * 1024.0 * 1024.0 * 1024.0) as u64
                            + self.settings.reserve_gib * 1024 * 1024 * 1024;
                        ui.colored_label(
                            if need <= free { success_color(ui) } else { error_color(ui) },
                            format!(
                                "{} · свободно {} · резерв {} ГиБ",
                                if need <= free {
                                    "Места достаточно"
                                } else {
                                    "Не хватает места"
                                },
                                human_bytes(free),
                                self.settings.reserve_gib
                            ),
                        );
                        ui.add(
                            egui::ProgressBar::new(
                                (need as f64 / free.max(1) as f64).clamp(0.0, 1.0) as f32,
                            )
                            .fill(if need <= free { success_color(ui) } else { error_color(ui) })
                            .desired_height(5.0),
                        );
                    } else {
                        ui.colored_label(
                            warning_color(ui),
                            "Выберите папку для проверки свободного места",
                        );
                    }
                    if form
                        .inspection
                        .as_ref()
                        .is_some_and(|i| i.estimated_bytes.is_none())
                    {
                        ui.colored_label(
                            warning_color(ui),
                            "API не сообщил размер. Укажите верхнюю оценку размера образа.",
                        );
                    }
                });
            } else {
                ui.label(RichText::new("Исходный образ").size(12.0).strong());
                ui.horizontal(|ui| {
                    if pill(ui, "Выбрать OVA / OVF / ISO", false).clicked() {
                        pick = Some(Pick::Upload);
                    }
                    ui.label(
                        RichText::new(form.path.file_name().unwrap_or_default().to_string_lossy())
                            .color(muted(ui)),
                    )
                    .on_hover_text(form.path.display().to_string());
                });
                field(ui, "Имя в каталоге", &mut form.object.name, false);
                let catalogs = inventory
                    .objects
                    .iter()
                    .filter(|o| o.kind == ObjectKind::Catalog)
                    .collect::<Vec<_>>();
                if !catalogs.iter().any(|o| o.name == form.object.catalog) {
                    form.object.catalog =
                        catalogs.first().map(|o| o.name.clone()).unwrap_or_default();
                }
                ui.label(RichText::new("Каталог").size(12.0).strong());
                egui::ComboBox::from_id_salt("upload-catalog")
                    .width(ui.available_width())
                    .selected_text(if form.object.catalog.is_empty() {
                        "Выберите или создайте каталог"
                    } else {
                        &form.object.catalog
                    })
                    .show_ui(ui, |ui| {
                        for o in catalogs {
                            ui.selectable_value(&mut form.object.catalog, o.name.clone(), &o.name)
                                .on_hover_text(format!("Owner: {}", o.owner));
                        }
                    });
                ui.horizontal(|ui| {
                    refresh = ui
                        .add_enabled_ui(self.busy == 0, |ui| pill(ui, "Обновить каталоги", false))
                        .inner
                        .clicked();
                    if pill(ui, "+ Создать каталог", false).clicked() {
                        self.catalog_form = CatalogForm {
                            open: true,
                            ..Default::default()
                        };
                    }
                    if self.busy > 0 {
                        ui.spinner();
                    }
                });
                ui.label(RichText::new("Виртуальный ЦОД").size(12.0).strong());
                egui::ComboBox::from_id_salt("upload-vdc")
                    .width(ui.available_width())
                    .selected_text(&form.object.vdc)
                    .show_ui(ui, |ui| {
                        for o in inventory
                            .objects
                            .iter()
                            .filter(|o| o.kind == ObjectKind::Vdc)
                        {
                            ui.selectable_value(&mut form.object.vdc, o.name.clone(), &o.name);
                        }
                    });
                ui.add_space(6.0);
                validate = ui
                    .add_enabled_ui(
                        form.path.is_file() && self.validation_cancel.is_none(),
                        |ui| pill(ui, "Проверить файл перед загрузкой", false),
                    )
                    .inner
                    .clicked();
                if self.validate_path == form.path && self.validation.is_some() {
                    ui.colored_label(success_color(ui), "Проверка файла пройдена");
                }
                if self.validate_path == form.path && self.validation_cancel.is_some() {
                    ui.horizontal(|ui| {
                        ui.spinner();
                        ui.label("Проверяется выбранный файл…");
                    });
                }
                checkbox(ui,
                    &mut form.eulas,
                    "Принимаю лицензионные соглашения (EULA) выбранного образа",
                );
            }
            ui.add_space(8.0);
            ui.horizontal_wrapped(|ui| {
                ui.label("Лимит скорости, МиБ/с");
                ui.add(egui::DragValue::new(&mut form.limit).range(0..=100000));
                ui.label(
                    RichText::new("0 — без ограничения")
                        .size(12.0)
                        .color(muted(ui)),
                );
            });
            if form.object.kind != ObjectKind::Media {
                ui.horizontal_wrapped(|ui| {
                    ui.label("Потоки OVF Tool");
                    ui.add(egui::DragValue::new(&mut form.parallel_threads).range(1..=16));
                    ui.label(RichText::new("4 по умолчанию · 1 для совместимости").size(12.0).color(muted(ui)));
                }).response.on_hover_text("Параллельная передача доступных дисков. Ускорение зависит от OVF Tool, числа дисков и сервера; один файл может остаться одним потоком.");
            }
            if !form.error.is_empty() {
                ui.colored_label(error_color(ui), &form.error);
            }
            if self.error && self.validate_path == form.path && self.validation_cancel.is_none() {
                ui.colored_label(error_color(ui), &self.notice);
            }
            if form.destination_key != form.destination_fingerprint() {
                form.reset_destination();
            }
            if !download || form.via_template {
                ui.add_space(10.0);
                card(ui, |ui| {
                    ui.label(RichText::new("Место и имя в Cloud Director").strong());
                    ui.label(RichText::new("Проверим каталог, дисковую политику, свободную квоту и совпадение имени.").color(muted(ui)));
                    check_destination = ui.add_enabled(!form.destination_busy, egui::Button::new("Проверить назначение")).clicked();
                    if form.destination_busy {
                        ui.label("Проверяется файл и назначение…");
                        if ui.button("Отменить проверку").clicked() { form.reset_destination(); }
                    }
                    if let Some(preview) = &form.destination_preview {
                        ui.label(&preview.summary);
                        if preview.capacity_verified {
                            ui.colored_label(success_color(ui), "Предварительная проверка пройдена");
                        } else {
                            ui.colored_label(warning_color(ui), "Cloud Director не подтвердил доступную ёмкость. Успех загрузки не гарантирован.");
                            checkbox(ui, &mut form.allow_unknown_capacity, "Продолжить при неизвестной квоте; сервер может отказать");
                        }
                        ui.label(RichText::new("Перед выполнением задания данные проверяются повторно.").size(12.0).color(muted(ui)));
                    }
                    if !form.destination_error.is_empty() { ui.colored_label(error_color(ui), &form.destination_error); }
                });
            }
            });
            let off = form
                .inspection
                .as_ref()
                .is_none_or(|i| i.vms.iter().all(VmState::is_off));
            let has_space = !download
                || (form.estimate_gib > 0.0
                    && form.path.is_dir()
                    && form.free.is_some_and(|free| {
                        free >= ((form.estimate_gib * 1024.0 * 1024.0 * 1024.0) as u64)
                            .saturating_add(
                                self.settings.reserve_gib.saturating_mul(1024 * 1024 * 1024),
                            )
                    }));
            ui.add_space(8.0);
            ui.separator();
            ui.add_space(8.0);
            let destination_ready = (download && !form.via_template) || form.destination_preview.as_ref().is_some_and(|p| p.capacity_verified || form.allow_unknown_capacity);
            let iso_ready = form.inspection.as_ref().is_none_or(|i| i.vms.iter().flat_map(|v| &v.media).all(|m| form.eject_confirm && !m.href.is_empty()));
            let disabled_reason = if !off { "Выключите ВМ и повторно проверьте её состояние" }
                else if !iso_ready { "Подтвердите отключение подключённого ISO" }
                else if self.tool_version.is_empty() { "Укажите папку работающего OVF Tool" }
                else if !has_space { "Проверьте папку и свободное место на компьютере" }
                else if !destination_ready { "Проверьте назначение в Cloud Director и устраните замечания" }
                else if self.busy > 0 { "Дождитесь завершения текущей проверки" }
                else { "" };
            if !disabled_reason.is_empty() { ui.colored_label(warning_color(ui), disabled_reason); }
            ui.horizontal(|ui| {
                if pill(ui, "Отмена", false).clicked() {
                    open = false;
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    start = ui
                        .add_enabled_ui(
                            destination_ready && off && self.busy == 0
                                && (!form.via_template
                                    || (!form.capture_name.trim().is_empty()
                                        && inventory.objects.iter().any(|c| {
                                            c.kind == ObjectKind::Catalog
                                                && c.href == form.capture_catalog
                                        })))
                                && form.inspection.as_ref().is_none_or(|i| {
                                    i.vms
                                        .iter()
                                        .flat_map(|v| &v.media)
                                        .all(|m| form.eject_confirm && !m.href.is_empty())
                                })
                                && !self.tool_version.is_empty()
                                && has_space
                                && (download || !form.object.catalog.is_empty()),
                            |ui| {
                                accent(
                                    ui,
                                    if download {
                                        "Начать скачивание"
                                    } else {
                                        "Начать загрузку"
                                    },
                                )
                            },
                        )
                        .inner
                        .clicked();
                    if self.busy > 0 {
                        ui.spinner();
                    }
                });
            });
        });
        if refresh {
            form.error.clear();
            form.reset_destination();
            self.refresh_catalogs(form.tenant.id, None);
        }
        if !open {
            form.reset_destination();
            return;
        }
        if check_destination {
            form.reset_destination();
            let session = self.sessions.get(&form.tenant.id).cloned();
            let catalog = if form.via_template {
                form.capture_catalog.clone()
            } else {
                inventory
                    .objects
                    .iter()
                    .find(|c| c.kind == ObjectKind::Catalog && c.name == form.object.catalog)
                    .map(|c| c.href.clone())
                    .unwrap_or_default()
            };
            let name = if form.via_template {
                form.capture_name.clone()
            } else {
                form.object.name.clone()
            };
            if catalog.is_empty() || name.trim().is_empty() || form.object.vdc.is_empty() {
                form.destination_error =
                    "Выберите каталог, виртуальный ЦОД и введите имя образа".into();
            } else if !download && !form.path.is_file() {
                form.destination_error = "Выберите существующий файл OVA, OVF или ISO".into();
            } else if let Some(session) = session {
                let key = form.destination_key.clone();
                let id = form.id;
                let path = form.path.clone();
                let vdc = form.object.vdc.clone();
                let required = form
                    .inspection
                    .as_ref()
                    .and_then(|i| i.estimated_bytes)
                    .unwrap_or(0);
                let cancel = CancellationToken::new();
                form.destination_cancel = Some(cancel.clone());
                form.destination_busy = true;
                let tx = self.sender.clone();
                self.runtime.spawn(async move {
                    let result = async {
                        let required = if download {
                            required
                        } else {
                            let validation_cancel = cancel.clone();
                            tokio::task::spawn_blocking(move || {
                                ovf::validate_package(&path, &validation_cancel)
                            })
                            .await??
                            .provisioned_bytes
                        };
                        anyhow::ensure!(!cancel.is_cancelled(), "Проверка отменена");
                        let report = session
                            .catalog_preflight(&catalog, &name, &vdc, required)
                            .await?;
                        report.check_capacity()?;
                        Ok::<_, anyhow::Error>(DestinationPreview {
                            summary: report.summary(),
                            capacity_verified: report.capacity_verified,
                        })
                    }
                    .await
                    .map_err(|e| format!("{e:#}"));
                    let _ = tx.send(Event::DestinationCheck(id, key, result));
                });
            } else {
                form.destination_error = "Войдите в выбранный тенант заново".into();
            }
        }
        if shutdown
            && let Some(session) = self.sessions.get(&form.tenant.id).cloned()
            && let Some(inspection) = form.inspection.clone()
        {
            let id = form.tenant.id;
            let tx = self.sender.clone();
            self.busy += 1;
            self.runtime.spawn(async move{let r=async{
            let current=session.inspect_export(&inspection.object).await?;
            let allowed=inspection.vms.iter().filter(|v|!v.is_off()).map(|v|v.href.as_str()).collect::<Vec<_>>();
            anyhow::ensure!(current.vms.len()==inspection.vms.len()&&current.vms.iter().all(|v|inspection.vms.iter().any(|a|a.href==v.href))&&current.vms.iter().filter(|v|!v.is_off()).all(|v|allowed.contains(&v.href.as_str())),"Состав или состояние vApp изменились. Откройте скачивание заново и проверьте список.");
            for vm in current.vms.iter().filter(|v|!v.is_off()){session.power_off_vm(&vm.href).await?;}
            session.inspect_export(&inspection.object).await
        }.await.map_err(|e|format!("{e:#}"));let _=tx.send(Event::Inspection(id,r));});
        }
        if validate {
            self.validate_path = form.path.clone();
            self.page = Page::Validate;
            self.start_validation();
        }
        if start {
            let valid = if download {
                if form.estimate_gib <= 0.0 {
                    Err("Укажите размер образа для проверки места".into())
                } else {
                    ovf::check_destination(
                        &form.path,
                        (form.estimate_gib * 1024.0 * 1024.0 * 1024.0) as u64,
                        self.settings.reserve_gib * 1024 * 1024 * 1024,
                    )
                    .map(|_| ())
                    .map_err(|e| e.to_string())
                }
            } else if !form.path.is_file()
                || form.object.catalog.is_empty()
                || form.object.vdc.is_empty()
                || form.object.name.trim().is_empty()
            {
                Err("Выберите файл, каталог, виртуальный ЦОД и имя".into())
            } else {
                Ok(())
            };
            if let Err(e) = valid {
                form.error = e;
            } else {
                let size = if download {
                    (form.estimate_gib * 1024.0 * 1024.0 * 1024.0) as u64
                } else {
                    std::fs::metadata(&form.path).map(|m| m.len()).unwrap_or(0)
                };
                let req = TransferRequest {
                    endpoint: form.endpoint.clone(),
                    tenant: form.tenant.clone(),
                    object: form.object.clone(),
                    direction: form.direction,
                    local_path: form.path.clone(),
                    format: form.format.clone(),
                    s3: form.s3.and_then(|id| {
                        self.settings
                            .s3_profiles
                            .iter()
                            .find(|p| p.id == id)
                            .cloned()
                    }),
                    limit_mib: form.limit,
                    estimated_bytes: size,
                    reserve_bytes: self.settings.reserve_gib * 1024 * 1024 * 1024,
                    ovf_directory: self.settings.ovf_directory.clone(),
                    accept_eulas: form.eulas,
                    parallel_threads: form.parallel_threads,
                    allow_unknown_capacity: form.allow_unknown_capacity,
                    capture: if form.via_template {
                        inventory
                            .objects
                            .iter()
                            .find(|c| c.href == form.capture_catalog)
                            .map(|c| CaptureTarget {
                                catalog_href: c.href.clone(),
                                catalog_name: c.name.clone(),
                                name: form.capture_name.trim().into(),
                            })
                    } else {
                        None
                    },
                    expected_vms: form
                        .inspection
                        .as_ref()
                        .map(|i| i.vms.iter().map(|v| v.href.clone()).collect())
                        .unwrap_or_default(),
                    eject_media: if form.eject_confirm {
                        form.inspection
                            .as_ref()
                            .map(|i| i.vms.iter().flat_map(|v| v.media.clone()).collect())
                            .unwrap_or_default()
                    } else {
                        Vec::new()
                    },
                };
                if self.jobs.active_count() > 0 {
                    self.queue_choice = Some(req);
                } else {
                    self.enqueue_transfer(req, false);
                    self.show_transfers = true;
                }
                return;
            }
        }
        self.transfer = Some(form);
        if let Some(pick) = pick {
            self.pick(pick);
        }
    }

    fn other_dialogs(&mut self, ctx: &egui::Context) {
        if let Some(message) = self.export_error.clone() {
            let mut open = true;
            dialog(ctx, "export-inspection-error", 560.0, |ui| {
                dialog_header(
                    ui,
                    "Скачивание недоступно",
                    "Проверка выбранной ВМ / vApp",
                    &mut open,
                );
                ui.add_space(12.0);
                ui.label(&message);
                ui.add_space(16.0);
                ui.horizontal(|ui| {
                    if ui.button("Копировать ошибку").clicked() {
                        ui.ctx().copy_text(message.clone());
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if accent(ui, "Понятно").clicked() {
                            open = false;
                        }
                    });
                });
            });
            if !open {
                self.export_error = None;
            }
        }
        if self.setup {
            let mut choose = false;
            egui::Window::new("Добро пожаловать в OVF UI").collapsible(false).resizable(false).default_width(500.0).anchor(egui::Align2::CENTER_CENTER,[0.0,0.0]).show(ctx,|ui|{ui.add_space(8.0);ui.label(RichText::new("Подключите OVF Tool").size(25.0).strong());ui.add_space(12.0);ui.label("Выберите основную папку установленного VMware OVF Tool.");ui.label("Путь сохранится; его можно изменить в настройках.");ui.add_space(18.0);choose=accent(ui,"Выбрать папку OVF Tool").clicked();ui.add_space(8.0);ui.hyperlink_to("Официальная страница OVF Tool","https://developer.broadcom.com/tools/open-virtualization-format-ovf-tool/latest");if !self.notice.is_empty()&&self.error{ui.colored_label(error_color(ui),&self.notice);}ui.add_space(8.0);if ui.button("Настроить подключения сначала").clicked(){self.setup=false;}});
            if choose {
                self.pick(Pick::Tool);
            }
        }
        if self.queue_choice.is_some() {
            let action = queue_dialog(
                ctx,
                &self.queue_choice.as_ref().unwrap().object.name,
                self.jobs.active_count(),
            );
            if let Some(action) = action {
                let req = self.queue_choice.take().unwrap();
                if action < 2 {
                    self.enqueue_transfer(req, action == 1);
                    self.show_transfers = true;
                }
            }
        }
        if self.show_logs {
            egui::Window::new("Журнал OVF UI")
                .open(&mut self.show_logs)
                .default_size([850.0, 520.0])
                .show(ctx, |ui| {
                    if ui.button("Копировать журнал").clicked() {
                        ui.ctx().copy_text(self.log_text.clone());
                    }
                    egui::ScrollArea::both().show(ui, |ui| {
                        ui.add(
                            egui::TextEdit::multiline(&mut self.log_text)
                                .font(egui::TextStyle::Monospace)
                                .desired_width(f32::INFINITY)
                                .interactive(false),
                        );
                    });
                });
        }
        if self.close_confirm {
            egui::Window::new("Завершить работу?")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0])
                .show(ctx, |ui| {
                    ui.label(
                        "Есть активные передачи или проверки. Неполные файлы останутся на диске.",
                    );
                    ui.add_space(12.0);
                    ui.horizontal(|ui| {
                        if ui.button("Продолжить работу").clicked() {
                            self.close_confirm = false;
                        }
                        if accent(ui, "Отменить операции и выйти").clicked() {
                            self.jobs.cancel_all();
                            if let Some(c) = &self.validation_cancel {
                                c.cancel();
                            }
                            self.closing = true;
                            self.close_confirm = false;
                        }
                    });
                });
        }
    }

    fn transfers_window(&mut self, ctx: &egui::Context) {
        if !self.show_transfers {
            return;
        }
        let mut open = true;
        let manager = self.jobs.clone();
        let mut notice = None;
        let mut clear_finished = false;
        ctx.show_viewport_immediate(
            egui::ViewportId::from_hash_of("transfers"),
            egui::ViewportBuilder::default()
                .with_title("Передачи — OVF UI")
                .with_inner_size([820.0, 620.0])
                .with_min_inner_size([640.0, 420.0]),
            |ctx, class| {
                if ctx.input(|i| i.viewport().close_requested()) {
                    open = false;
                }
                let content = |ui: &mut egui::Ui| {
                    ui.horizontal(|ui| {
                        ui.heading("Передачи");
                        ui.label(
                            RichText::new(format!("{} активных", manager.active_count()))
                                .color(muted(ui)),
                        );
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            if ui.button("Очистить завершённые").clicked() {
                                clear_finished = true;
                            }
                        });
                    });
                    ui.add_space(16.0);
                    egui::ScrollArea::vertical().show(ui, |ui| {
                        let jobs = manager.snapshots();
                        if jobs.is_empty() {
                            ui.add_space(40.0);
                            ui.label("Здесь появятся ваши загрузки и скачивания.");
                        }
                        for job in jobs.iter().rev() {
                            transfer_card(ui, job, &manager, &mut notice);
                            ui.add_space(12.0);
                        }
                    });
                };
                if class == egui::ViewportClass::Embedded {
                    egui::Window::new("Передачи")
                        .default_width(760.0)
                        .show(ctx, content);
                } else {
                    egui::CentralPanel::default()
                        .frame(
                            egui::Frame::new()
                                .fill(ctx.style().visuals.panel_fill)
                                .inner_margin(24),
                        )
                        .show(ctx, content);
                }
                ctx.request_repaint_after(Duration::from_millis(500));
            },
        );
        if clear_finished {
            for job in manager.clear_finished() {
                self.observe_transfer_status(&job);
            }
            self.refresh_completed_transfers(true);
        }
        self.show_transfers = open;
        if let Some(n) = notice {
            self.say(n, true);
        }
    }
}

impl eframe::App for DockApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.handle_events();
        self.refresh_completed_transfers(false);
        if ctx.input(|i| i.viewport().close_requested())
            && !self.closing
            && (self.jobs.pending_count() > 0 || self.busy > 0)
        {
            ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            self.close_confirm = true;
        }
        if self.closing && self.jobs.active_count() == 0 && self.validation_cancel.is_none() {
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        }
        self.sidebar(ctx);
        egui::TopBottomPanel::bottom("status")
            .frame(
                egui::Frame::new()
                    .fill(ctx.style().visuals.panel_fill)
                    .inner_margin(egui::Margin::symmetric(20, 9)),
            )
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.colored_label(
                        if self.error {
                            error_color(ui)
                        } else {
                            muted(ui)
                        },
                        if self.notice.is_empty() {
                            "Готово к работе"
                        } else {
                            &self.notice
                        },
                    );
                });
            });
        egui::CentralPanel::default()
            .frame(
                egui::Frame::new()
                    .fill(ctx.style().visuals.panel_fill)
                    .inner_margin(28),
            )
            .show(ctx, |ui| {
                self.header(ui);
                match self.page {
                    Page::Machines | Page::Catalogs => self.inventory_page(ui),
                    Page::Validate => self.validation_page(ui),
                    Page::Settings => self.settings_page(ui),
                }
            });
        self.connection_dialog(ctx);
        self.s3_dialog(ctx);
        self.backup_dialog(ctx);
        self.transfer_dialog(ctx);
        self.catalog_dialog(ctx);
        self.other_dialogs(ctx);
        self.transfers_window(ctx);
        ctx.request_repaint_after(Duration::from_millis(
            if self.jobs.active_count() > 0 || self.busy > 0 {
                250
            } else {
                1000
            },
        ));
    }
}

fn apply_theme(ctx: &egui::Context, light: bool) {
    let mut visuals = if light {
        egui::Visuals::light()
    } else {
        egui::Visuals::dark()
    };
    visuals.panel_fill = if light {
        Color32::from_rgb(250, 250, 252)
    } else {
        Color32::from_rgb(13, 14, 18)
    };
    visuals.window_fill = if light {
        Color32::WHITE
    } else {
        Color32::from_rgb(25, 27, 33)
    };
    visuals.extreme_bg_color = if light {
        Color32::from_rgb(240, 241, 245)
    } else {
        Color32::from_rgb(17, 18, 23)
    };
    visuals.faint_bg_color = if light {
        Color32::WHITE
    } else {
        Color32::from_rgb(24, 26, 32)
    };
    visuals.selection.bg_fill = RED.gamma_multiply(0.24);
    visuals.selection.stroke = egui::Stroke::new(
        1.0_f32,
        if light {
            Color32::BLACK
        } else {
            Color32::WHITE
        },
    );
    visuals.widgets.active.bg_fill = RED;
    visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0_f32, RED);
    visuals.widgets.active.bg_stroke = egui::Stroke::new(1.0_f32, RED);
    visuals.widgets.noninteractive.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    visuals.widgets.open.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    visuals.widgets.inactive.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    visuals.widgets.hovered.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    visuals.widgets.active.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    visuals.widgets.hovered.bg_fill = if light {
        Color32::from_rgb(233, 234, 240)
    } else {
        Color32::from_rgb(45, 47, 57)
    };
    visuals.widgets.inactive.bg_fill = if light {
        Color32::from_rgb(236, 237, 242)
    } else {
        Color32::from_rgb(32, 34, 42)
    };
    visuals.widgets.noninteractive.bg_stroke = egui::Stroke::new(
        1.0_f32,
        if light {
            Color32::from_rgb(180, 188, 202)
        } else {
            Color32::from_rgb(43, 45, 53)
        },
    );
    visuals.hyperlink_color = if light {
        RED
    } else {
        Color32::from_rgb(255, 112, 127)
    };
    visuals.widgets.inactive.weak_bg_fill = visuals.widgets.inactive.bg_fill;
    visuals.widgets.hovered.weak_bg_fill = visuals.widgets.hovered.bg_fill;
    visuals.widgets.active.weak_bg_fill = RED;
    let foreground = if light {
        Color32::from_rgb(35, 37, 44)
    } else {
        Color32::from_rgb(229, 231, 237)
    };
    visuals.widgets.noninteractive.fg_stroke.color = foreground;
    visuals.widgets.inactive.fg_stroke.color = foreground;
    visuals.widgets.hovered.fg_stroke.color = foreground;
    // egui also uses the active foreground for every RichText::strong label.
    visuals.widgets.active.fg_stroke.color = foreground;
    visuals.widgets.active.bg_fill = visuals.widgets.hovered.bg_fill;
    visuals.widgets.active.weak_bg_fill = visuals.widgets.hovered.bg_fill;
    visuals.widgets.open.fg_stroke.color = foreground;
    visuals.widgets.inactive.bg_stroke = visuals.widgets.noninteractive.bg_stroke;
    ctx.set_visuals(visuals);
    ctx.style_mut(|s| {
        s.spacing.item_spacing = Vec2::new(10.0, 8.0);
        s.spacing.button_padding = Vec2::new(12.0, 8.0);
        s.spacing.interact_size.y = CONTROL_HEIGHT;
        s.spacing.text_edit_width = 240.0;
        s.spacing.scroll.bar_width = 6.0;
        s.spacing.scroll.floating_width = 3.0;
        s.text_styles
            .insert(egui::TextStyle::Body, egui::FontId::proportional(14.0));
        s.text_styles
            .insert(egui::TextStyle::Button, egui::FontId::proportional(13.0));
    });
}
fn accent(ui: &mut egui::Ui, label: &str) -> egui::Response {
    ui.add(
        egui::Button::new(RichText::new(label).color(Color32::WHITE))
            .fill(RED)
            .corner_radius(CONTROL_RADIUS)
            .min_size(Vec2::new(0.0, CONTROL_HEIGHT)),
    )
}
fn nav(ui: &mut egui::Ui, label: &str, selected: bool) -> bool {
    pill_sized(
        ui,
        label,
        selected,
        Vec2::new(ui.available_width(), CONTROL_HEIGHT),
    )
    .clicked()
}
fn pill(ui: &mut egui::Ui, label: &str, selected: bool) -> egui::Response {
    let galley = ui.painter().layout_no_wrap(
        label.into(),
        egui::FontId::proportional(13.0),
        ui.visuals().text_color(),
    );
    pill_sized(ui, label, selected, Vec2::new(galley.size().x + 28.0, 36.0))
}
fn pill_sized(ui: &mut egui::Ui, label: &str, selected: bool, size: Vec2) -> egui::Response {
    let (rect, response) = ui.allocate_exact_size(size, egui::Sense::click());
    let border = if selected || response.hovered() || response.has_focus() {
        RED
    } else {
        ui.visuals().widgets.noninteractive.bg_stroke.color
    };
    ui.painter().rect(
        rect,
        egui::CornerRadius::same(CONTROL_RADIUS),
        if selected { RED } else { Color32::TRANSPARENT },
        egui::Stroke::new(1.0_f32, border),
        egui::StrokeKind::Inside,
    );
    ui.painter().text(
        rect.center(),
        egui::Align2::CENTER_CENTER,
        label,
        egui::FontId::proportional(13.0),
        if selected {
            Color32::WHITE
        } else {
            ui.visuals().text_color()
        },
    );
    response.widget_info(|| {
        egui::WidgetInfo::selected(
            egui::WidgetType::SelectableLabel,
            ui.is_enabled(),
            selected,
            label,
        )
    });
    response.on_hover_cursor(egui::CursorIcon::PointingHand)
}
fn short_version(version: &str) -> &str {
    version
        .split_whitespace()
        .find(|s| s.chars().next().is_some_and(|c| c.is_ascii_digit()))
        .unwrap_or("—")
}
fn checkbox(
    ui: &mut egui::Ui,
    checked: &mut bool,
    label: impl Into<egui::WidgetText>,
) -> egui::Response {
    ui.scope(|ui| {
        let visuals = ui.visuals_mut();
        visuals.widgets.inactive.corner_radius = egui::CornerRadius::same(4);
        visuals.widgets.hovered.corner_radius = egui::CornerRadius::same(4);
        visuals.widgets.active.corner_radius = egui::CornerRadius::same(4);
        visuals.widgets.noninteractive.corner_radius = egui::CornerRadius::same(4);
        ui.checkbox(checked, label)
    })
    .inner
}
fn form_dialog(
    ctx: &egui::Context,
    id: &str,
    width: f32,
    headings: (&str, &str),
    open: &mut bool,
    body: impl FnOnce(&mut egui::Ui),
    action: (&str, bool),
) -> bool {
    let mut submit = false;
    egui::Modal::new(egui::Id::new(id))
        .frame(
            egui::Frame::window(&ctx.style())
                .inner_margin(24)
                .corner_radius(18),
        )
        .show(ctx, |ui| {
            ui.set_width(width.min(ctx.screen_rect().width() - 80.0));
            dialog_header(ui, headings.0, headings.1, open);
            ui.add_space(16.0);
            egui::ScrollArea::vertical()
                .id_salt((id, "body"))
                .max_height((ctx.screen_rect().height() - 280.0).max(100.0))
                .show(ui, body);
            ui.add_space(12.0);
            ui.separator();
            ui.add_space(8.0);
            if !action.1 {
                ui.label(
                    RichText::new("Дождитесь завершения текущей операции")
                        .size(12.0)
                        .color(muted(ui)),
                );
            }
            ui.horizontal(|ui| {
                if pill(ui, "Отмена", false).clicked() {
                    *open = false;
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    submit = ui
                        .add_enabled_ui(action.1, |ui| accent(ui, action.0))
                        .inner
                        .clicked();
                });
            });
        });
    submit && *open
}

fn dialog(ctx: &egui::Context, id: &str, width: f32, content: impl FnOnce(&mut egui::Ui)) {
    egui::Modal::new(egui::Id::new(id))
        .frame(
            egui::Frame::window(&ctx.style())
                .inner_margin(24)
                .corner_radius(18),
        )
        .show(ctx, |ui| {
            ui.set_width(width.min(ctx.screen_rect().width() - 80.0));
            ui.set_max_height(ctx.screen_rect().height() - 96.0);
            egui::ScrollArea::vertical()
                .max_height(ctx.screen_rect().height() - 96.0)
                .show(ui, content);
        });
}
fn dialog_header(ui: &mut egui::Ui, title: &str, subtitle: &str, open: &mut bool) {
    ui.horizontal(|ui| {
        ui.vertical(|ui| {
            ui.label(
                RichText::new(title)
                    .size(23.0)
                    .strong()
                    .color(error_color(ui)),
            );
            ui.label(RichText::new(subtitle).size(12.0).color(muted(ui)));
        });
        ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
            if ui
                .add(
                    egui::Button::new(RichText::new("×").size(21.0))
                        .min_size(Vec2::splat(CONTROL_HEIGHT))
                        .corner_radius(CONTROL_RADIUS)
                        .stroke(ui.visuals().widgets.noninteractive.bg_stroke),
                )
                .on_hover_text("Закрыть")
                .clicked()
            {
                *open = false;
            }
        });
    });
}
fn card<R>(ui: &mut egui::Ui, content: impl FnOnce(&mut egui::Ui) -> R) -> R {
    egui::Frame::new()
        .fill(ui.visuals().faint_bg_color)
        .stroke(ui.visuals().widgets.noninteractive.bg_stroke)
        .corner_radius(SURFACE_RADIUS)
        .inner_margin(18)
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            content(ui)
        })
        .inner
}
fn catalog_grid(ui: &mut egui::Ui, objects: &[CloudObject], selected: &mut String) {
    let catalogs = objects
        .iter()
        .filter(|o| o.kind == ObjectKind::Catalog && !o.owner.is_empty())
        .collect::<Vec<_>>();
    if catalogs.is_empty() {
        ui.label("Каталогов с владельцем пока нет. Создайте новый каталог.");
        return;
    }
    let gap = 10.0;
    let columns = ((ui.available_width() + gap) / 220.0).floor().max(1.0) as usize;
    let width = ((ui.available_width() - gap * (columns - 1) as f32) / columns as f32).max(100.0);
    egui::ScrollArea::vertical()
        .id_salt("owned-catalogs")
        .max_height(184.0)
        .auto_shrink([false, true])
        .show(ui, |ui| {
            for row in catalogs.chunks(columns) {
                ui.horizontal(|ui| {
                    for c in row {
                        let active = *selected == c.name;
                        let frame = egui::Frame::new()
                            .fill(ui.visuals().faint_bg_color)
                            .stroke(egui::Stroke::new(
                                1.0_f32,
                                if active {
                                    RED
                                } else {
                                    ui.visuals().widgets.noninteractive.bg_stroke.color
                                },
                            ))
                            .corner_radius(SURFACE_RADIUS)
                            .inner_margin(12);
                        let response = ui
                            .allocate_ui_with_layout(
                                Vec2::new(width, 76.0),
                                egui::Layout::top_down(egui::Align::LEFT),
                                |ui| {
                                    frame
                                        .show(ui, |ui| {
                                            ui.set_width(width - 24.0);
                                            ui.set_min_height(48.0);
                                            ui.add(
                                                egui::Label::new(
                                                    RichText::new(&c.name).strong().color(
                                                        if active {
                                                            error_color(ui)
                                                        } else {
                                                            ui.visuals().text_color()
                                                        },
                                                    ),
                                                )
                                                .truncate(),
                                            );
                                            ui.add(
                                                egui::Label::new(
                                                    RichText::new(format!("Owner: {}", c.owner))
                                                        .size(12.0)
                                                        .color(muted(ui)),
                                                )
                                                .truncate(),
                                            );
                                        })
                                        .response
                                        .interact(egui::Sense::click())
                                },
                            )
                            .inner;
                        if response.hovered() || response.has_focus() {
                            ui.painter().rect_stroke(
                                response.rect,
                                SURFACE_RADIUS,
                                egui::Stroke::new(1.0_f32, RED),
                                egui::StrokeKind::Inside,
                            );
                        }
                        if response.clicked() {
                            *selected = if active {
                                String::new()
                            } else {
                                c.name.clone()
                            };
                        }
                        response
                            .on_hover_text(format!("{}\nOwner: {}", c.name, c.owner))
                            .on_hover_cursor(egui::CursorIcon::PointingHand);
                    }
                });
            }
        });
}
fn stat(ui: &mut egui::Ui, title: &str, value: &str, subtitle: &str) {
    card(ui, |ui| {
        ui.label(RichText::new(title).size(10.0).color(muted(ui)));
        ui.add_space(5.0);
        ui.label(RichText::new(value).size(30.0).strong());
        ui.label(RichText::new(subtitle).size(11.0).color(muted(ui)));
    });
}
fn field(ui: &mut egui::Ui, label: &str, value: &mut String, password: bool) {
    ui.label(RichText::new(label).size(12.0).strong());
    ui.add(
        egui::TextEdit::singleline(value)
            .password(password)
            .margin(egui::Margin::symmetric(12, 10))
            .desired_width(f32::INFINITY),
    );
}
fn logo(ui: &mut egui::Ui) {
    let (r, _) = ui.allocate_exact_size(Vec2::new(36.0, 40.0), egui::Sense::hover());
    let p = ui.painter();
    p.rect_filled(
        egui::Rect::from_min_size(r.min + Vec2::new(0.0, 4.0), Vec2::new(23.0, 23.0)),
        5.0,
        RED,
    );
    p.rect_stroke(
        egui::Rect::from_min_size(r.min + Vec2::new(10.0, 15.0), Vec2::new(23.0, 23.0)),
        5.0,
        egui::Stroke::new(2.0_f32, RED),
        egui::StrokeKind::Inside,
    );
}
fn status_label(status: &str) -> &str {
    match status {
        "4" | "POWERED_ON" => "Включена",
        "8" | "POWERED_OFF" => "Выключена",
        "3" | "SUSPENDED" => "Приостановлена",
        "RESOLVED" => "Готово",
        "0" | "UNRESOLVED" => "Не готово",
        "unknown" => "Неизвестное состояние",
        _ => status,
    }
}
fn transfer_card(
    ui: &mut egui::Ui,
    job: &JobSnapshot,
    manager: &JobManager,
    notice: &mut Option<String>,
) {
    card(ui, |ui| {
        ui.horizontal(|ui| {
            ui.label(RichText::new(&job.name).size(17.0).strong());
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.colored_label(
                    match job.status {
                        JobStatus::Completed => success_color(ui),
                        JobStatus::Failed => error_color(ui),
                        _ => muted(ui),
                    },
                    job.status.label(),
                );
            });
        });
        ui.label(
            RichText::new(format!(
                "{}  ·  {}",
                if job.upload {
                    "Загрузка в каталог"
                } else {
                    "Скачивание"
                },
                job.created
            ))
            .size(11.0)
            .color(muted(ui)),
        );
        ui.add_space(8.0);
        if job.status.active() || job.status == JobStatus::Completed {
            ui.add(egui::ProgressBar::new(job.progress).fill(RED).text(
                if job.progress > 0.0 && job.progress < 0.01 {
                    format!("{:.1}%", job.progress * 100.0)
                } else {
                    format!("{:.0}%", job.progress * 100.0)
                },
            ));
        }
        if job.status.active() && !job.phase.is_empty() {
            ui.label(
                RichText::new(match job.phase_progress {
                    Some(p) => format!("{} · {:.0}%", job.phase, p * 100.0),
                    None => job.phase.clone(),
                })
                .size(13.0)
                .strong(),
            );
        }
        if !job.current_file.is_empty() {
            ui.label(
                RichText::new(format!("Файл: {}", job.current_file))
                    .size(12.0)
                    .color(muted(ui)),
            );
        }
        ui.label(RichText::new(&job.destination).size(12.0));
        ui.horizontal_wrapped(|ui| {
            ui.label(format!("Передано: {}", human_bytes(job.transferred)));
            ui.label(format!("Сеть: {}/с", human_bytes(job.network_bps)));
            ui.label(format!(
                "Диск: {}",
                job.write_bps
                    .map(|b| format!("{}/с", human_bytes(b)))
                    .unwrap_or_else(|| "—".into())
            ))
            .on_hover_text("Скорость записи OVF Tool по счётчику ОС; учитывает файловый кэш.");
            ui.label(format!("{}:{:02}", job.elapsed / 60, job.elapsed % 60));
        });
        if !job.message.is_empty() {
            ui.label(
                RichText::new(&job.message)
                    .color(if job.status == JobStatus::Failed {
                        error_color(ui)
                    } else {
                        muted(ui)
                    })
                    .size(12.0),
            );
        }
        ui.horizontal(|ui| {
            if job.status.active() || job.status == JobStatus::Queued {
                if ui.button("Отменить").clicked() {
                    manager.cancel(job.id);
                }
                let mut limit = job.limit_mib;
                ui.label("МиБ/с:");
                if ui
                    .add(egui::DragValue::new(&mut limit).range(0..=100000))
                    .changed()
                {
                    manager.set_limit(job.id, limit);
                }
            } else if matches!(
                job.status,
                JobStatus::Failed | JobStatus::Cancelled | JobStatus::Interrupted
            ) && ui.button("Повторить в очереди").clicked()
                && let Err(e) = manager.retry(job.id, false)
            {
                *notice = Some(e.to_string());
            }
            if ui.button("Копировать путь").clicked() {
                ui.ctx().copy_text(job.destination.clone());
            }
        });
        egui::CollapsingHeader::new("Журнал передачи")
            .id_salt(job.id)
            .show(ui, |ui| {
                for line in &job.log {
                    if line.trim_start().starts_with('+')
                        || matches!(line.as_str(), "PROGRESS" | "TASK" | "RESULT" | "TARGET_ID")
                    {
                        continue;
                    }
                    ui.label(RichText::new(line).monospace().size(11.0));
                }
            });
    });
}
fn transfer_choice(
    ui: &mut egui::Ui,
    title: &str,
    description: &str,
    primary: bool,
) -> egui::Response {
    let response = egui::Frame::new()
        .fill(if primary {
            RED.gamma_multiply(0.10)
        } else {
            ui.visuals().faint_bg_color
        })
        .stroke(egui::Stroke::new(
            1.0_f32,
            if primary {
                RED
            } else {
                ui.visuals().widgets.noninteractive.bg_stroke.color
            },
        ))
        .corner_radius(SURFACE_RADIUS)
        .inner_margin(18)
        .show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.label(RichText::new(title).size(16.0).strong());
            ui.label(RichText::new(description).size(12.0).color(muted(ui)));
        })
        .response
        .interact(egui::Sense::click());
    if response.hovered() {
        ui.painter().rect_stroke(
            response.rect,
            12,
            egui::Stroke::new(1.0_f32, RED),
            egui::StrokeKind::Inside,
        );
    }
    response.on_hover_cursor(egui::CursorIcon::PointingHand)
}

fn queue_dialog(ctx: &egui::Context, name: &str, active: usize) -> Option<usize> {
    let mut action = None;
    let mut open = true;
    dialog(ctx, "queue-choice", 500.0, |ui| {
        dialog_header(
            ui,
            "Новая передача",
            &format!("Сейчас выполняется: {}", active),
            &mut open,
        );
        ui.add_space(16.0);
        ui.label(RichText::new(name).size(16.0).strong());
        ui.add_space(12.0);
        if transfer_choice(
            ui,
            "Добавить в очередь",
            "Начнётся автоматически после текущих передач.",
            true,
        )
        .clicked()
        {
            action = Some(0);
        }
        ui.add_space(8.0);
        if transfer_choice(
            ui,
            "Запустить одновременно",
            "Передачи будут делить сеть и ресурсы диска.",
            false,
        )
        .clicked()
        {
            action = Some(1);
        }
        ui.add_space(16.0);
        if pill(ui, "Отмена", false).clicked() {
            action = Some(2);
        }
    });
    if !open {
        action = Some(2);
    }

    action
}

/// UI fixture: no network operations, never compiled into the portable release.
#[cfg(feature = "test-fixtures")]
pub fn visual_fixture(ctx: &egui::Context, manager: &JobManager, queue: bool) {
    apply_theme(ctx, false);
    egui::CentralPanel::default()
        .frame(
            egui::Frame::new()
                .inner_margin(24)
                .fill(ctx.style().visuals.panel_fill),
        )
        .show(ctx, |ui| {
            ui.heading("Передачи");
            ui.add_space(16.0);
            let job = JobSnapshot {
                id: Uuid::nil(),
                tenant_id: None,
                name: "Ubuntu Server · production".into(),
                upload: false,
                destination: "D:\\Образы\\ubuntu.ova".into(),
                status: JobStatus::Preparing,
                progress: 0.0025,
                transferred: 0,
                total: 10 * 1024 * 1024 * 1024,
                network_bps: 0,
                write_bps: Some(0),
                elapsed: 44,
                message: String::new(),
                log: std::collections::VecDeque::from([
                    "Cloud Director: подготовка образа — выполняется".into(),
                ]),
                created: "01.11 · 14:20".into(),
                limit_mib: 0,
                phase: "Подготовка в Cloud Director".into(),
                phase_progress: Some(0.05),
                current_file: String::new(),
            };
            transfer_card(ui, &job, manager, &mut None);
            ui.add_space(14.0);
            let mut processing = job;
            processing.id = Uuid::from_u128(1);
            processing.upload = true;
            processing.name = "Ubuntu Server · catalog".into();
            processing.status = JobStatus::Processing;
            processing.progress = 0.986;
            processing.phase = "Обработка в Cloud Director".into();
            processing.phase_progress = Some(0.9);
            processing.transferred = 3_760_490_496;
            processing.current_file = "ubuntu-disk1.vmdk".into();
            processing.message =
                "Файлы переданы. Ожидание подтверждения готовности от Cloud Director.".into();
            processing.log = std::collections::VecDeque::from([
                "Отправляется файл: ubuntu.ovf".into(),
                "Отправляется файл: ubuntu-disk1.vmdk".into(),
                "Cloud Director: импорт шаблона — выполняется".into(),
            ]);
            transfer_card(ui, &processing, manager, &mut None);
        });
    if queue {
        let _ = queue_dialog(ctx, "Ubuntu Server · новый образ", 2);
    }
}
