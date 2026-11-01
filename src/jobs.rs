use crate::{
    api::CloudSession,
    bridge::{Bridge, Traffic},
    model::*,
    ovf,
    store::Store,
};
use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::{process::Command, runtime::Handle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobStatus {
    Queued,
    Preparing,
    Transferring,
    Processing,
    UploadingS3,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
}
impl JobStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::Queued => "В очереди",
            Self::Preparing => "Подготовка",
            Self::Transferring => "Передача OVF Tool",
            Self::Processing => "Обработка в Cloud Director",
            Self::UploadingS3 => "Отправка в S3",
            Self::Completed => "Завершено",
            Self::Failed => "Ошибка",
            Self::Cancelled => "Отменено",
            Self::Interrupted => "Прервано при закрытии",
        }
    }
    pub fn active(self) -> bool {
        matches!(
            self,
            Self::Preparing | Self::Transferring | Self::Processing | Self::UploadingS3
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JobSnapshot {
    pub id: Uuid,
    #[serde(default)]
    pub tenant_id: Option<Uuid>,
    pub name: String,
    pub upload: bool,
    pub destination: String,
    pub status: JobStatus,
    pub progress: f32,
    pub transferred: u64,
    pub total: u64,
    pub network_bps: u64,
    pub write_bps: Option<u64>,
    pub elapsed: u64,
    pub message: String,
    pub log: VecDeque<String>,
    pub created: String,
    pub limit_mib: u64,
    #[serde(default)]
    pub phase: String,
    #[serde(default)]
    pub phase_progress: Option<f32>,
    #[serde(default)]
    pub current_file: String,
}

struct Job {
    view: JobSnapshot,
    request: Option<TransferRequest>,
    cancel: CancellationToken,
    traffic: Arc<Traffic>,
}

type Reservations = Arc<Mutex<HashMap<String, HashMap<Uuid, u64>>>>;
type LocalReservations = Arc<Mutex<HashMap<String, HashMap<Uuid, LocalBudget>>>>;

struct LocalBudget {
    required: u64,
    stage: PathBuf,
}

#[derive(Clone)]
pub struct JobManager {
    jobs: Arc<Mutex<Vec<Job>>>,
    store: Store,
    runtime: Handle,
    reservations: LocalReservations,
    cloud_reservations: Reservations,
}

impl JobManager {
    pub fn new(store: Store, runtime: Handle) -> Self {
        let history = std::fs::read(store.directory.join("transfers.json"))
            .ok()
            .and_then(|d| serde_json::from_slice::<Vec<JobSnapshot>>(&d).ok())
            .unwrap_or_default();
        let jobs=history.into_iter().map(|mut view|{if view.status.active()||view.status==JobStatus::Queued{view.status=JobStatus::Interrupted;view.message="Предыдущая сессия завершена. Для повторной передачи выберите объект заново.".into();}Job{view,request:None,cancel:CancellationToken::new(),traffic:Arc::new(Traffic::default())}}).collect();
        Self {
            jobs: Arc::new(Mutex::new(jobs)),
            store,
            runtime,
            reservations: Default::default(),
            cloud_reservations: Default::default(),
        }
    }
    pub fn snapshots(&self) -> Vec<JobSnapshot> {
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .map(|j| j.view.clone())
            .collect()
    }
    pub fn active_count(&self) -> usize {
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .filter(|j| j.view.status.active())
            .count()
    }
    pub fn pending_count(&self) -> usize {
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .filter(|j| j.view.status.active() || j.view.status == JobStatus::Queued)
            .count()
    }
    pub fn enqueue(&self, request: TransferRequest, parallel: bool) -> Uuid {
        let id = Uuid::new_v4();
        let traffic = Arc::new(Traffic::default());
        traffic.limit.store(
            request.limit_mib.saturating_mul(1024 * 1024),
            Ordering::Relaxed,
        );
        let destination = if let Some(s3) = &request.s3 {
            format!("S3 / {} / {}", s3.name, s3.bucket)
        } else {
            request.local_path.display().to_string()
        };
        let view = JobSnapshot {
            id,
            tenant_id: Some(request.tenant.id),
            name: request.object.name.clone(),
            upload: request.direction == Direction::Upload,
            destination,
            status: JobStatus::Queued,
            progress: 0.0,
            transferred: 0,
            total: request.estimated_bytes,
            network_bps: 0,
            write_bps: None,
            elapsed: 0,
            message: String::new(),
            log: VecDeque::new(),
            created: chrono::Local::now().format("%d.%m %H:%M").to_string(),
            limit_mib: request.limit_mib,
            phase: "Подключение к Cloud Director".into(),
            phase_progress: None,
            current_file: String::new(),
        };
        self.jobs.lock().unwrap().push(Job {
            view,
            request: Some(request),
            cancel: CancellationToken::new(),
            traffic,
        });
        self.persist();
        if parallel || self.active_count() == 0 {
            self.start(id);
        }
        id
    }
    pub fn cancel(&self, id: Uuid) {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(j) = jobs.iter_mut().find(|j| j.view.id == id) {
            j.cancel.cancel();
            if j.view.status == JobStatus::Queued {
                j.view.status = JobStatus::Cancelled;
            }
        }
        drop(jobs);
        self.persist();
    }
    pub fn cancel_all(&self) {
        let ids = self
            .snapshots()
            .iter()
            .filter(|j| j.status.active() || j.status == JobStatus::Queued)
            .map(|j| j.id)
            .collect::<Vec<_>>();
        for id in ids {
            self.cancel(id);
        }
    }
    pub fn set_limit(&self, id: Uuid, mib: u64) {
        if let Some(j) = self
            .jobs
            .lock()
            .unwrap()
            .iter_mut()
            .find(|j| j.view.id == id)
        {
            j.traffic
                .limit
                .store(mib.saturating_mul(1024 * 1024), Ordering::Relaxed);
            j.view.limit_mib = mib;
            if let Some(request) = &mut j.request {
                request.limit_mib = mib;
            }
        }
    }
    pub fn retry(&self, id: Uuid, parallel: bool) -> Result<()> {
        let req = self
            .jobs
            .lock()
            .unwrap()
            .iter()
            .find(|j| j.view.id == id)
            .and_then(|j| j.request.clone())
            .context("Повтор после перезапуска: выберите объект в списке заново")?;
        self.enqueue(req, parallel);
        Ok(())
    }
    pub fn clear_finished(&self) -> Vec<JobSnapshot> {
        // Observe and remove under the same lock so a just-completed transfer
        // cannot disappear before the UI processes its completion notification.
        let mut removed = Vec::new();
        {
            let mut jobs = self.jobs.lock().unwrap();
            jobs.retain(|job| {
                let keep = job.view.status.active() || job.view.status == JobStatus::Queued;
                if !keep {
                    removed.push(job.view.clone());
                }
                keep
            });
        }
        self.persist();
        removed
    }
    fn mutate(&self, id: Uuid, f: impl FnOnce(&mut JobSnapshot)) {
        if let Some(j) = self
            .jobs
            .lock()
            .unwrap()
            .iter_mut()
            .find(|j| j.view.id == id)
        {
            f(&mut j.view);
        }
    }
    fn log(&self, id: Uuid, line: String) {
        let line = if matches!(
            line.as_str(),
            "Отправляется файл: file" | "Скачивается файл: file"
        ) {
            let name = self
                .jobs
                .lock()
                .unwrap()
                .iter()
                .find(|j| j.view.id == id)
                .map(|j| j.view.name.clone())
                .unwrap_or_else(|| "ISO / Media".into());
            format!(
                "{}: {name} (поток образа)",
                line.split(':').next().unwrap_or("Файл")
            )
        } else {
            line
        };
        let line = crate::store::redact(&line);
        self.store.log("TRANSFER", &format!("{id}: {line}"));
        self.mutate(id, |v| {
            if v.log.len() >= 120 {
                v.log.pop_front();
            }
            v.log.push_back(line);
        });
    }
    fn persist(&self) {
        let snapshots = self.snapshots();
        let first = snapshots.len().saturating_sub(200);
        if let Ok(data) = serde_json::to_vec(&snapshots[first..]) {
            let _ = crate::store::atomic_write(&self.store.directory.join("transfers.json"), &data);
        }
    }
    fn start(&self, id: Uuid) {
        let data = {
            let mut jobs = self.jobs.lock().unwrap();
            let Some(j) = jobs
                .iter_mut()
                .find(|j| j.view.id == id && j.view.status == JobStatus::Queued)
            else {
                return;
            };
            j.view.status = JobStatus::Preparing;
            (
                j.request.clone().unwrap(),
                j.traffic.clone(),
                j.cancel.clone(),
            )
        };
        let manager = self.clone();
        self.runtime.spawn(async move {
            let result = manager.run(id, data.0, data.1, data.2.clone()).await;
            let status = if data.2.is_cancelled() {
                JobStatus::Cancelled
            } else if result.is_ok() {
                JobStatus::Completed
            } else {
                JobStatus::Failed
            };
            let message = match result {
                Ok(message) => message,
                Err(e) => format!("{e:#}"),
            };
            manager.log(id, message.clone());
            manager.mutate(id, |v| {
                v.status = status;
                v.network_bps = 0;
                v.write_bps = None;
                v.message = crate::store::redact(&message);
                if status == JobStatus::Completed {
                    v.progress = 1.0;
                    v.phase = "Готово".into();
                    v.phase_progress = Some(1.0);
                }
            });
            manager.persist();
            // Queued tasks start only after all explicitly parallel transfers finish.
            let next = {
                let jobs = manager.jobs.lock().unwrap();
                if jobs.iter().any(|j| j.view.status.active()) {
                    None
                } else {
                    jobs.iter()
                        .find(|j| j.view.status == JobStatus::Queued)
                        .map(|j| j.view.id)
                }
            };
            if let Some(next) = next {
                manager.start(next);
            }
        });
    }

    async fn run(
        &self,
        id: Uuid,
        mut request: TransferRequest,
        traffic: Arc<Traffic>,
        cancel: CancellationToken,
    ) -> Result<String> {
        let version = ovf::probe(&request.ovf_directory).await?;
        if request.parallel_threads > 1
            && crate::updates::Version::parse(&version)
                .is_none_or(|v| v < crate::updates::Version(4, 4, 0))
        {
            request.parallel_threads = 1;
            self.log(
                id,
                "Для этой версии OVF Tool выбран один поток совместимости.".into(),
            );
        }
        let store = self.store.clone();
        let tenant_id = request.tenant.id;
        let secret = tokio::task::spawn_blocking(move || store.secret(tenant_id)).await??;
        let session = tokio::select! {_=cancel.cancelled()=>bail!("Передача отменена"),s=CloudSession::login(&request.endpoint,&request.tenant,secret)=>s?};
        let mut reservation = None;
        let mut cloud_reservation = None;
        let mut stage = None;
        let output = if request.direction == Direction::Download {
            verify_download_locator(&session, &request.object).await?;
            let inspection = session.inspect_export(&request.object).await?;
            crate::export_workflow::ensure_reviewed(
                &inspection,
                &request.expected_vms,
                &request.eject_media,
            )?;
            let required = inspection
                .estimated_bytes
                .unwrap_or(request.estimated_bytes)
                .max(request.estimated_bytes);
            ensure!(
                required > 0,
                "Не указан размер для проверки свободного места"
            );
            reservation =
                Some(self.reserve(id, &request.local_path, required, request.reserve_bytes)?);
            if let Some(target) = &request.capture {
                let report = session
                    .catalog_preflight(
                        &target.catalog_href,
                        &target.name,
                        &request.object.vdc,
                        required,
                    )
                    .await?;
                self.log(id, report.summary());
                ensure!(
                    report.catalog_name == target.catalog_name,
                    "Выбранный каталог переименован. Откройте скачивание заново."
                );
                // Check capture permission before any approved ISO ejection;
                // capture_for_download repeats this check before the POST.
                session.capture_action(&target.catalog_href).await?;
                ensure!(
                    report.capacity_verified || request.allow_unknown_capacity,
                    "Cloud Director не сообщил точный остаток места. Проверьте назначение и подтвердите продолжение с неизвестным остатком."
                );
                cloud_reservation =
                    Some(self.reserve_cloud(id, &request, &target.name, &report)?);
            }
            let progress = |message: &str, percent: Option<u8>| {
                if !message.is_empty() {
                    self.log(id, message.into());
                }
                self.mutate(id, |v| {
                    v.phase_progress = percent.map(|p| p as f32 / 100.0);
                    v.progress = 0.001;
                });
            };
            self.mutate(id, |v| v.phase = "Проверка и извлечение ISO".into());
            for media in inspection.vms.iter().flat_map(|v| &v.media) {
                self.log(
                    id,
                    format!(
                        "Извлечение ISO: {} / {}. Автоматическое подключение обратно отключено.",
                        media.vm_name, media.name
                    ),
                );
                session
                    .eject_reviewed_media(media, &cancel, &progress)
                    .await?;
            }
            let current = session.inspect_export(&request.object).await?;
            crate::export_workflow::ensure_reviewed(&current, &request.expected_vms, &[])?;
            if let Some(target) = &request.capture {
                self.mutate(id, |v| {
                    v.phase = "Создание шаблона в каталоге".into();
                    v.phase_progress = None;
                    v.progress = 0.001;
                });
                self.log(id, format!("Создание шаблона: {} / {}. Шаблон останется в каталоге, в том числе при отмене.", target.catalog_name, target.name));
                request.object = session
                    .capture_for_download(&current.object, target, &cancel, &progress)
                    .await?;
                request.capture = None;
                request.expected_vms.clear();
                request.eject_media.clear();
                if let Some(job) = self
                    .jobs
                    .lock()
                    .unwrap()
                    .iter_mut()
                    .find(|j| j.view.id == id)
                {
                    job.request = Some(request.clone());
                }
                // Keep the cloud reservation while downloading: catalog readiness
                // does not guarantee that the quota accounting has caught up yet.
                verify_download_locator(&session, &request.object).await?;
                self.log(
                    id,
                    "Шаблон готов. Скачивание из каталога через OVF Tool.".into(),
                );
            }
            self.mutate(id, |v| {
                v.phase = "Подготовка передачи".into();
                v.phase_progress = None;
            });
            let directory = request
                .local_path
                .join(format!(".ovfdock-{}.partial", id.simple()));
            std::fs::create_dir(&directory)?;
            let file = directory.join(format!(
                "{}.{}",
                safe_filename(&request.object.name),
                request.format.to_lowercase()
            ));
            self.mutate(id, |v| {
                v.destination = directory.display().to_string();
                v.total = required;
            });
            stage = Some(directory);
            file
        } else {
            ensure!(request.local_path.is_file(), "Исходный файл не найден");
            self.mutate(id, |v| v.phase = "Проверка пакета перед загрузкой".into());
            let path = request.local_path.clone();
            let token = cancel.clone();
            let validation =
                tokio::task::spawn_blocking(move || ovf::validate_package(&path, &token)).await??;
            if request.object.kind != ObjectKind::Media {
                self.log(id, "Проверка совместимости пакета через OVF Tool…".into());
                let verified = ovf::verify_with_tool(
                    &request.ovf_directory,
                    &request.local_path,
                    cancel.clone(),
                )
                .await?;
                self.log(id, verified);
            }
            let catalogs = session.catalogs().await?;
            let targets = catalogs
                .iter()
                .filter(|c| c.name == request.object.catalog)
                .collect::<Vec<_>>();
            ensure!(
                targets.len() == 1,
                "Каталог не найден или имя каталога неоднозначно. Обновите список и выберите каталог заново."
            );
            let report = session
                .catalog_preflight(
                    &targets[0].href,
                    &request.object.name,
                    &request.object.vdc,
                    validation.provisioned_bytes,
                )
                .await?;
            self.log(id, report.summary());
            ensure!(
                report.capacity_verified || request.allow_unknown_capacity,
                "Cloud Director не сообщил точный остаток места. Проверьте назначение и подтвердите продолжение с неизвестным остатком."
            );
            cloud_reservation =
                Some(self.reserve_cloud(id, &request, &request.object.name, &report)?);
            request.local_path.canonicalize()?
        };
        let bridge = Bridge::start(session.clone(), traffic.clone(), cancel.child_token()).await?;
        ensure!(!cancel.is_cancelled(), "Передача отменена");
        let args = ovf::arguments(&request, &bridge, &output)?;
        let mut command = Command::new(ovf::executable(&request.ovf_directory)?);
        command
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        ovf::hide_console(&mut command);
        let mut child = command.spawn().context("Не удалось запустить OVF Tool")?;
        let pid = child.id().context("Не найден процесс OVF Tool")?;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let out = tokio::spawn(ovf::read_lines(
            child.stdout.take().unwrap(),
            sender.clone(),
        ));
        let err = tokio::spawn(ovf::read_lines(child.stderr.take().unwrap(), sender));
        self.mutate(id, |v| v.status = JobStatus::Transferring);
        self.log(
            id,
            "Запущен OVF Tool; авторизация через локальную защищённую сессию".into(),
        );
        let start = Instant::now();
        let mut sample = Instant::now();
        let mut previous = 0;
        let mut previous_write = process_write_bytes(pid);
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        let mut machine = ovf::MachineOutput::default();
        let mut transfer_progress = None;
        let status = loop {
            tokio::select! {
                _=cancel.cancelled()=>{let _=child.kill().await;let _=child.wait().await;bail!("Передача отменена. Неполные файлы оставлены в папке .partial.");},
                line=receiver.recv(),if !receiver.is_closed()||!receiver.is_empty()=>{if let Some(line)=line{
                    if let Some(p)=machine.feed(&line){transfer_progress=Some(p);}
                    if let Some(text)=readable_tool_line(&line){self.log(id,text);}
                }},
                _=tick.tick()=>{
                    let total=if request.direction==Direction::Download{traffic.received.load(Ordering::Relaxed)}else{traffic.sent.load(Ordering::Relaxed)};
                    let dt=sample.elapsed().as_secs_f64().max(0.001);let write=process_write_bytes(pid);
                    let write_rate=write.zip(previous_write).map(|(a,b)|((a.saturating_sub(b))as f64/dt)as u64);
                    self.mutate(id,|v|{v.transferred=total;v.network_bps=((total.saturating_sub(previous))as f64/dt)as u64;v.write_bps=write_rate;v.elapsed=start.elapsed().as_secs();});
                    self.cloud_progress(id,&traffic,transfer_progress,machine.preparing,false);
                    sample=Instant::now();previous=total;previous_write=write;
                    if request.direction==Direction::Download
                        && let Ok(free)=fs2::available_space(&request.local_path)&& free<request.reserve_bytes{let _=child.kill().await;let _=child.wait().await;bail!("Свободное место опустилось ниже резерва. Неполные файлы сохранены.");}
                    if let Some(status)=child.try_wait()?{break status}
                }
            }
        };
        let _ = out.await;
        let _ = err.await;
        while let Ok(line) = receiver.try_recv() {
            machine.feed(&line);
            if let Some(text) = readable_tool_line(&line) {
                self.log(id, text);
            }
        }
        let diagnostics = traffic.diagnostics();
        if !status.success() || machine.failed {
            for line in &diagnostics {
                self.log(id, line.clone());
            }
            let cause = diagnostics
                .iter()
                .rev()
                .find(|s| {
                    s.contains("HTTP 4") || s.contains("HTTP 5") || s.starts_with("Ошибка адаптера")
                })
                .cloned()
                .unwrap_or_default();
            bail!(
                "{} {} Код выхода: {}. Неполные файлы сохранены в {}",
                machine.error_summary(),
                cause,
                status.code().unwrap_or(-1),
                output.parent().unwrap_or(&output).display()
            );
        }
        self.log(
            id,
            "OVF Tool передал файлы. Проверяется завершение задач Cloud Director.".into(),
        );
        self.mutate(id, |v| {
            v.status = JobStatus::Processing;
            v.network_bps = 0;
            v.write_bps = None;
        });
        self.await_cloud(
            id,
            &session,
            &traffic,
            &cancel,
            request.direction == Direction::Upload,
            start,
        )
        .await?;
        drop(cloud_reservation);
        drop(bridge);
        if let Some(stage) = stage {
            ensure!(
                output.is_file() && std::fs::metadata(&output)?.len() > 0,
                "OVF Tool сообщил об успехе, но итоговый файл отсутствует"
            );
            self.mutate(id, |v| {
                v.phase = "Проверка скачанного пакета".into();
                v.phase_progress = None;
            });
            let path = output.clone();
            let token = cancel.clone();
            tokio::task::spawn_blocking(move || ovf::validate_package(&path, &token)).await??;
            ensure!(
                !cancel.is_cancelled(),
                "Проверка отменена; файлы остаются в .partial"
            );
            let final_dir = request.local_path.join(format!(
                "{}-{}",
                safe_filename(&request.object.name),
                &id.simple().to_string()[..8]
            ));
            ensure!(!final_dir.exists(), "Итоговая папка уже существует");
            std::fs::rename(&stage, &final_dir)?;
            drop(reservation);
            let final_file = final_dir.join(output.file_name().unwrap());
            self.mutate(id, |v| v.destination = final_file.display().to_string());
            if let Some(s3) = request.s3 {
                ensure!(request.format != "OVF", "Для S3 выберите OVA или ISO");
                self.mutate(id, |v| {
                    v.status = JobStatus::UploadingS3;
                    v.progress = 0.0;
                    v.transferred = 0;
                    v.total = std::fs::metadata(&final_file).map(|m| m.len()).unwrap_or(0);
                    v.write_bps = None;
                    v.network_bps = 0;
                    v.phase = "Отправка в S3".into();
                    v.phase_progress = Some(0.0);
                });
                traffic.sent.store(0, Ordering::Relaxed);
                let store = self.store.clone();
                let s3id = s3.id;
                let secret = tokio::task::spawn_blocking(move || store.secret(s3id)).await??;
                let key = crate::s3::object_key(
                    &s3,
                    id,
                    &final_file.file_name().unwrap().to_string_lossy(),
                );
                let upload = crate::s3::upload_file(
                    &s3,
                    secret,
                    &final_file,
                    &key,
                    traffic.clone(),
                    cancel.clone(),
                );
                tokio::pin!(upload);
                let mut last = 0;
                let mut sampled = Instant::now();
                let mut timer = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tokio::select! {
                        result=&mut upload=>{result.with_context(||format!("Локальный образ сохранён: {}",final_file.display()))?;break},
                        _=timer.tick()=>{let n=traffic.sent.load(Ordering::Relaxed);let dt=sampled.elapsed().as_secs_f64().max(0.001);self.mutate(id,|v|{v.transferred=n;v.network_bps=((n-last)as f64/dt)as u64;v.progress=((n as f64/v.total.max(1)as f64)as f32).min(0.99);v.phase_progress=Some(v.progress);v.elapsed=start.elapsed().as_secs();});last=n;sampled=Instant::now();}
                    }
                }
                return Ok(format!(
                    "Сохранено в S3: {}/{}. Локальная копия: {}",
                    s3.bucket,
                    key,
                    final_file.display()
                ));
            }
            Ok(format!("Сохранено: {}", final_file.display()))
        } else {
            Ok("Загрузка в каталог завершена".into())
        }
    }

    fn cloud_progress(
        &self,
        id: Uuid,
        traffic: &Traffic,
        transfer: Option<f32>,
        preparing: bool,
        finalizing: bool,
    ) {
        let (trace, events) = {
            let mut trace = traffic.cloud.lock().unwrap();
            let events = trace.drain_events();
            (trace.clone(), events)
        };
        for event in events {
            self.log(id, event);
        }
        self.mutate(id, |v| {
            let processing = finalizing || transfer.is_some_and(|p| p >= 1.0);
            let (phase, percent, overall) = if processing {
                (
                    "Обработка в Cloud Director",
                    trace.progress().map(|p| p as f32 / 100.0),
                    0.95 + trace.progress().unwrap_or(0) as f32 / 100.0 * 0.04,
                )
            } else if (preparing || trace.pending()) && transfer.is_none_or(|p| p <= 0.0) {
                (
                    "Подготовка в Cloud Director",
                    trace.progress().map(|p| p as f32 / 100.0),
                    (trace.progress().unwrap_or(0) as f32 / 100.0 * 0.05).max(0.001),
                )
            } else if let Some(p) = transfer {
                ("Передача файлов", Some(p), 0.05 + p * 0.90)
            } else {
                ("Подключение и подготовка", None, 0.001)
            };
            v.phase = phase.into();
            v.phase_progress = percent;
            v.progress = v.progress.max(overall).min(0.99);
            v.current_file = if trace.current_file == "file" {
                format!("{} (образ)", v.name)
            } else {
                trace.current_file.clone()
            };
            v.message = if processing {
                "Файлы переданы. Ожидание подтверждения готовности от Cloud Director.".into()
            } else {
                String::new()
            };
            if processing {
                v.status = JobStatus::Processing;
            } else if (preparing || trace.pending()) && transfer.is_none_or(|p| p <= 0.0) {
                v.status = JobStatus::Preparing;
            } else {
                v.status = JobStatus::Transferring;
            }
        });
    }

    pub(crate) async fn await_cloud(
        &self,
        id: Uuid,
        session: &CloudSession,
        traffic: &Traffic,
        cancel: &CancellationToken,
        upload: bool,
        start: Instant,
    ) -> Result<()> {
        let wait = async {
            let mut ready_samples = 0;
            loop {
                let trace = traffic.cloud.lock().unwrap().clone();
                trace.check_errors()?;
                if upload {
                    ensure!(
                        !trace.targets.is_empty(),
                        "Файлы переданы, но API не вернул адрес созданного образа. Готовность в Cloud Director не подтверждена; проверьте каталог перед повторной загрузкой."
                    );
                }
                for (href, task) in &trace.tasks {
                    if task.pending() {
                        let xml = session
                            .get(href)
                            .await
                            .context("Не удалось проверить задачу Cloud Director")?;
                        traffic.cloud.lock().unwrap().observe(&xml, false)?;
                    }
                }
                if upload {
                    for href in trace.targets.keys() {
                        let xml = session
                            .get(href)
                            .await
                            .context("Не удалось подтвердить готовность образа")?;
                        traffic.cloud.lock().unwrap().observe(&xml, false)?;
                    }
                }
                self.cloud_progress(id, traffic, Some(1.0), false, true);
                self.mutate(id, |v| v.elapsed = start.elapsed().as_secs());
                let trace = traffic.cloud.lock().unwrap().clone();
                trace.check_errors()?;
                let ready = if upload {
                    trace.upload_ready()
                } else {
                    !trace.pending()
                };
                if ready {
                    ready_samples += 1;
                } else {
                    ready_samples = 0;
                }
                if ready_samples >= if upload { 2 } else { 1 } {
                    self.log(id, "Cloud Director подтвердил завершение обработки.".into());
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        };
        tokio::select! {
            _=cancel.cancelled()=>bail!("Ожидание отменено. Обработка уже переданного образа в Cloud Director может продолжаться; проверьте каталог перед повторной загрузкой."),
            result=tokio::time::timeout(Duration::from_secs(2*60*60),wait)=>result.context("Cloud Director не подтвердил готовность за 2 часа. Операция не отмечена успешной; проверьте задачи ЦОДа.")?,
        }
    }

    fn reserve_cloud(
        &self,
        id: Uuid,
        request: &TransferRequest,
        name: &str,
        report: &crate::catalog_preflight::CatalogPreflight,
    ) -> Result<Vec<Reservation>> {
        report.check_capacity()?;
        // The resource URI, rather than the saved connection UUID, is the shared
        // identity when the same tenant is configured through multiple logins.
        let canonical = |href: &str| -> Result<String> { Ok(url::Url::parse(href)?.to_string()) };
        let name_key = serde_json::to_string(&(
            "name",
            canonical(&report.catalog_href)?,
            crate::catalog_preflight::normalized_name(name),
        ))?;
        let mut map = self.cloud_reservations.lock().unwrap();
        ensure!(
            map.get(&name_key).is_none_or(|v| v.is_empty()),
            "Образ с именем «{name}» уже создаётся другим заданием в этом каталоге."
        );
        let mut entries = vec![(name_key, 1)];
        let mut candidates = 0;
        let mut possible = 0;
        let mut unverified = report.required_bytes == 0;
        for policy in report.profiles.iter().filter(|p| p.enabled != Some(false)) {
            let key =
                serde_json::to_string(&("policy", policy_reservation_identity(&policy.href)?))?;
            if entries.iter().any(|(existing, _)| existing == &key) {
                continue;
            }
            candidates += 1;
            let reserved = map
                .get(&key)
                .map(|v| {
                    v.values()
                        .try_fold(0u64, |sum, bytes| sum.checked_add(*bytes))
                })
                .unwrap_or(Some(0))
                .context("Переполнение резервов облачной квоты")?;
            if let Some(free) = policy.free_bytes {
                if free.saturating_sub(reserved) >= report.required_bytes {
                    possible += 1;
                    unverified |= policy.enabled != Some(true);
                } else {
                    unverified = true;
                }
            } else {
                possible += 1;
                unverified = true;
            }
            entries.push((key, report.required_bytes));
        }
        ensure!(
            candidates == 0 || possible > 0,
            "Недостаточно облачной квоты с учётом других активных заданий: требуется {}. {}",
            human_bytes(report.required_bytes),
            report.summary()
        );
        ensure!(
            (candidates > 0 && !unverified) || request.allow_unknown_capacity,
            "Достаточность облачной квоты с учётом активных заданий не подтверждена. Обновите проверку назначения или подтвердите неизвестный остаток."
        );
        // No entry is inserted until all checks pass: a rejected job must not
        // leave either its name or a partial set of policy reservations behind.
        Ok(entries
            .into_iter()
            .map(|(key, bytes)| {
                map.entry(key.clone()).or_default().insert(id, bytes);
                Reservation {
                    map: self.cloud_reservations.clone(),
                    key,
                    id,
                }
            })
            .collect())
    }

    fn reserve(&self, id: Uuid, path: &Path, bytes: u64, reserve: u64) -> Result<LocalReservation> {
        let key = volume_key(path)?;
        let mut reservations = self.reservations.lock().unwrap();
        let volume = reservations.entry(key.clone()).or_default();
        let existing = volume
            .values()
            .try_fold(0u64, |sum, b| sum.checked_add(b.remaining()))
            .context("Слишком большой резерв")?;
        ovf::check_destination(
            path,
            bytes
                .checked_add(existing)
                .context("Слишком большой резерв")?,
            reserve,
        )?;
        volume.insert(
            id,
            LocalBudget {
                required: bytes,
                stage: path.join(format!(".ovfdock-{}.partial", id.simple())),
            },
        );
        Ok(LocalReservation {
            map: self.reservations.clone(),
            key,
            id,
        })
    }
}

/// Ensure that the name-based locator OVF Tool will use still identifies the
/// exact resource reviewed in the UI. Query results may include equal names.
fn ensure_locator_target(object: &CloudObject, records: &[CloudObject]) -> Result<()> {
    let same_name = |a: &str, b: &str| {
        crate::catalog_preflight::normalized_name(a) == crate::catalog_preflight::normalized_name(b)
    };
    let candidates: Vec<_> = records
        .iter()
        .filter(|r| {
            r.kind == object.kind
                && same_name(&r.name, &object.name)
                && (object.vdc.is_empty() || same_name(&r.vdc, &object.vdc))
                && (object.catalog.is_empty() || same_name(&r.catalog, &object.catalog))
        })
        .collect();
    ensure!(
        candidates.len() == 1,
        "Имя «{}» отсутствует или неоднозначно для OVF Tool. Обновите список объектов и выберите объект заново.",
        object.name
    );
    let current = candidates[0];
    ensure!(
        current.href == object.href
            && current.name == object.name
            && current.catalog == object.catalog
            && current.vdc == object.vdc
            && (object.kind != ObjectKind::Vm
                || (current.parent_href == object.parent_href
                    && current.parent_name == object.parent_name)),
        "Объект «{}» был переименован, перемещён или заменён. Откройте скачивание заново.",
        object.name
    );
    Ok(())
}

fn policy_reservation_identity(href: &str) -> Result<String> {
    let mut url = url::Url::parse(href)?;
    // AdminCatalog references and tenant Query API can expose different views
    // of the same policy UUID. Both must consume a single reservation budget.
    let path = url.path().replacen(
        "/api/admin/vdcStorageProfile/",
        "/api/vdcStorageProfile/",
        1,
    );
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url.to_string())
}

async fn verify_download_locator(session: &CloudSession, object: &CloudObject) -> Result<()> {
    let kind = match object.kind {
        ObjectKind::Vapp => "vApp",
        ObjectKind::Vm => "vm",
        ObjectKind::Template => "vAppTemplate",
        ObjectKind::Media => "media",
        _ => bail!("Этот объект нельзя скачать через OVF Tool"),
    };
    ensure_locator_target(object, &session.query(kind).await?)?;
    if object.kind == ObjectKind::Vm {
        let mut parent = object.clone();
        parent.kind = ObjectKind::Vapp;
        parent.name = object.parent_name.clone();
        parent.href = object.parent_href.clone();
        ensure_locator_target(&parent, &session.query("vApp").await?)?;
    }
    if !object.catalog.is_empty() {
        let catalogs = session.query("catalog").await?;
        ensure!(
            catalogs
                .iter()
                .filter(|c| crate::catalog_preflight::normalized_name(&c.name)
                    == crate::catalog_preflight::normalized_name(&object.catalog))
                .count()
                == 1,
            "Есть несколько каталогов с одинаковым именем; скачивание через OVF Tool неоднозначно."
        );
    }
    Ok(())
}

impl LocalBudget {
    fn remaining(&self) -> u64 {
        // Credit only blocks actually allocated in this job's private staging
        // directory. Logical file lengths and network counters are insufficient
        // for sparse files, compression and preallocation.
        self.required
            .saturating_sub(allocated_tree_bytes(&self.stage).unwrap_or(0))
    }
}

fn allocated_tree_bytes(path: &Path) -> Result<u64> {
    use fs2::FileExt;
    if !path.exists() {
        return Ok(0);
    }
    let mut paths = vec![path.to_owned()];
    let mut total = 0u64;
    let mut visited = 0u32;
    while let Some(path) = paths.pop() {
        visited += 1;
        ensure!(visited <= 10_000, "Слишком много файлов в папке передачи");
        let metadata = std::fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            if metadata.file_attributes() & 0x400 != 0 {
                continue;
            } // reparse point
        }
        if metadata.is_dir() {
            for entry in std::fs::read_dir(path)? {
                paths.push(entry?.path());
            }
        } else if metadata.is_file() {
            let bytes = std::fs::File::open(path)?.allocated_size()?;
            total = total
                .checked_add(bytes)
                .context("Переполнение размера файлов передачи")?;
        }
    }
    Ok(total)
}

struct LocalReservation {
    map: LocalReservations,
    key: String,
    id: Uuid,
}
impl Drop for LocalReservation {
    fn drop(&mut self) {
        if let Some(volume) = self.map.lock().unwrap().get_mut(&self.key) {
            volume.remove(&self.id);
        }
    }
}

fn readable_tool_line(line: &str) -> Option<String> {
    let line = line.trim();
    if line.starts_with('+')
        || matches!(
            line,
            "PROGRESS" | "TASK" | "RESULT" | "TARGET_ID" | "PROBE" | "ERROR"
        )
        || line.is_empty()
    {
        None
    } else {
        Some(line.into())
    }
}

struct Reservation {
    map: Reservations,
    key: String,
    id: Uuid,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some(v) = self.map.lock().unwrap().get_mut(&self.key) {
            v.remove(&self.id);
        }
    }
}

fn volume_key(path: &Path) -> Result<String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(std::fs::metadata(path)?.dev().to_string())
    }
    #[cfg(windows)]
    {
        let p = path.canonicalize()?;
        Ok(p.components()
            .next()
            .context("Не найден том")?
            .as_os_str()
            .to_string_lossy()
            .to_lowercase())
    }
}

pub fn process_write_bytes(pid: u32) -> Option<u64> {
    #[cfg(windows)]
    unsafe {
        use windows_sys::Win32::{
            Foundation::CloseHandle,
            System::Threading::{
                GetProcessIoCounters, IO_COUNTERS, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
            },
        };
        let h = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if h.is_null() {
            return None;
        }
        let mut io: IO_COUNTERS = std::mem::zeroed();
        let ok = GetProcessIoCounters(h, &mut io);
        CloseHandle(h);
        if ok != 0 {
            Some(io.WriteTransferCount)
        } else {
            None
        }
    }
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string(format!("/proc/{pid}/io")).ok()?;
        text.lines().find_map(|l| {
            l.strip_prefix("write_bytes:")
                .and_then(|s| s.trim().parse().ok())
        })
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = pid;
        None
    }
}

#[cfg(test)]
mod reservation_tests {
    use super::*;
    use crate::catalog_preflight::{CatalogPreflight, CatalogStorageProfile};

    fn object() -> CloudObject {
        crate::api::parse_records(r#"<R><VAppTemplateRecord name="image" catalogName="mine" vdcName="vdc" href="https://cloud.invalid/api/vAppTemplate/1"/></R>"#).unwrap().0.remove(0)
    }

    fn request() -> TransferRequest {
        TransferRequest {
            endpoint: Endpoint {
                id: Uuid::new_v4(),
                name: "mock".into(),
                url: "https://cloud.invalid".into(),
                tenants: vec![],
            },
            tenant: Tenant {
                id: Uuid::new_v4(),
                organization: "org".into(),
                username: String::new(),
                auth: AuthKind::BearerToken,
            },
            object: object(),
            direction: Direction::Upload,
            local_path: PathBuf::new(),
            format: "OVA".into(),
            s3: None,
            limit_mib: 0,
            estimated_bytes: 100,
            reserve_bytes: 0,
            ovf_directory: PathBuf::new(),
            accept_eulas: false,
            capture: None,
            expected_vms: vec![],
            eject_media: vec![],
            parallel_threads: 1,
            allow_unknown_capacity: false,
        }
    }

    fn report(required_bytes: u64) -> CatalogPreflight {
        CatalogPreflight {
            catalog_href: "https://cloud.invalid/api/catalog/1".into(),
            catalog_name: "mine".into(),
            required_bytes,
            profiles: vec![CatalogStorageProfile {
                href: "https://cloud.invalid/api/vdcStorageProfile/1".into(),
                name: "fast".into(),
                enabled: Some(true),
                limit_bytes: Some(200),
                used_bytes: Some(50),
                free_bytes: Some(150),
            }],
            warnings: vec![],
            capacity_verified: true,
        }
    }

    #[tokio::test]
    async fn duplicate_names_share_unicode_identity_across_saved_connections() {
        let dir = tempfile::tempdir().unwrap();
        let manager = JobManager::new(Store::for_test(dir.path().into()), Handle::current());
        let first = manager
            .reserve_cloud(Uuid::new_v4(), &request(), "ÉКО", &report(20))
            .unwrap();
        let second = manager.reserve_cloud(Uuid::new_v4(), &request(), "e\u{301}ко", &report(20));
        assert!(second.err().unwrap().to_string().contains("уже создаётся"));
        drop(first);
        assert!(
            manager
                .reserve_cloud(Uuid::new_v4(), &request(), "e\u{301}ко", &report(20))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn quota_reservations_are_atomic_and_released_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let manager = JobManager::new(Store::for_test(dir.path().into()), Handle::current());
        let first = manager
            .reserve_cloud(Uuid::new_v4(), &request(), "first", &report(100))
            .unwrap();
        assert!(
            manager
                .reserve_cloud(Uuid::new_v4(), &request(), "second", &report(100))
                .is_err()
        );
        // Rejected operation did not leave its name reserved.
        let second = manager
            .reserve_cloud(Uuid::new_v4(), &request(), "second", &report(50))
            .unwrap();
        assert!(
            manager
                .reserve_cloud(Uuid::new_v4(), &request(), "third", &report(1))
                .is_err()
        );
        drop(first);
        assert!(
            manager
                .reserve_cloud(Uuid::new_v4(), &request(), "third", &report(100))
                .is_ok()
        );
        drop(second);
    }

    #[tokio::test]
    async fn admin_and_tenant_policy_views_share_one_quota_reservation() {
        let dir = tempfile::tempdir().unwrap();
        let manager = JobManager::new(Store::for_test(dir.path().into()), Handle::current());
        let mut admin = report(100);
        admin.profiles[0].href = "https://cloud.invalid/api/admin/vdcStorageProfile/1".into();
        let _first = manager
            .reserve_cloud(Uuid::new_v4(), &request(), "first", &admin)
            .unwrap();
        let mut other_catalog = report(100);
        other_catalog.catalog_href = "https://cloud.invalid/api/catalog/2".into();
        assert!(
            manager
                .reserve_cloud(Uuid::new_v4(), &request(), "second", &other_catalog)
                .is_err()
        );
    }

    #[tokio::test]
    async fn disabled_policy_is_not_reserved_and_unknown_requires_consent() {
        let dir = tempfile::tempdir().unwrap();
        let manager = JobManager::new(Store::for_test(dir.path().into()), Handle::current());
        let mut destination = report(50);
        let mut disabled = destination.profiles[0].clone();
        disabled.href.push('2');
        disabled.enabled = Some(false);
        disabled.free_bytes = Some(0);
        destination.profiles.push(disabled);
        assert!(
            manager
                .reserve_cloud(Uuid::new_v4(), &request(), "first", &destination)
                .is_ok()
        );
        destination.profiles[0].free_bytes = None;
        assert!(
            manager
                .reserve_cloud(Uuid::new_v4(), &request(), "first", &destination)
                .is_err()
        );
        let mut accepted = request();
        accepted.allow_unknown_capacity = true;
        assert!(
            manager
                .reserve_cloud(Uuid::new_v4(), &accepted, "first", &destination)
                .is_ok()
        );
    }

    #[test]
    fn downloaded_blocks_are_not_reserved_twice() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let budget = LocalBudget {
            required: 4 * 1024 * 1024,
            stage: dir.path().into(),
        };
        assert_eq!(budget.remaining(), budget.required);
        let mut file = std::fs::File::create(dir.path().join("disk.vmdk")).unwrap();
        file.write_all(&vec![0x5a; 1024 * 1024]).unwrap();
        file.sync_all().unwrap();
        let allocated = fs2::FileExt::allocated_size(&file).unwrap();
        assert!(allocated > 0);
        assert_eq!(
            budget.remaining(),
            budget.required.saturating_sub(allocated)
        );
    }

    #[test]
    fn locators_reject_duplicate_replaced_and_renamed_objects() {
        let selected = object();
        assert!(ensure_locator_target(&selected, std::slice::from_ref(&selected)).is_ok());
        let mut replacement = selected.clone();
        replacement.href.push('2');
        assert!(ensure_locator_target(&selected, std::slice::from_ref(&replacement)).is_err());
        assert!(
            ensure_locator_target(&selected, &[selected.clone(), replacement.clone()]).is_err()
        );
        replacement.catalog = "another".into();
        assert!(ensure_locator_target(&selected, &[selected.clone(), replacement]).is_ok());
        let mut renamed = selected.clone();
        renamed.name = "Image".into();
        assert!(ensure_locator_target(&selected, &[renamed]).is_err());
    }
}
