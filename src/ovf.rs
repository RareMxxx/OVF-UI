use crate::{
    bridge::Bridge,
    model::{CloudObject, Direction, ObjectKind, TransferRequest},
    store::redact,
};
use anyhow::{Context, Result, bail, ensure};
use roxmltree::Document;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command};
use tokio_util::sync::CancellationToken;

#[path = "iso.rs"]
mod iso;

pub fn executable(directory: &Path) -> Result<PathBuf> {
    let file = directory.join(if cfg!(windows) {
        "ovftool.exe"
    } else {
        "ovftool"
    });
    ensure!(
        file.is_file(),
        "В выбранной папке не найден {}",
        if cfg!(windows) {
            "ovftool.exe"
        } else {
            "ovftool"
        }
    );
    Ok(file.canonicalize()?)
}

pub fn hide_console(command: &mut Command) {
    #[cfg(windows)]
    {
        command.creation_flags(0x08000000);
    }
    #[cfg(not(windows))]
    {
        let _ = command;
    }
}

// OVF Tool's own URL/path parser does not accept Windows verbatim paths
// returned by std::fs::canonicalize (even though Rust can read them).
pub fn tool_path(path: &Path) -> String {
    let value = path.to_string_lossy();
    #[cfg(windows)]
    {
        if let Some(unc) = value.strip_prefix(r"\\?\UNC\") {
            return format!(r"\\{unc}");
        }
        if let Some(drive) = value.strip_prefix(r"\\?\") {
            return drive.to_owned();
        }
    }
    value.into_owned()
}

pub async fn probe(directory: &Path) -> Result<String> {
    let mut cmd = Command::new(executable(directory)?);
    cmd.arg("--version").kill_on_drop(true);
    hide_console(&mut cmd);
    let output = tokio::time::timeout(Duration::from_secs(15), cmd.output())
        .await
        .context("OVF Tool не ответил за 15 секунд")??;
    ensure!(output.status.success(), "OVF Tool не запускается");
    let version = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    ensure!(!version.is_empty(), "OVF Tool не вернул версию");
    Ok(version)
}

pub fn locator(port: u16, org: &str, object: &CloudObject) -> Result<String> {
    let mut url = url::Url::parse(&format!("vcloud://127.0.0.1:{port}/"))?;
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("org", org);
        if !object.vdc.is_empty() {
            q.append_pair("vdc", &object.vdc);
        }
        if !object.catalog.is_empty() {
            q.append_pair("catalog", &object.catalog);
        }
        let key = match object.kind {
            ObjectKind::Vapp => "vapp",
            ObjectKind::Template => "vappTemplate",
            ObjectKind::Media => "media",
            _ => bail!("Этот тип объекта нельзя передать через OVF Tool"),
        };
        q.append_pair(key, &object.name);
    }
    // vCloud locators expect percent escapes rather than form-style '+'.
    Ok(url.to_string().replace('+', "%20"))
}

pub fn arguments(request: &TransferRequest, bridge: &Bridge, output: &Path) -> Result<Vec<String>> {
    let remote = locator(bridge.port, &request.tenant.organization, &request.object)?;
    let mut args = vec!["--machineOutput".into()];
    if request.parallel_threads > 1 && request.object.kind != ObjectKind::Media {
        args.push(format!(
            "--parallelThreads={}",
            request.parallel_threads.clamp(1, 16)
        ));
    }
    if request.accept_eulas {
        args.push("--acceptAllEulas".into());
    }
    match request.direction {
        Direction::Download => {
            args.push(format!("--I:sourceSessionTicket={}", bridge.ticket));
            args.push(format!("--sourceSSLThumbprint={}", bridge.thumbprint));
            args.push(format!("--targetType={}", request.format));
            args.push(remote);
            args.push(tool_path(output));
        }
        Direction::Upload => {
            args.push(format!("--I:targetSessionTicket={}", bridge.ticket));
            args.push(format!("--targetSSLThumbprint={}", bridge.thumbprint));
            if request.object.kind == ObjectKind::Media {
                args.push("--sourceType=ISO".into());
            }
            args.push(tool_path(&request.local_path));
            args.push(remote);
        }
    }
    Ok(args)
}

pub fn check_destination(directory: &Path, required: u64, reserve: u64) -> Result<u64> {
    ensure!(directory.is_dir(), "Выберите существующую папку скачивания");
    let free = fs2::available_space(directory).context("Не удалось проверить свободное место")?;
    let need = required
        .checked_add(reserve)
        .context("Слишком большой размер образа")?;
    ensure!(
        free >= need,
        "Недостаточно места: свободно {}, требуется {} с учётом резерва",
        crate::model::human_bytes(free),
        crate::model::human_bytes(need)
    );
    let mut test = tempfile::NamedTempFile::new_in(directory)
        .context("В выбранную папку нельзя записывать файлы")?;
    test.write_all(b"OVF Dock write check")?;
    test.as_file().sync_all()?;
    Ok(free)
}

pub fn parse_progress(line: &str) -> Option<f32> {
    static PERCENT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let re = PERCENT.get_or_init(|| {
        regex::Regex::new(
            r"(?i)(?:progress\s*[:=]?\s*|transfer completed\s*[:=]?\s*)(\d+(?:\.\d+)?)\s*%?",
        )
        .unwrap()
    });
    let value = re.captures(line)?.get(1)?.as_str().parse::<f32>().ok()?;
    Some((value / 100.0).clamp(0.0, 1.0))
}

#[derive(Default)]
pub struct MachineOutput {
    section: String,
    pub failed: bool,
    pub success: bool,
    pub needs_input: bool,
    pub preparing: bool,
    error_xml: String,
}
impl MachineOutput {
    pub fn feed(&mut self, line: &str) -> Option<f32> {
        let line = line.trim();
        if let Some(detail) = line.strip_prefix('+') {
            let detail = detail.trim();
            if self.section == "TASK" {
                self.preparing = detail != "done";
            }
            if self.section == "ERROR" && self.error_xml.len() < 64 * 1024 {
                self.error_xml.push_str(detail);
                self.error_xml.push('\n');
            }
            if self.section == "PROGRESS" {
                return detail
                    .parse::<f32>()
                    .ok()
                    .map(|n| (n / 100.0).clamp(0.0, 1.0));
            }
            if self.section == "RESULT" {
                self.failed |= detail.starts_with("ERROR");
                self.success |= detail == "SUCCESS";
            }
        } else if [
            "PROGRESS",
            "RESULT",
            "AUTHENTICATION",
            "EULA",
            "PROBE",
            "MANIFEST",
            "CERTIFICATE",
            "TARGET_ID",
            "VALIDATEHOST",
            "ERROR",
            "TASK",
        ]
        .contains(&line)
        {
            self.section = line.into();
            if line == "TASK" {
                self.preparing = true;
            }
            if line == "PROGRESS" || line == "RESULT" {
                self.preparing = false;
            }
            self.needs_input |= line == "AUTHENTICATION" || line == "EULA";
        }
        parse_progress(line)
    }
    pub fn error_summary(&self) -> String {
        if let Ok(doc) = Document::parse(&self.error_xml) {
            let kind = doc
                .descendants()
                .find(|n| n.has_tag_name("Type"))
                .and_then(|n| n.text())
                .unwrap_or("ovftool.error");
            let detail = doc
                .descendants()
                .filter(|n| n.has_tag_name("LocalizedMsg"))
                .filter_map(|n| n.text())
                .map(str::trim)
                .collect::<Vec<_>>()
                .join("; ");
            if kind == "ovftool.login" {
                return format!(
                    "OVF Tool не смог открыть сессию Cloud Director ({kind}). Проверьте журнал HTTP ниже; при HTTP 401 переподключите тенант, при 403 проверьте права, при 406 — совместимость API."
                );
            }
            return format!("{detail} ({kind})");
        }
        "OVF Tool не сообщил текст причины. Проверьте журнал передачи и доступность endpoint."
            .into()
    }
}

pub async fn read_lines<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    sender: tokio::sync::mpsc::UnboundedSender<String>,
) {
    let mut buf = [0u8; 4096];
    let mut pending = Vec::new();
    while let Ok(n) = reader.read(&mut buf).await {
        if n == 0 {
            break;
        }
        for b in &buf[..n] {
            if *b == b'\n' || *b == b'\r' {
                if !pending.is_empty() {
                    let _ = sender.send(redact(&String::from_utf8_lossy(&pending)));
                    pending.clear();
                }
            } else if pending.len() < 16 * 1024 {
                pending.push(*b);
            }
        }
    }
    if !pending.is_empty() {
        let _ = sender.send(redact(&String::from_utf8_lossy(&pending)));
    }
}

#[derive(Clone, Debug, Default)]
pub struct ValidationReport {
    pub lines: Vec<String>,
    pub bytes: u64,
    /// Conservative remote storage estimate: provisioned virtual disks or package bytes.
    pub provisioned_bytes: u64,
    pub warnings: Vec<String>,
}

#[derive(Default)]
struct FileInfo {
    len: u64,
    sha1: String,
    sha256: String,
}

fn normalized_member(name: &str) -> Result<String> {
    let decoded = percent_encoding::percent_decode_str(name)
        .decode_utf8()
        .context("Некорректный путь в OVF")?;
    ensure!(
        !decoded.contains('\\') && !decoded.contains(':') && !decoded.contains('\0'),
        "Недопустимый путь в пакете"
    );
    let path = Path::new(decoded.as_ref());
    ensure!(
        !path.is_absolute()
            && path
                .components()
                .all(|c| matches!(c, Component::Normal(_) | Component::CurDir)),
        "Выход за границы пакета"
    );
    let value = path
        .components()
        .filter_map(|c| {
            if let Component::Normal(s) = c {
                Some(s.to_string_lossy().into_owned())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("/");
    ensure!(!value.is_empty(), "Пустой путь в пакете");
    Ok(value)
}

fn digest_reader(reader: &mut impl Read, cancel: &CancellationToken) -> Result<FileInfo> {
    let mut a = Sha1::new();
    let mut b = Sha256::new();
    let mut len = 0;
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        ensure!(!cancel.is_cancelled(), "Проверка отменена");
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        a.update(&buf[..n]);
        b.update(&buf[..n]);
        len += n as u64;
    }
    Ok(FileInfo {
        len,
        sha1: hex::encode(a.finalize()),
        sha256: hex::encode(b.finalize()),
    })
}

struct Descriptor {
    references: Vec<(String, Option<u64>)>,
    provisioned_bytes: u64,
}

fn capacity_bytes(capacity: &str, units: Option<&str>) -> Result<u64> {
    let capacity: u64 = capacity.parse().context("Некорректная ёмкость диска OVF")?;
    let unit = units
        .unwrap_or("byte")
        .split_whitespace()
        .collect::<String>();
    let factor = if unit == "byte" {
        1
    } else {
        let powers = unit
            .strip_prefix("byte*")
            .context("Неподдерживаемые единицы capacityAllocationUnits в OVF")?;
        let (base, exponent) = powers
            .split_once('^')
            .context("Некорректные единицы ёмкости диска OVF")?;
        let base: u64 = base.parse()?;
        ensure!(
            matches!(base, 2 | 10),
            "Неподдерживаемое основание единиц ёмкости OVF"
        );
        base.checked_pow(exponent.parse()?)
            .context("Переполнение единиц ёмкости диска OVF")?
    };
    capacity
        .checked_mul(factor)
        .context("Переполнение ёмкости диска OVF")
}

fn references(xml: &str) -> Result<Descriptor> {
    let doc = Document::parse(xml).context("OVF содержит некорректный XML")?;
    ensure!(
        doc.root_element().has_tag_name("Envelope"),
        "Не найден OVF Envelope"
    );
    ensure!(
        doc.descendants()
            .any(|n| n.has_tag_name("VirtualSystem") || n.has_tag_name("VirtualSystemCollection")),
        "В OVF нет описания виртуальных машин"
    );
    let mut result = Vec::new();
    let mut file_ids = BTreeSet::new();
    let mut file_names = BTreeSet::new();
    for n in doc.descendants().filter(|n| n.has_tag_name("File")) {
        let a = |name| n.attributes().find(|a| a.name() == name).map(|a| a.value());
        let href = a("href").context("У File отсутствует href")?;
        let decoded = percent_encoding::percent_decode_str(href).decode_utf8_lossy();
        ensure!(
            !decoded.to_ascii_lowercase().ends_with(".iso"),
            "OVF ссылается на ISO «{decoded}». Для загрузки в vCD извлеките ISO из ВМ и экспортируйте пакет заново. Дескриптор и манифест автоматически не изменяются."
        );
        let id = a("id")
            .filter(|id| !id.is_empty())
            .context("У File отсутствует id")?;
        ensure!(
            file_ids.insert(id.to_owned()),
            "Повторяющийся File id в OVF: {id}"
        );
        let size = a("size").map(str::parse).transpose()?;
        let name = normalized_member(href)?;
        ensure!(
            file_names.insert(name.clone()),
            "Повторяющаяся ссылка File в OVF: {name}"
        );
        result.push((name, size));
    }
    let mut disk_ids = BTreeSet::new();
    let mut provisioned_bytes = 0u64;
    for disk in doc.descendants().filter(|n| n.has_tag_name("Disk")) {
        let a = |name| {
            disk.attributes()
                .find(|a| a.name() == name)
                .map(|a| a.value())
        };
        let id = a("diskId")
            .filter(|id| !id.is_empty())
            .context("У Disk отсутствует diskId")?;
        ensure!(
            disk_ids.insert(id.to_owned()),
            "Повторяющийся diskId в OVF: {id}"
        );
        if let Some(reference) = a("fileRef").filter(|s| !s.is_empty()) {
            ensure!(
                file_ids.contains(reference),
                "Disk {id} ссылается на отсутствующий File id: {reference}"
            );
        }
        let capacity = capacity_bytes(
            a("capacity").context("У Disk отсутствует capacity")?,
            a("capacityAllocationUnits"),
        )?;
        provisioned_bytes = provisioned_bytes
            .checked_add(capacity)
            .context("Суммарная ёмкость дисков OVF слишком велика")?;
    }
    for item in doc.descendants().filter(|n| n.has_tag_name("Item")) {
        let kind = item
            .children()
            .find(|n| n.has_tag_name("ResourceType"))
            .and_then(|n| n.text());
        if matches!(kind, Some("15" | "16")) {
            ensure!(
                !item.children().any(|n| n.has_tag_name("HostResource")
                    && n.text().is_some_and(|s| !s.trim().is_empty())),
                "В OVF есть ссылка на носитель CD/DVD. Извлеките ISO и экспортируйте пакет заново; пустой CD/DVD-привод допустим."
            );
        }
    }
    Ok(Descriptor {
        references: result,
        provisioned_bytes,
    })
}

fn check_manifest(
    manifest: &str,
    directory: &Path,
    files: &BTreeMap<String, FileInfo>,
) -> Result<usize> {
    let mut count = 0;
    let mut covered = BTreeSet::new();
    let mut entries = BTreeSet::new();
    for line in manifest.lines().filter(|l| !l.trim().is_empty()) {
        let (algorithm, rest) = line
            .split_once('(')
            .context("Некорректная строка манифеста")?;
        let (name, hash) = rest
            .rsplit_once(")=")
            .or_else(|| rest.rsplit_once(") ="))
            .context("Некорректная строка манифеста")?;
        let name = directory
            .join(normalized_member(name)?)
            .to_string_lossy()
            .replace('\\', "/");
        ensure!(
            entries.insert((algorithm.trim().to_uppercase(), name.clone())),
            "Повторяющаяся запись манифеста: {name}"
        );
        let file = files
            .get(&name)
            .with_context(|| format!("Файл из манифеста отсутствует: {name}"))?;
        let actual = match algorithm.trim().to_uppercase().as_str() {
            "SHA1" => &file.sha1,
            "SHA256" => &file.sha256,
            _ => bail!("Не поддерживается алгоритм манифеста: {}", algorithm.trim()),
        };
        ensure!(
            actual.eq_ignore_ascii_case(hash.trim()),
            "Контрольная сумма не совпадает: {name}"
        );
        covered.insert(name);
        count += 1;
    }
    ensure!(count > 0, "Манифест пуст");
    for name in files.keys().filter(|name| {
        !name.to_ascii_lowercase().ends_with(".mf") && !name.to_ascii_lowercase().ends_with(".cert")
    }) {
        ensure!(
            covered.contains(name),
            "Манифест не содержит контрольную сумму файла: {name}"
        );
    }
    Ok(count)
}

pub fn validate_package(path: &Path, cancel: &CancellationToken) -> Result<ValidationReport> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    ensure!(
        matches!(ext.as_str(), "ova" | "ovf" | "iso"),
        "Поддерживаются OVA, OVF и ISO"
    );
    if ext == "iso" {
        let structure = iso::validate(path, cancel)?;
        let info = digest_reader(&mut File::open(path)?, cancel)?;
        return Ok(ValidationReport {
            lines: vec![
                structure,
                format!(
                    "ISO прочитан полностью: {}",
                    crate::model::human_bytes(info.len)
                ),
                format!("SHA256: {}", info.sha256),
            ],
            bytes: info.len,
            provisioned_bytes: info.len,
            warnings: vec![
                "Проверены основные дескрипторы и границы ISO, прочитан весь файл. Полная целостность файловой системы, загрузочность и содержимое ОС не проверяются; для сверки с оригиналом сравните SHA256.".into(),
            ],
        });
    }
    let mut files = BTreeMap::new();
    let mut descriptors = Vec::new();
    let mut manifests = Vec::new();
    if ext == "ova" {
        let mut archive = tar::Archive::new(File::open(path)?);
        for entry in archive.entries()? {
            ensure!(!cancel.is_cancelled(), "Проверка отменена");
            let mut entry = entry?;
            if entry.header().entry_type().is_dir() {
                continue;
            }
            ensure!(
                entry.header().entry_type().is_file(),
                "OVA содержит ссылку или специальный файл"
            );
            let name = normalized_member(&entry.path()?.to_string_lossy())?;
            ensure!(
                !name.to_ascii_lowercase().ends_with(".iso"),
                "OVA содержит ISO «{name}». Экспортируйте пакет без подключённого ISO."
            );
            ensure!(
                !files.contains_key(&name),
                "Повторяющийся файл в OVA: {name}"
            );
            let info =
                if name.to_lowercase().ends_with(".ovf") || name.to_lowercase().ends_with(".mf") {
                    ensure!(
                        entry.size() <= 16 * 1024 * 1024,
                        "Описание или манифест превышает 16 МиБ"
                    );
                    let mut data = Vec::new();
                    entry.read_to_end(&mut data)?;
                    let text = String::from_utf8(data.clone())?;
                    if name.to_lowercase().ends_with(".ovf") {
                        descriptors.push((name.clone(), text));
                    } else {
                        manifests.push((name.clone(), text));
                    }
                    digest_reader(&mut data.as_slice(), cancel)?
                } else {
                    digest_reader(&mut entry, cancel)?
                };
            ensure!(info.len == entry.size(), "Файл в OVA усечён: {name}");
            files.insert(name, info);
        }
    } else {
        ensure!(
            fs::metadata(path)?.len() <= 16 * 1024 * 1024,
            "OVF превышает 16 МиБ"
        );
        let xml = fs::read_to_string(path)?;
        let parent = path
            .parent()
            .context("Не найдена папка OVF")?
            .canonicalize()?;
        let name = path
            .file_name()
            .context("Не найдено имя OVF")?
            .to_string_lossy()
            .into_owned();
        files.insert(name.clone(), digest_reader(&mut File::open(path)?, cancel)?);
        for (href, _) in references(&xml)?.references {
            let target = parent
                .join(&href)
                .canonicalize()
                .with_context(|| format!("Файл не найден: {href}"))?;
            ensure!(
                target.starts_with(&parent),
                "Ссылка ведёт за пределы папки OVF"
            );
            ensure!(
                !files.contains_key(&href),
                "File ссылается на сам OVF-дескриптор: {href}"
            );
            files.insert(href, digest_reader(&mut File::open(target)?, cancel)?);
        }
        descriptors.push((name, xml));
        let manifest = path.with_extension("mf");
        if manifest.exists() {
            ensure!(
                fs::metadata(&manifest)?.len() <= 16 * 1024 * 1024,
                "Манифест слишком большой"
            );
            manifests.push((
                manifest.file_name().unwrap().to_string_lossy().into_owned(),
                fs::read_to_string(manifest)?,
            ));
        }
    }
    ensure!(
        descriptors.len() == 1,
        "Пакет должен содержать ровно один OVF-дескриптор"
    );
    let (descriptor_name, xml) = &descriptors[0];
    let dir = Path::new(descriptor_name).parent().unwrap_or(Path::new(""));
    let descriptor = references(xml)?;
    for (href, expected) in descriptor.references {
        let joined = dir.join(&href).to_string_lossy().replace('\\', "/");
        ensure!(
            joined != *descriptor_name,
            "File ссылается на сам OVF-дескриптор"
        );
        let file = files
            .get(&joined)
            .with_context(|| format!("Файл OVF отсутствует: {joined}"))?;
        if let Some(size) = expected {
            ensure!(size == file.len, "Размер файла не совпадает с OVF: {href}");
        }
    }
    let mut report = ValidationReport {
        bytes: files
            .values()
            .try_fold(0u64, |total, f| total.checked_add(f.len))
            .context("Размер пакета слишком велик")?,
        provisioned_bytes: descriptor.provisioned_bytes,
        lines: vec![
            "Структура OVF и ссылки на файлы корректны".into(),
            format!("Прочитано файлов: {}", files.len()),
        ],
        warnings: Vec::new(),
    };
    report.provisioned_bytes = report.provisioned_bytes.max(report.bytes);
    report.lines.push(format!(
        "Оценка места в каталоге: {}",
        crate::model::human_bytes(report.provisioned_bytes)
    ));
    if manifests.is_empty() {
        report.warnings.push(
            "Манифест отсутствует: сравнить контрольные суммы с оригиналом невозможно.".into(),
        );
    }
    ensure!(manifests.len() <= 1, "Пакет содержит несколько манифестов");
    for (name, manifest) in manifests {
        let n = check_manifest(
            &manifest,
            Path::new(&name).parent().unwrap_or(Path::new("")),
            &files,
        )?;
        report
            .lines
            .push(format!("Контрольные суммы манифеста совпали: {n}"));
    }
    Ok(report)
}

pub async fn verify_with_tool(
    directory: &Path,
    path: &Path,
    cancel: CancellationToken,
) -> Result<String> {
    let mut cmd = Command::new(executable(directory)?);
    cmd.arg("--verifyOnly")
        .arg(tool_path(path))
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    hide_console(&mut cmd);
    let output = tokio::select! {_=cancel.cancelled()=>bail!("Проверка отменена"),result=tokio::time::timeout(Duration::from_secs(3600),cmd.output())=>result.context("Проверка OVF Tool превысила один час")??};
    let text = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    ensure!(
        output.status.success(),
        "OVF Tool отклонил пакет: {}",
        redact(&text)
    );
    Ok(redact(&text))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(windows)]
    #[test]
    fn converts_windows_verbatim_paths_for_ovf_tool() {
        assert_eq!(
            tool_path(Path::new(r"\\?\C:\images\test.ova")),
            r"C:\images\test.ova"
        );
        assert_eq!(
            tool_path(Path::new(r"\\?\UNC\server\share\test.ovf")),
            r"\\server\share\test.ovf"
        );
    }
    #[test]
    fn rejects_iso_reference_and_connected_cd_but_accepts_empty_drive_and_standalone_iso() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("vm.ovf");
        fs::write(&p,r#"<Envelope xmlns:ovf="urn:ovf"><References><File ovf:href="boot%2eISO"/></References><VirtualSystem/></Envelope>"#).unwrap();
        assert!(
            validate_package(&p, &CancellationToken::new())
                .unwrap_err()
                .to_string()
                .contains("ISO")
        );
        fs::write(&p,r#"<Envelope><VirtualSystem><VirtualHardwareSection><Item><ResourceType>15</ResourceType><HostResource>ovf:/file/boot</HostResource></Item></VirtualHardwareSection></VirtualSystem></Envelope>"#).unwrap();
        assert!(validate_package(&p, &CancellationToken::new()).is_err());
        fs::write(&p,r#"<Envelope><VirtualSystem><VirtualHardwareSection><Item><ResourceType>15</ResourceType><HostResource/></Item></VirtualHardwareSection></VirtualSystem></Envelope>"#).unwrap();
        assert!(validate_package(&p, &CancellationToken::new()).is_ok());
        let iso = d.path().join("boot.iso");
        fs::write(&iso, iso::fixture_iso()).unwrap();
        assert!(validate_package(&iso, &CancellationToken::new()).is_ok());
    }
    #[test]
    fn rejects_unreferenced_iso_inside_ova() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("vm.ova");
        let mut archive = tar::Builder::new(File::create(&p).unwrap());
        for (name, data) in [
            ("vm.ovf", "<Envelope><VirtualSystem/></Envelope>"),
            ("boot.ISO", "iso"),
        ] {
            let mut h = tar::Header::new_gnu();
            h.set_size(data.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            archive.append_data(&mut h, name, data.as_bytes()).unwrap();
        }
        archive.finish().unwrap();
        drop(archive);
        assert!(
            validate_package(&p, &CancellationToken::new())
                .unwrap_err()
                .to_string()
                .contains("ISO")
        );
    }
    #[test]
    fn refuses_path_traversal() {
        for name in [
            "../disk.vmdk",
            "%2e%2e/disk.vmdk",
            "C:/a",
            "/root/a",
            "a\\b",
        ] {
            assert!(normalized_member(name).is_err(), "{name}");
        }
    }
    #[test]
    fn parses_progress() {
        assert_eq!(parse_progress("PROGRESS 37"), Some(0.37));
        assert_eq!(parse_progress("Transfer Completed: 90%"), Some(0.9));
        assert_eq!(parse_progress("disk 500GB"), None);
    }
    #[test]
    fn handles_documented_multiline_machine_output() {
        let mut p = MachineOutput::default();
        assert_eq!(p.feed("PROGRESS"), None);
        assert_eq!(p.feed("+ 42"), Some(0.42));
        p.feed("RESULT");
        p.feed("+ ERROR");
        assert!(p.failed);
        assert_eq!(p.feed("+ 99"), None);
    }
    #[test]
    fn real_ovftool_login_error_has_actionable_summary() {
        let mut machine = MachineOutput::default();
        for line in "ERROR\n+ <Errors>\n+ <Error>\n+ <Type>ovftool.login</Type>\n+ <LocalizedMsg>\n+ Failed to login: \n+ </LocalizedMsg>\n+ <Arg>\n+ \n+ </Arg>\n+ </Error>\n+ </Errors>\nRESULT\n+ ERROR".lines(){machine.feed(line);}
        assert!(machine.failed);
        assert!(machine.error_summary().contains("HTTP 401"));
        assert!(machine.error_summary().contains("406"));
    }
    #[test]
    fn detects_missing_disk() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("vm.ovf");
        fs::write(&p,r#"<Envelope xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1"><References><File ovf:href="disk.vmdk" ovf:size="12"/></References><VirtualSystem/></Envelope>"#).unwrap();
        assert!(validate_package(&p, &CancellationToken::new()).is_err());
    }
    #[test]
    fn checks_manifest_and_disk_size() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("vm.ovf");
        let xml = r#"<Envelope xmlns:ovf="http://schemas.dmtf.org/ovf/envelope/1"><References><File ovf:id="file1" ovf:href="disk.vmdk" ovf:size="4"/></References><VirtualSystem/></Envelope>"#;
        fs::write(&p, xml).unwrap();
        fs::write(d.path().join("disk.vmdk"), b"disk").unwrap();
        fs::write(
            d.path().join("vm.mf"),
            format!(
                "SHA256(vm.ovf)= {}\nSHA256(disk.vmdk)= {}\n",
                hex::encode(Sha256::digest(xml.as_bytes())),
                hex::encode(Sha256::digest(b"disk"))
            ),
        )
        .unwrap();
        assert!(validate_package(&p, &CancellationToken::new()).is_ok());
        fs::write(d.path().join("disk.vmdk"), b"oops").unwrap();
        assert!(validate_package(&p, &CancellationToken::new()).is_err());
    }

    #[test]
    fn validates_disk_capacity_ids_and_manifest_coverage() {
        let xml = r#"<Envelope xmlns:o="urn:ovf"><References><File o:id="file1" o:href="disk.vmdk"/></References><DiskSection><Disk o:diskId="disk1" o:fileRef="file1" o:capacity="2" o:capacityAllocationUnits="byte * 2^30"/><Disk o:diskId="blank" o:capacity="1000"/></DiskSection><VirtualSystem/></Envelope>"#;
        assert_eq!(
            references(xml).unwrap().provisioned_bytes,
            2 * 1024 * 1024 * 1024 + 1000
        );
        for broken in [xml.replace("o:fileRef=\"file1\"", "o:fileRef=\"missing\""), xml.replace("o:diskId=\"blank\"", "o:diskId=\"disk1\""), xml.replace("2^30", "2^100"), xml.replace("<DiskSection>", "<References><File o:id=\"file1\" o:href=\"other.vmdk\"/></References><DiskSection>")] {
            assert!(references(&broken).is_err());
        }
        let d = tempfile::tempdir().unwrap();
        let ovf = d.path().join("vm.ovf");
        fs::write(&ovf, xml).unwrap();
        fs::write(d.path().join("disk.vmdk"), b"disk").unwrap();
        let report = validate_package(&ovf, &CancellationToken::new()).unwrap();
        assert_eq!(report.provisioned_bytes, 2 * 1024 * 1024 * 1024 + 1000);
        fs::write(
            d.path().join("vm.mf"),
            format!("SHA256(vm.ovf)= {}", hex::encode(Sha256::digest(xml))),
        )
        .unwrap();
        let error = validate_package(&ovf, &CancellationToken::new())
            .unwrap_err()
            .to_string();
        assert!(error.contains("Манифест не содержит") && error.contains("disk.vmdk"));
    }

    #[test]
    fn nested_ova_manifest_paths_are_relative_to_manifest() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("nested.ova");
        let xml = r#"<Envelope xmlns:o="urn:ovf"><References><File o:id="file1" o:href="disk.vmdk"/></References><VirtualSystem/></Envelope>"#;
        let manifest = format!(
            "SHA256(vm.ovf)= {}\nSHA256(disk.vmdk)= {}",
            hex::encode(Sha256::digest(xml)),
            hex::encode(Sha256::digest(b"disk"))
        );
        let mut archive = tar::Builder::new(File::create(&path).unwrap());
        for (name, data) in [
            ("nested/vm.ovf", xml),
            ("nested/disk.vmdk", "disk"),
            ("nested/vm.mf", manifest.as_str()),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            archive
                .append_data(&mut header, name, data.as_bytes())
                .unwrap();
        }
        archive.finish().unwrap();
        drop(archive);
        assert!(validate_package(&path, &CancellationToken::new()).is_ok());
    }

    #[test]
    #[ignore = "Writes local QA fixtures only when explicitly requested"]
    fn write_local_iso_qa_fixtures() {
        let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join(".local/qa-media");
        fs::create_dir_all(&directory).unwrap();
        let image = iso::fixture_iso();
        fs::write(directory.join("ovf-ui-valid-nonbootable.iso"), &image).unwrap();
        fs::write(
            directory.join("ovf-ui-truncated.iso"),
            &image[..image.len() - 2048],
        )
        .unwrap();
        fs::write(directory.join("ovf-ui-random.iso"), vec![0xA5; 49152]).unwrap();
    }
}
