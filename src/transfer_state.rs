//! Per-transfer observations of the original (not rewritten) Cloud Director XML.
use anyhow::{Result, ensure};
use roxmltree::Document;
use std::collections::{BTreeMap, VecDeque};

#[derive(Clone, Default, Debug)]
pub struct CloudTask {
    pub status: String,
    pub progress: Option<u8>,
    pub operation: String,
    pub error: String,
}
impl CloudTask {
    pub fn pending(&self) -> bool {
        !matches!(
            self.status.as_str(),
            "success" | "error" | "aborted" | "canceled" | "cancelled"
        )
    }
    pub fn failed(&self) -> bool {
        matches!(
            self.status.as_str(),
            "error" | "aborted" | "canceled" | "cancelled"
        )
    }
}
#[derive(Clone, Default, Debug)]
pub struct CloudTrace {
    pub tasks: BTreeMap<String, CloudTask>,
    pub targets: BTreeMap<String, bool>,
    pub current_file: String,
    events: VecDeque<String>,
}
impl CloudTrace {
    pub fn event(&mut self, message: String) {
        if self.events.len() >= 200 {
            self.events.pop_front();
        }
        self.events.push_back(crate::store::redact(&message));
    }
    pub fn drain_events(&mut self) -> Vec<String> {
        self.events.drain(..).collect()
    }
    pub fn file(&mut self, method: &str, path: &str) {
        let name = path.rsplit('/').next().unwrap_or("");
        let name = percent_encoding::percent_decode_str(name).decode_utf8_lossy();
        if name.is_empty() {
            return;
        }
        self.current_file = name.chars().filter(|c| !c.is_control()).take(200).collect();
        self.event(format!(
            "{}: {}",
            if method == "PUT" {
                "Отправляется файл"
            } else {
                "Скачивается файл"
            },
            self.current_file
        ));
    }
    pub fn observe(&mut self, xml: &str, mutation: bool) -> Result<()> {
        let doc = Document::parse(xml)?;
        let root = doc.root_element();
        let href = root.attribute("href").unwrap_or("");
        let is_resource = matches!(root.tag_name().name(), "VAppTemplate" | "Media");
        let known_resource = self.targets.contains_key(href);
        if is_resource && (mutation || known_resource) && !href.is_empty() {
            self.targets.insert(href.into(), resource_ready(xml)?);
        }
        if mutation {
            for n in doc
                .descendants()
                .filter(|n| matches!(n.tag_name().name(), "Entity" | "Owner"))
            {
                if n.attribute("type").is_some_and(|t| {
                    t.contains("vappTemplate")
                        || t.contains("vAppTemplate")
                        || t.contains(".media+")
                }) && let Some(href) = n.attribute("href")
                {
                    self.targets.entry(href.into()).or_insert(false);
                }
            }
        }
        for n in doc.descendants().filter(|n| n.has_tag_name("Task")) {
            let Some(href) = n.attribute("href") else {
                continue;
            };
            if !(mutation
                || root.has_tag_name("Task")
                || known_resource
                || self.tasks.contains_key(href))
            {
                continue;
            }
            let status = n.attribute("status").unwrap_or("unknown").to_owned();
            let progress = n
                .children()
                .find(|c| c.has_tag_name("Progress"))
                .and_then(|c| c.text())
                .and_then(|s| s.trim().parse::<u8>().ok())
                .filter(|p| *p <= 100);
            let operation = n
                .attribute("operationName")
                .or_else(|| n.attribute("operation"))
                .unwrap_or("Задача Cloud Director")
                .to_owned();
            let error = n
                .children()
                .find(|c| c.has_tag_name("Error"))
                .and_then(|c| c.attribute("message"))
                .unwrap_or("")
                .to_owned();
            if self.tasks.get(href).is_none_or(|old| old.status != status) {
                self.event(format!(
                    "Cloud Director: {operation} — {}",
                    match status.as_str() {
                        "success" => "выполнено",
                        "error" => "ошибка",
                        "running" => "выполняется",
                        "queued" | "preRunning" => "в очереди",
                        other => other,
                    }
                ));
            }
            self.tasks.insert(
                href.into(),
                CloudTask {
                    status,
                    progress,
                    operation,
                    error,
                },
            );
        }
        Ok(())
    }
    pub fn check_errors(&self) -> Result<()> {
        if let Some(task) = self.tasks.values().find(|t| t.failed()) {
            anyhow::bail!(
                "Cloud Director: {} — {}. {}",
                task.operation,
                task.status,
                crate::store::redact(&task.error)
            );
        }
        Ok(())
    }
    pub fn pending(&self) -> bool {
        self.tasks.values().any(CloudTask::pending)
    }
    pub fn progress(&self) -> Option<u8> {
        self.tasks
            .values()
            .filter(|t| t.pending())
            .filter_map(|t| t.progress)
            .min()
    }
    pub fn upload_ready(&self) -> bool {
        !self.targets.is_empty()
            && self.targets.values().all(|ready| *ready)
            && !self.pending()
            && self.check_errors().is_ok()
    }
}

pub fn resource_ready(xml: &str) -> Result<bool> {
    let doc = Document::parse(xml)?;
    let root = doc.root_element();
    ensure!(
        matches!(root.tag_name().name(), "VAppTemplate" | "Media"),
        "Ответ API не содержит созданный шаблон или ISO"
    );
    let status = root.attribute("status").unwrap_or("");
    ensure!(
        !matches!(
            status,
            "-1" | "14" | "15" | "FAILED_CREATION" | "REJECTED" | "TRANSFER_TIMEOUT"
        ),
        "Cloud Director отклонил образ: статус {status}"
    );
    if !matches!(status, "1" | "8" | "RESOLVED" | "POWERED_OFF")
        || root.attribute("ovfDescriptorUploaded") == Some("false")
    {
        return Ok(false);
    }
    for node in doc.descendants() {
        if node.has_tag_name("Task") {
            let status = node.attribute("status").unwrap_or("unknown");
            ensure!(
                !matches!(status, "error" | "aborted" | "canceled" | "cancelled"),
                "Ошибка обработки образа: {}",
                node.descendants()
                    .find(|n| n.has_tag_name("Error"))
                    .and_then(|n| n.attribute("message"))
                    .unwrap_or(status)
            );
            if status != "success" {
                return Ok(false);
            }
        }
        if node.has_tag_name("File")
            && let (Some(size), Some(sent)) =
                (node.attribute("size"), node.attribute("bytesTransferred"))
            && let (Ok(size), Ok(sent)) = (size.parse::<u64>(), sent.parse::<u64>())
            && sent < size
        {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preparation_progress_and_task_failure() {
        let mut t = CloudTrace::default();
        t.observe(r#"<Task href="https://cloud/api/task/1" status="running" operationName="enableDownload"><Progress>5</Progress></Task>"#,true).unwrap();
        assert_eq!(t.progress(), Some(5));
        assert!(t.pending());
        t.observe(r#"<Task href="https://cloud/api/task/1" status="error"><Error message="Storage unavailable"/></Task>"#,false).unwrap();
        assert!(
            t.check_errors()
                .unwrap_err()
                .to_string()
                .contains("Storage unavailable")
        );
    }
    #[test]
    fn upload_waits_for_entity_and_tasks_after_bytes_finish() {
        let mut t = CloudTrace::default();
        t.observe(r#"<VAppTemplate href="https://cloud/api/vAppTemplate/1" status="0" ovfDescriptorUploaded="true"><Tasks><Task href="https://cloud/api/task/1" status="running"><Progress>90</Progress></Task></Tasks><Files><File size="100" bytesTransferred="100"/></Files></VAppTemplate>"#,true).unwrap();
        assert!(!t.upload_ready());
        t.observe(
            r#"<Task href="https://cloud/api/task/1" status="success"/>"#,
            false,
        )
        .unwrap();
        assert!(!t.upload_ready());
        t.observe(r#"<VAppTemplate href="https://cloud/api/vAppTemplate/1" status="8" ovfDescriptorUploaded="true"/>"#,false).unwrap();
        assert!(t.upload_ready());
    }
    #[test]
    fn unrelated_inventory_tasks_are_not_tracked() {
        let mut t = CloudTrace::default();
        t.observe(
            r#"<Vdc><Tasks><Task href="other" status="error"/></Tasks></Vdc>"#,
            false,
        )
        .unwrap();
        assert!(t.tasks.is_empty());
        assert!(!resource_ready(r#"<Media status="1"><Files><File size="100" bytesTransferred="99"/></Files></Media>"#).unwrap());
    }
}
