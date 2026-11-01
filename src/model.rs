use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub ovf_directory: PathBuf,
    pub download_directory: PathBuf,
    pub endpoints: Vec<Endpoint>,
    pub s3_profiles: Vec<S3Profile>,
    pub light_theme: bool,
    pub reserve_gib: u64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            ovf_directory: PathBuf::new(),
            download_directory: PathBuf::new(),
            endpoints: Vec::new(),
            s3_profiles: Vec::new(),
            light_theme: false,
            reserve_gib: 0,
        }
    }
}

#[cfg(test)]
mod settings_tests {
    use super::Settings;

    #[test]
    fn legacy_repository_setting_is_ignored_and_not_exported() {
        for value in ["", "another/project"] {
            let settings: Settings =
                serde_json::from_value(serde_json::json!({"github_repository": value})).unwrap();
            assert!(
                serde_json::to_value(settings)
                    .unwrap()
                    .get("github_repository")
                    .is_none()
            );
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub id: Uuid,
    pub name: String,
    pub url: String,
    pub tenants: Vec<Tenant>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: Uuid,
    pub organization: String,
    pub username: String,
    pub auth: AuthKind,
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum AuthKind {
    #[default]
    Password,
    ApiToken,
    BearerToken,
}

impl AuthKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Password => "Логин и пароль",
            Self::ApiToken => "API token (refresh token)",
            Self::BearerToken => "Временный Bearer token",
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct S3Profile {
    pub id: Uuid,
    pub name: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub prefix: String,
    pub access_key: String,
}

#[derive(Clone, Serialize, Deserialize, Default, zeroize::Zeroize, zeroize::ZeroizeOnDrop)]
pub struct Secret {
    pub value: String,
    pub session_token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectKind {
    Vapp,
    Vm,
    Template,
    Media,
    Catalog,
    Vdc,
}

impl ObjectKind {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Vapp => "vApp",
            Self::Vm => "ВМ",
            Self::Template => "Шаблон",
            Self::Media => "ISO",
            Self::Catalog => "Каталог",
            Self::Vdc => "Виртуальный ЦОД",
        }
    }
}

#[derive(Clone, Debug)]
pub struct CloudObject {
    pub href: String,
    pub name: String,
    pub kind: ObjectKind,
    pub vdc: String,
    pub catalog: String,
    pub parent_href: String,
    pub parent_name: String,
    pub status: String,
    pub bytes: Option<u64>,
    pub cpu: Option<u64>,
    pub memory_mb: Option<u64>,
    pub owner: String,
}

#[derive(Clone, Default)]
pub struct Inventory {
    pub objects: Vec<CloudObject>,
    pub warnings: Vec<String>,
    pub api_version: String,
    pub org_href: String,
}
impl Inventory {
    pub fn user_object_count(&self) -> usize {
        let catalogs = self
            .objects
            .iter()
            .filter(|o| o.kind == ObjectKind::Catalog && !o.owner.is_empty())
            .map(|o| o.name.as_str())
            .collect::<std::collections::HashSet<_>>();
        self.objects
            .iter()
            .filter(|o| match o.kind {
                ObjectKind::Catalog => !o.owner.is_empty(),
                ObjectKind::Template | ObjectKind::Media => catalogs.contains(o.catalog.as_str()),
                ObjectKind::Vm | ObjectKind::Vapp => !o.owner.is_empty(),
                ObjectKind::Vdc => false,
            })
            .count()
    }
}

#[cfg(test)]
mod inventory_tests {
    #[test]
    fn counts_owned_objects_without_service_images_or_vdcs() {
        let (objects,_)=crate::api::parse_records(r#"<R><CatalogRecord name="mine" ownerName="me"/><CatalogRecord name="service"/><VAppTemplateRecord catalogName="mine" name="template"/><VAppTemplateRecord catalogName="service" name="hidden"/><VMRecord name="vm" ownerName="me"/><VAppRecord name="vapp" ownerName="me"/><OrgVdcRecord name="vdc"/></R>"#).unwrap();
        assert_eq!(
            super::Inventory {
                objects,
                ..Default::default()
            }
            .user_object_count(),
            4
        );
    }
}

#[derive(Clone, Debug)]
pub struct VmState {
    pub href: String,
    pub name: String,
    pub status: String,
    pub media: Vec<MountedMedia>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountedMedia {
    pub vm_href: String,
    pub vm_name: String,
    pub href: String,
    pub name: String,
}

#[derive(Clone)]
pub struct CaptureTarget {
    pub catalog_href: String,
    pub catalog_name: String,
    pub name: String,
}

impl VmState {
    pub fn is_off(&self) -> bool {
        matches!(self.status.as_str(), "8" | "POWERED_OFF")
    }
}

#[derive(Clone)]
pub struct ExportInspection {
    pub object: CloudObject,
    pub vms: Vec<VmState>,
    pub estimated_bytes: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Download,
    Upload,
}

#[derive(Clone)]
pub struct TransferRequest {
    pub endpoint: Endpoint,
    pub tenant: Tenant,
    pub object: CloudObject,
    pub direction: Direction,
    pub local_path: PathBuf,
    pub format: String,
    pub s3: Option<S3Profile>,
    pub limit_mib: u64,
    pub estimated_bytes: u64,
    pub reserve_bytes: u64,
    pub ovf_directory: PathBuf,
    pub accept_eulas: bool,
    pub capture: Option<CaptureTarget>,
    pub expected_vms: Vec<String>,
    pub eject_media: Vec<MountedMedia>,
    pub parallel_threads: u8,
    pub allow_unknown_capacity: bool,
}

pub fn human_bytes(bytes: u64) -> String {
    let units = ["Б", "КиБ", "МиБ", "ГиБ", "ТиБ"];
    let mut n = bytes as f64;
    let mut i = 0;
    while n >= 1024.0 && i < units.len() - 1 {
        n /= 1024.0;
        i += 1;
    }
    format!("{n:.1} {}", units[i])
}

pub fn safe_filename(name: &str) -> String {
    let s: String = name
        .chars()
        .map(|c| {
            if c.is_control() || "<>:\"/\\|?*".contains(c) {
                '_'
            } else {
                c
            }
        })
        .take(100)
        .collect();
    let s = s.trim_matches([' ', '.']);
    if s.is_empty() {
        "export".into()
    } else {
        format!("vm-{s}")
    }
}
