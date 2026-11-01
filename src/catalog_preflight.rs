//! Read-only checks immediately before creating catalog content.
use crate::{api::CloudSession, model::human_bytes};
use anyhow::{Context, Result, ensure};
use roxmltree::Document;
use unicode_normalization::UnicodeNormalization;

/// Share the same conservative identity rule between cloud checks and in-flight
/// reservations, including composed/decomposed Unicode names.
pub fn normalized_name(name: &str) -> String {
    name.trim()
        .nfc()
        .collect::<String>()
        .to_lowercase()
        .nfc()
        .collect()
}

#[derive(Clone, Debug)]
pub struct CatalogStorageProfile {
    pub href: String,
    pub name: String,
    pub enabled: Option<bool>,
    pub limit_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub free_bytes: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct CatalogPreflight {
    pub catalog_href: String,
    pub catalog_name: String,
    pub required_bytes: u64,
    pub profiles: Vec<CatalogStorageProfile>,
    pub warnings: Vec<String>,
    pub capacity_verified: bool,
}

impl CatalogPreflight {
    /// Multiple catalog profiles are possible. We cannot assume which one vCD
    /// chooses or add their quotas together as if they formed one disk.
    pub fn check_capacity(&self) -> Result<()> {
        ensure!(
            self.profiles.is_empty() || self.profiles.iter().any(|p| p.enabled != Some(false)),
            "Все дисковые политики каталога «{}» отключены",
            self.catalog_name
        );
        let candidates: Vec<_> = self
            .profiles
            .iter()
            .filter(|p| p.enabled != Some(false))
            .collect();
        if self.required_bytes > 0
            && !candidates.is_empty()
            && candidates
                .iter()
                .all(|p| p.free_bytes.is_some_and(|free| free < self.required_bytes))
        {
            anyhow::bail!(
                "Недостаточно квоты на дисковой политике каталога «{}»: требуется {}; {}",
                self.catalog_name,
                human_bytes(self.required_bytes),
                self.profile_summary()
            );
        }
        Ok(())
    }

    fn profile_summary(&self) -> String {
        self.profiles
            .iter()
            .map(|p| {
                format!(
                    "{}: {}",
                    p.name,
                    if p.enabled == Some(false) {
                        "отключена".into()
                    } else if let Some(free) = p.free_bytes {
                        format!("свободно по квоте {}", human_bytes(free))
                    } else {
                        "доступный объём неизвестен (квота не задана или API не вернул данные)"
                            .into()
                    }
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    pub fn summary(&self) -> String {
        let state = if self.capacity_verified {
            "По известной квоте место достаточно"
        } else {
            "Достаточность места не подтверждена"
        };
        let mut parts = vec![format!(
            "Каталог «{}». {}. Требуется {}.",
            self.catalog_name,
            state,
            human_bytes(self.required_bytes)
        )];
        if !self.profiles.is_empty() {
            parts.push(self.profile_summary());
        }
        parts.extend(self.warnings.iter().cloned());
        parts.join(" ")
    }
}

type CatalogDetails = (Vec<(String, String)>, Option<String>);
fn catalog_details(xml: &str, name: &str) -> Result<CatalogDetails> {
    let doc = Document::parse(xml)?;
    let root = doc.root_element();
    ensure!(
        matches!(root.tag_name().name(), "Catalog" | "AdminCatalog"),
        "API вернул не каталог"
    );
    for item in root.descendants().filter(|n| n.has_tag_name("CatalogItem")) {
        ensure!(
            item.attribute("name")
                .is_none_or(|existing| normalized_name(existing) != normalized_name(name)),
            "Объект с именем «{name}» уже существует в каталоге. Выберите другое имя; перезапись отключена."
        );
    }
    let refs = root
        .children()
        .filter(|n| n.has_tag_name("CatalogStorageProfiles"))
        .flat_map(|n| n.children())
        .filter(|n| n.has_tag_name("VdcStorageProfile"))
        .filter_map(|n| {
            n.attribute("href").map(|href| {
                (
                    href.to_owned(),
                    n.attribute("name")
                        .unwrap_or("Дисковая политика")
                        .to_owned(),
                )
            })
        })
        .collect();
    let admin = root
        .children()
        .find(|n| {
            n.has_tag_name("Link")
                && n.attribute("type") == Some("application/vnd.vmware.admin.catalog+xml")
        })
        .and_then(|n| n.attribute("href"))
        .map(str::to_owned);
    Ok((refs, admin))
}

fn storage_profile(xml: &str, href: &str, fallback_name: &str) -> Result<CatalogStorageProfile> {
    let doc = Document::parse(xml)?;
    let root = doc.root_element();
    ensure!(
        matches!(
            root.tag_name().name(),
            "VdcStorageProfile" | "AdminVdcStorageProfile"
        ),
        "API вернул не дисковую политику"
    );
    let value = |tag| {
        root.children()
            .find(|n| n.has_tag_name(tag))
            .and_then(|n| n.text())
            .map(str::trim)
    };
    let number = |tag| -> Result<Option<u64>> {
        value(tag)
            .map(str::parse)
            .transpose()
            .with_context(|| format!("Некорректный {tag} дисковой политики"))
    };
    let enabled = value("Enabled")
        .map(|v| match v {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            _ => anyhow::bail!("Некорректный Enabled дисковой политики"),
        })
        .transpose()?;
    // API currently defines Units=MB. Never interpret an unknown unit as bytes.
    let limit_bytes = if value("Units") == Some("MB") {
        number("Limit")?
            .filter(|v| *v > 0)
            .map(|v| v.checked_mul(1024 * 1024).context("Переполнение квоты"))
            .transpose()?
    } else {
        None
    };
    let used_bytes = number("StorageUsedMB")?
        .map(|v| v.checked_mul(1024 * 1024).context("Переполнение объёма"))
        .transpose()?;
    Ok(CatalogStorageProfile {
        href: href.into(),
        name: root.attribute("name").unwrap_or(fallback_name).into(),
        enabled,
        limit_bytes,
        used_bytes,
        free_bytes: limit_bytes
            .zip(used_bytes)
            .map(|(limit, used)| limit.saturating_sub(used)),
    })
}

#[derive(Clone, Debug)]
struct StorageProfileRecord {
    profile: CatalogStorageProfile,
    vdc_href: String,
    vdc_name: String,
    is_default: bool,
}

fn profile_records(xml: &str) -> Result<(Vec<StorageProfileRecord>, Option<String>)> {
    let doc = Document::parse(xml)?;
    let mut records = Vec::new();
    for node in doc
        .root_element()
        .children()
        .filter(|n| n.has_tag_name("OrgVdcStorageProfileRecord"))
    {
        let mb = |key: &str| -> Result<Option<u64>> {
            node.attribute(key)
                .map(|s| {
                    s.parse::<u64>()
                        .context("Некорректная квота в Query API")?
                        .checked_mul(1024 * 1024)
                        .context("Переполнение квоты Query API")
                })
                .transpose()
        };
        let limit_bytes = mb("storageLimitMB")?.filter(|n| *n > 0);
        let used_bytes = mb("storageUsedMB")?;
        let enabled = match node.attribute("isEnabled") {
            Some("true" | "1") => Some(true),
            Some("false" | "0") => Some(false),
            _ => None,
        };
        records.push(StorageProfileRecord {
            profile: CatalogStorageProfile {
                href: node
                    .attribute("href")
                    .context("Query API не вернул адрес дисковой политики")?
                    .into(),
                name: node.attribute("name").unwrap_or("Дисковая политика").into(),
                enabled,
                limit_bytes,
                used_bytes,
                free_bytes: limit_bytes
                    .zip(used_bytes)
                    .map(|(limit, used)| limit.saturating_sub(used)),
            },
            vdc_href: node.attribute("vdc").unwrap_or("").into(),
            vdc_name: node.attribute("vdcName").unwrap_or("").into(),
            is_default: matches!(
                node.attribute("isDefaultStorageProfile"),
                Some("true" | "1")
            ),
        });
    }
    let next = doc
        .root_element()
        .children()
        .find(|n| n.has_tag_name("Link") && n.attribute("rel") == Some("nextPage"))
        .and_then(|n| n.attribute("href"))
        .map(str::to_owned);
    Ok((records, next))
}

fn inherited_profile(records: &[StorageProfileRecord], vdc: &str) -> Option<CatalogStorageProfile> {
    if vdc.trim().is_empty() {
        return None;
    }
    let defaults: Vec<_> = records
        .iter()
        .filter(|r| r.is_default && (r.vdc_href == vdc || r.vdc_name == vdc))
        .collect();
    // Equal VDC display names must never select a policy by first match.
    (defaults.len() == 1).then(|| defaults[0].profile.clone())
}

fn same_profile(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    match (url::Url::parse(left), url::Url::parse(right)) {
        (Ok(a), Ok(b)) => {
            a.origin() == b.origin() && a.path().rsplit('/').next() == b.path().rsplit('/').next()
        }
        _ => false,
    }
}

impl CloudSession {
    async fn catalog_storage_records(&self) -> Result<Vec<StorageProfileRecord>> {
        let mut next =
            Some("api/query?type=orgVdcStorageProfile&format=records&pageSize=128".to_owned());
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        while let Some(href) = next.take() {
            ensure!(
                seen.len() < 1000 && seen.insert(href.clone()),
                "Зацикленная пагинация дисковых политик"
            );
            let (mut records, link) = profile_records(&self.get(&href).await?)?;
            result.append(&mut records);
            next = link;
        }
        Ok(result)
    }

    pub async fn catalog_preflight(
        &self,
        catalog_href: &str,
        name: &str,
        vdc: &str,
        required_bytes: u64,
    ) -> Result<CatalogPreflight> {
        ensure!(!name.trim().is_empty(), "Введите имя образа");
        ensure!(
            name == name.trim() && !name.chars().any(char::is_control),
            "Имя не должно содержать управляющие символы или пробелы по краям"
        );
        let catalogs = self.query("catalog").await?;
        let catalog = catalogs
            .iter()
            .find(|c| c.href == catalog_href)
            .context("Выбранный каталог больше недоступен. Обновите список каталогов")?;
        ensure!(
            catalogs
                .iter()
                .filter(|c| normalized_name(&c.name) == normalized_name(&catalog.name))
                .count()
                == 1,
            "Есть несколько каталогов с одинаковым именем; OVF Tool не сможет однозначно выбрать каталог"
        );
        let xml = self.get(catalog_href).await?;
        let (mut refs, admin) = catalog_details(&xml, name)?;
        let mut warnings = Vec::new();
        let mut confirmed_empty_profiles = false;
        if refs.is_empty() {
            let admin = admin.or_else(|| {
                let mut url = url::Url::parse(catalog_href).ok()?;
                let tail = url.path().strip_prefix("/api/catalog/")?.to_owned();
                url.set_path(&format!("/api/admin/catalog/{tail}"));
                Some(url.to_string())
            });
            if let Some(admin) = admin {
                match self.get(&admin).await {
                    Ok(xml) => {
                        refs = catalog_details(&xml, name)?.0;
                        let doc = Document::parse(&xml)?;
                        confirmed_empty_profiles = doc.root_element().has_tag_name("AdminCatalog")
                            && doc.root_element().children().any(|n| n.has_tag_name("CatalogStorageProfiles"))
                            && refs.is_empty();
                    }
                    Err(_) => warnings.push("API не предоставил настройки дисковой политики каталога (возможно, нет права чтения AdminCatalog).".into()),
                }
            }
        }
        let mut profiles = Vec::new();
        for (href, label) in refs {
            if profiles
                .iter()
                .any(|p: &CatalogStorageProfile| p.href == href)
            {
                continue;
            }
            match self
                .get(&href)
                .await
                .and_then(|xml| storage_profile(&xml, &href, &label))
            {
                Ok(profile) => profiles.push(profile),
                Err(_) => {
                    profiles.push(CatalogStorageProfile {
                        href,
                        name: label,
                        enabled: None,
                        limit_bytes: None,
                        used_bytes: None,
                        free_bytes: None,
                    });
                }
            }
        }
        if profiles.is_empty()
            || profiles
                .iter()
                .any(|p| p.free_bytes.is_none() || p.enabled.is_none())
        {
            match self.catalog_storage_records().await {
                Ok(records) => {
                    if profiles.is_empty() && confirmed_empty_profiles {
                        if let Some(profile) = inherited_profile(&records, vdc) {
                            warnings.push(format!("В настройках каталога политика не задана: унаследована политика по умолчанию ЦОДа «{vdc}» — «{}». Остаток получен из Query API.", profile.name));
                            profiles.push(profile);
                        }
                    } else {
                        for profile in &mut profiles {
                            if let Some(record) = records
                                .iter()
                                .find(|r| same_profile(&r.profile.href, &profile.href))
                            {
                                // Preserve stricter direct-resource data where both
                                // endpoints return it; query supplies omitted usage.
                                profile.limit_bytes =
                                    match (profile.limit_bytes, record.profile.limit_bytes) {
                                        (Some(a), Some(b)) => Some(a.min(b)),
                                        (a, b) => a.or(b),
                                    };
                                profile.used_bytes =
                                    match (profile.used_bytes, record.profile.used_bytes) {
                                        (Some(a), Some(b)) => Some(a.max(b)),
                                        (a, b) => a.or(b),
                                    };
                                profile.enabled = if profile.enabled == Some(false)
                                    || record.profile.enabled == Some(false)
                                {
                                    Some(false)
                                } else {
                                    profile.enabled.or(record.profile.enabled)
                                };
                                profile.free_bytes = profile
                                    .limit_bytes
                                    .zip(profile.used_bytes)
                                    .map(|(limit, used)| limit.saturating_sub(used));
                            }
                        }
                    }
                }
                Err(_) => {
                    warnings.push("Query API не предоставил текущую квоту дисковых политик.".into())
                }
            }
        }
        if profiles.is_empty() {
            warnings.push(format!("Политика каталога не определена: настройка AdminCatalog недоступна либо для ЦОДа «{vdc}» нет однозначной политики по умолчанию."));
        }
        if required_bytes == 0 {
            warnings.push("Размер дисков для предварительной проверки неизвестен.".into());
        }
        let candidates: Vec<_> = profiles
            .iter()
            .filter(|p| p.enabled != Some(false))
            .collect();
        let capacity_verified = required_bytes > 0
            && !candidates.is_empty()
            && candidates.iter().all(|p| {
                p.enabled == Some(true) && p.free_bytes.is_some_and(|free| free >= required_bytes)
            });
        if candidates.len() > 1 && !capacity_verified {
            warnings.push(
                "В каталоге несколько политик; окончательное размещение определяет Cloud Director."
                    .into(),
            );
        }
        let result = CatalogPreflight {
            catalog_href: catalog_href.into(),
            catalog_name: catalog.name.clone(),
            required_bytes,
            profiles,
            warnings,
            capacity_verified,
        };
        result.check_capacity()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn catalog_item_collision_includes_media_and_whitespace() {
        let xml = r#"<Catalog><CatalogItems><CatalogItem name="disk.iso" href="/item/1"/><CatalogItem name="vm" href="/item/2"/></CatalogItems></Catalog>"#;
        assert!(catalog_details(xml, "disk.iso").is_err());
        assert!(catalog_details(xml, "vm").is_err());
        assert!(catalog_details(xml, "DISK.ISO").is_err());
        assert!(catalog_details(xml, "fresh").is_ok());
    }
    #[test]
    fn unicode_names_share_identity_with_reservations() {
        assert_eq!(normalized_name("  ÉКО  "), normalized_name("e\u{301}ко"));
        let xml = "<Catalog><CatalogItems><CatalogItem name=\"ÉКО\"/></CatalogItems></Catalog>";
        assert!(catalog_details(xml, "e\u{301}ко").is_err());
    }
    #[test]
    fn parses_all_catalog_profiles_without_default_vdc_guess() {
        let xml = r#"<AdminCatalog><CatalogStorageProfiles><VdcStorageProfile name="fast" href="/p/1"/><VdcStorageProfile name="slow" href="/p/2"/></CatalogStorageProfiles></AdminCatalog>"#;
        assert_eq!(catalog_details(xml, "new").unwrap().0.len(), 2);
    }
    fn profile(limit: u64, used: u64) -> CatalogStorageProfile {
        storage_profile(&format!("<VdcStorageProfile name=\"fast\"><Enabled>true</Enabled><Units>MB</Units><Limit>{limit}</Limit><StorageUsedMB>{used}</StorageUsedMB></VdcStorageProfile>"), "/p/1", "fast").unwrap()
    }
    #[test]
    fn zero_limit_is_unknown_and_overuse_saturates() {
        assert_eq!(profile(0, 12).free_bytes, None);
        assert_eq!(profile(10, 12).free_bytes, Some(0));
        assert_eq!(profile(20, 12).free_bytes, Some(8 * 1024 * 1024));
    }
    #[test]
    fn insufficient_known_quota_fails_but_unknown_is_not_verified() {
        let mut report = CatalogPreflight {
            catalog_href: "/c/1".into(),
            catalog_name: "mine".into(),
            required_bytes: 9 * 1024 * 1024,
            profiles: vec![profile(20, 12)],
            warnings: vec![],
            capacity_verified: false,
        };
        assert!(report.check_capacity().is_err());
        report.profiles = vec![profile(0, 12)];
        assert!(report.check_capacity().is_ok());
        assert!(report.summary().contains("не подтверждена"));
    }
    #[test]
    fn disabled_profiles_and_multiple_destinations_are_not_combined() {
        let mut report = CatalogPreflight {
            catalog_href: "/c/1".into(),
            catalog_name: "mine".into(),
            required_bytes: 9 * 1024 * 1024,
            profiles: vec![profile(20, 12), profile(20, 12)],
            warnings: vec![],
            capacity_verified: false,
        };
        // Two profiles with 8 MiB each do not fit one 9 MiB image.
        assert!(report.check_capacity().is_err());
        report.profiles[0].enabled = Some(false);
        report.profiles[1].enabled = Some(false);
        assert!(
            report
                .check_capacity()
                .unwrap_err()
                .to_string()
                .contains("отключены")
        );
    }
    #[test]
    fn unknown_units_missing_usage_and_overflow_are_never_free_space() {
        let xml = "<VdcStorageProfile><Enabled>true</Enabled><Units>GB</Units><Limit>100</Limit><StorageUsedMB>1</StorageUsedMB></VdcStorageProfile>";
        assert_eq!(storage_profile(xml, "/p", "p").unwrap().free_bytes, None);
        let xml = "<VdcStorageProfile><Units>MB</Units><Limit>100</Limit></VdcStorageProfile>";
        assert_eq!(storage_profile(xml, "/p", "p").unwrap().free_bytes, None);
        let xml = format!(
            "<VdcStorageProfile><Units>MB</Units><Limit>{}</Limit></VdcStorageProfile>",
            u64::MAX
        );
        assert!(storage_profile(&xml, "/p", "p").is_err());
    }
    #[test]
    fn query_record_reports_real_usage_and_default_for_selected_vdc() {
        let xml = r#"<QueryResultRecords xmlns="http://www.vmware.com/vcloud/v1.5"><OrgVdcStorageProfileRecord name="SSD" href="https://cloud.invalid/api/vdcStorageProfile/p1" vdc="https://cloud.invalid/api/vdc/v1" vdcName="vdc-a" isEnabled="true" isDefaultStorageProfile="true" storageUsedMB="35912" storageLimitMB="102400"/><Link rel="nextPage" href="https://cloud.invalid/api/query?page=2"/></QueryResultRecords>"#;
        let (records, next) = profile_records(xml).unwrap();
        assert_eq!(
            next.as_deref(),
            Some("https://cloud.invalid/api/query?page=2")
        );
        let policy = inherited_profile(&records, "vdc-a").unwrap();
        assert_eq!(policy.free_bytes, Some(66_488 * 1024 * 1024));
        assert_eq!(policy.enabled, Some(true));
        assert!(inherited_profile(&records, "another-vdc").is_none());
        assert!(inherited_profile(&records, "").is_none());
    }
    #[test]
    fn default_vdc_names_must_be_unambiguous_and_zero_limit_is_unknown() {
        let xml = r#"<R><OrgVdcStorageProfileRecord href="https://c/api/vdcStorageProfile/1" vdcName="same" vdc="https://c/api/vdc/1" isDefaultStorageProfile="true" storageUsedMB="10" storageLimitMB="0"/><OrgVdcStorageProfileRecord href="https://c/api/vdcStorageProfile/2" vdcName="same" vdc="https://c/api/vdc/2" isDefaultStorageProfile="true" storageUsedMB="10" storageLimitMB="20"/></R>"#;
        let (records, _) = profile_records(xml).unwrap();
        assert!(inherited_profile(&records, "same").is_none());
        assert_eq!(
            inherited_profile(&records, "https://c/api/vdc/1")
                .unwrap()
                .free_bytes,
            None
        );
        assert_eq!(
            inherited_profile(&records, "https://c/api/vdc/2")
                .unwrap()
                .free_bytes,
            Some(10 * 1024 * 1024)
        );
    }
    #[test]
    fn profile_matching_accepts_admin_reference_but_not_other_origin() {
        assert!(same_profile(
            "https://c/api/admin/vdcStorageProfile/id",
            "https://c/api/vdcStorageProfile/id"
        ));
        assert!(!same_profile(
            "https://c/api/vdcStorageProfile/id",
            "https://other/api/vdcStorageProfile/id"
        ));
    }
}
