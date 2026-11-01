use anyhow::{Context, Result, ensure};
pub const DOWNLOAD_URL: &str =
    "https://developer.broadcom.com/tools/open-virtualization-format-ovf-tool/latest/";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(pub u32, pub u32, pub u32);
impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.0, self.1, self.2)
    }
}
impl Version {
    pub fn parse(text: &str) -> Option<Self> {
        let re = regex::Regex::new(r"\b(\d+)\.(\d+)\.(\d+)\b").unwrap();
        let c = re.captures(text)?;
        Some(Self(
            c[1].parse().ok()?,
            c[2].parse().ok()?,
            c[3].parse().ok()?,
        ))
    }
}
pub fn parse_latest(html: &str) -> Result<Version> {
    // Read the product version selector, never unrelated JS/analytics versions.
    let re = regex::Regex::new(
        r#"(?is)<option\b[^>]*value=["'][^"']*/tools/open-virtualization-format-ovf-tool/latest/?["'][^>]*>\s*v?(\d+\.\d+\.\d+)[^<]*</option>"#,
    )?;
    let c = re
        .captures(html)
        .context("Broadcom не вернул номер версии OVF Tool. Откройте страницу загрузки вручную.")?;
    Version::parse(&c[1]).context("Некорректная версия на странице Broadcom")
}
pub async fn check_latest() -> Result<Version> {
    let client = reqwest::Client::builder()
        .https_only(true)
        .timeout(std::time::Duration::from_secs(25))
        .user_agent("OVF-UI update-check")
        .build()?;
    let mut r = client
        .get(DOWNLOAD_URL)
        .send()
        .await
        .context("Не удалось связаться с Broadcom")?
        .error_for_status()?;
    let mut data = Vec::new();
    while let Some(chunk) = r.chunk().await? {
        ensure!(
            data.len() + chunk.len() <= 2 * 1024 * 1024,
            "Слишком большой ответ Broadcom"
        );
        data.extend_from_slice(&chunk);
    }
    parse_latest(&String::from_utf8(data)?)
}
pub fn due(last: Option<chrono::NaiveDate>, today: chrono::NaiveDate) -> bool {
    last != Some(today)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reads_product_selector_not_analytics_version() {
        let html = r#"<script>sdk='99.0.0'</script><option value="/tools/open-virtualization-format-ovf-tool/8.0" >v8.0.0</option><option value="/tools/open-virtualization-format-ovf-tool/latest" selected>v9.1.1(Latest)</option>"#;
        assert_eq!(parse_latest(html).unwrap(), Version(9, 1, 1));
        assert!(parse_latest("Access denied 99.0.0").is_err());
        assert!(Version(9, 10, 0) > Version(9, 9, 9));
    }
    #[test]
    fn checks_at_startup_and_first_tick_after_midnight() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 11, 1).unwrap();
        assert!(due(None, today));
        assert!(!due(Some(today), today));
        assert!(due(Some(today), today.succ_opt().unwrap()));
    }
}
