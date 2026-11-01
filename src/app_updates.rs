//! Public GitHub release discovery. No credentials or installers are downloaded.
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use std::time::Duration;

pub const DEFAULT_REPOSITORY: &str = "RareMxxx/OVF-UI";
const MAX_RELEASE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct AppRelease {
    pub version: String,
    pub url: String,
    pub newer: bool,
}

#[derive(Deserialize)]
struct GithubRelease {
    tag_name: String,
    html_url: String,
    draft: bool,
    prerelease: bool,
}

pub fn validate_repository(repository: &str) -> Result<&str> {
    let repository = repository.trim();
    let parts = repository.split('/').collect::<Vec<_>>();
    ensure!(
        parts.len() == 2
            && !parts[0].is_empty()
            && parts[0].len() <= 39
            && !parts[0].starts_with('-')
            && !parts[0].ends_with('-')
            && !parts[0].contains("--")
            && parts[0]
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-')
            && !parts[1].is_empty()
            && parts[1].len() <= 100
            && parts[1] != "."
            && parts[1] != ".."
            && parts[1]
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c)),
        "Укажите публичный репозиторий GitHub в формате owner/repository, без URL, пробелов и дополнительных путей."
    );
    Ok(repository)
}

fn parse_release(repository: &str, body: &[u8], current: &str) -> Result<AppRelease> {
    let repository = validate_repository(repository)?;
    let release: GithubRelease =
        serde_json::from_slice(body).context("GitHub вернул некорректное описание выпуска")?;
    ensure!(
        !release.draft && !release.prerelease,
        "GitHub вернул черновик или предварительный выпуск; ожидается стабильная версия."
    );
    let version = semver::Version::parse(
        release
            .tag_name
            .strip_prefix('v')
            .unwrap_or(&release.tag_name),
    )
    .context("Тег выпуска должен иметь вид v1.2.3 или 1.2.3 (SemVer)")?;
    ensure!(
        version.pre.is_empty(),
        "Тег содержит предварительную версию; стабильное обновление не найдено."
    );
    let current =
        semver::Version::parse(current).context("Некорректная версия текущего приложения")?;
    let url = url::Url::parse(&release.html_url).context("Некорректная ссылка выпуска GitHub")?;
    let segments = url
        .path_segments()
        .map(|s| s.collect::<Vec<_>>())
        .unwrap_or_default();
    let repo_parts = repository.split('/').collect::<Vec<_>>();
    ensure!(
        url.scheme() == "https"
            && url.host_str() == Some("github.com")
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && segments.len() == 5
            && segments[0].eq_ignore_ascii_case(repo_parts[0])
            && segments[1].eq_ignore_ascii_case(repo_parts[1])
            && segments[2] == "releases"
            && segments[3] == "tag"
            && percent_encoding::percent_decode_str(segments[4]).decode_utf8_lossy()
                == release.tag_name,
        "GitHub вернул ссылку вне страницы выпуска выбранного репозитория; открытие запрещено."
    );
    Ok(AppRelease {
        version: version.to_string(),
        url: url.to_string(),
        // Build metadata is not part of SemVer precedence.
        newer: version.cmp_precedence(&current).is_gt(),
    })
}

// Keep this client separate from Cloud Director, S3 and the publisher's GitHub
// connection. No credentials, cookies, git configuration or keyring are read.
fn anonymous_release_request(repository: &str) -> Result<reqwest::RequestBuilder> {
    let repository = validate_repository(repository)?;
    let client = reqwest::Client::builder()
        .https_only(true)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(25))
        .build()?;
    Ok(client
        .get(format!(
            "https://api.github.com/repos/{repository}/releases/latest"
        ))
        .header(
            reqwest::header::USER_AGENT,
            concat!("OVF-UI/", env!("CARGO_PKG_VERSION")),
        )
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28"))
}

pub async fn check_app_release() -> Result<AppRelease> {
    let repository = DEFAULT_REPOSITORY;
    let mut response = anonymous_release_request(repository)?
        .send()
        .await
        .context("Не удалось проверить выпуск GitHub: проверьте подключение к Интернету и доступ к api.github.com")?;
    match response.status().as_u16() {
        200 => {}
        404 => bail!(
            "Публичный стабильный выпуск OVF UI пока не опубликован. Повторите проверку позже."
        ),
        403 | 429 => bail!(
            "GitHub ограничил анонимные запросы или запретил доступ (HTTP {}). Повторите проверку позже. Вводить токен не нужно.",
            response.status()
        ),
        301..=308 => bail!(
            "Адрес обновлений перенаправлен. Проверка остановлена; обратитесь к разработчику OVF UI."
        ),
        status => bail!(
            "GitHub не смог проверить выпуск {repository}: HTTP {status}. Повторите проверку позже."
        ),
    }
    ensure!(
        response
            .content_length()
            .is_none_or(|size| size <= MAX_RELEASE_BYTES as u64),
        "Ответ GitHub слишком большой (более 1 МиБ)."
    );
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .context("Ответ GitHub прерван во время чтения")?
    {
        ensure!(
            body.len().saturating_add(chunk.len()) <= MAX_RELEASE_BYTES,
            "Ответ GitHub слишком большой (более 1 МиБ)."
        );
        body.extend_from_slice(&chunk);
    }
    parse_release(repository, &body, env!("CARGO_PKG_VERSION"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn release_request_is_anonymous_read_only_and_contains_no_profile_data() {
        let request = anonymous_release_request(DEFAULT_REPOSITORY)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(request.method(), reqwest::Method::GET);
        assert_eq!(
            request.url().as_str(),
            "https://api.github.com/repos/RareMxxx/OVF-UI/releases/latest"
        );
        assert!(request.body().is_none());
        for forbidden in [
            "authorization",
            "proxy-authorization",
            "cookie",
            "x-vcloud-authorization",
            "x-vmware-vcloud-access-token",
            "referer",
        ] {
            assert!(!request.headers().contains_key(forbidden), "{forbidden}");
        }
        assert_eq!(
            request.headers()["user-agent"],
            concat!("OVF-UI/", env!("CARGO_PKG_VERSION"))
        );
    }

    fn payload(tag: &str, url: &str) -> Vec<u8> {
        serde_json::to_vec(
            &serde_json::json!({"tag_name":tag,"html_url":url,"draft":false,"prerelease":false}),
        )
        .unwrap()
    }

    #[test]
    fn repository_is_not_an_arbitrary_url_or_path() {
        assert_eq!(
            validate_repository(" owner/OVF-UI ").unwrap(),
            "owner/OVF-UI"
        );
        for invalid in [
            "",
            "owner",
            "https://github.com/owner/repo",
            "owner/repo/releases",
            "owner/..",
            "owner/repo?x",
            "owner%2fother/repo",
            "-owner/repo",
            "owner/re po",
        ] {
            assert!(validate_repository(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn compares_semver_and_ignores_build_metadata() {
        let r = parse_release(
            "owner/repo",
            &payload(
                "v0.10.0",
                "https://github.com/owner/repo/releases/tag/v0.10.0",
            ),
            "0.9.0",
        )
        .unwrap();
        assert!(r.newer);
        assert_eq!(r.version, "0.10.0");
        assert_eq!(r.url, "https://github.com/owner/repo/releases/tag/v0.10.0");
        for (tag, current, newer) in [
            ("1.0.0", "1.0.0-rc.1", true),
            ("1.0.0+build2", "1.0.0+build1", false),
            ("0.9.0", "0.10.0", false),
        ] {
            let r = parse_release(
                "owner/repo",
                &payload(
                    tag,
                    &format!("https://github.com/owner/repo/releases/tag/{tag}"),
                ),
                current,
            )
            .unwrap();
            assert_eq!(r.newer, newer);
        }
    }

    #[test]
    fn rejects_external_or_mismatched_release_links() {
        for url in [
            "http://github.com/owner/repo/releases/tag/v1.0.0",
            "https://github.com.evil.test/owner/repo/releases/tag/v1.0.0",
            "https://github.com/other/repo/releases/tag/v1.0.0",
            "https://github.com/owner/repo/releases/tag/v9.0.0",
            "https://github.com/owner/repo/releases/download/v1.0.0",
            "https://user@github.com/owner/repo/releases/tag/v1.0.0",
            "https://github.com/owner/repo/releases/tag/v1.0.0?next=evil",
            "https://github.com/owner/repo/releases/tag/v1.0.0#fragment",
            "https://github.com:8443/owner/repo/releases/tag/v1.0.0",
            "https://github.com/owner/repo/releases/tag/v1.0.0%2f..%2fother",
        ] {
            assert!(
                parse_release("owner/repo", &payload("v1.0.0", url), "0.4.0").is_err(),
                "{url}"
            );
        }
    }

    #[test]
    fn rejects_malformed_and_prerelease_payloads() {
        assert!(parse_release("owner/repo", b"{}", "0.4.0").is_err());
        for tag in ["release-1.0.0", "v1.0.0-rc.1", "v01.0.0"] {
            assert!(
                parse_release(
                    "owner/repo",
                    &payload(
                        tag,
                        &format!("https://github.com/owner/repo/releases/tag/{tag}")
                    ),
                    "0.4.0"
                )
                .is_err()
            );
        }
        for flag in ["draft", "prerelease"] {
            let mut p: serde_json::Value = serde_json::from_slice(&payload(
                "v1.0.0",
                "https://github.com/owner/repo/releases/tag/v1.0.0",
            ))
            .unwrap();
            p[flag] = true.into();
            assert!(
                parse_release("owner/repo", &serde_json::to_vec(&p).unwrap(), "0.4.0").is_err()
            );
        }
    }
}
