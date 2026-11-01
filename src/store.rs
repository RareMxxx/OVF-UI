use crate::model::{Secret, Settings};
use aes_gcm::{
    Aes256Gcm, KeyInit,
    aead::{Aead, Payload},
};
use anyhow::{Context, Result, ensure};
use argon2::{Algorithm, Argon2, Params, Version};
use base64::{Engine, engine::general_purpose::STANDARD};
use directories::ProjectDirs;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use uuid::Uuid;
use zeroize::Zeroizing;

#[derive(Clone)]
pub struct Store {
    pub directory: PathBuf,
    log_lock: Arc<Mutex<()>>,
}

impl Store {
    #[cfg(test)]
    pub(crate) fn for_test(directory: PathBuf) -> Self {
        Self {
            directory,
            log_lock: Arc::new(Mutex::new(())),
        }
    }
    pub fn open() -> Result<Self> {
        let directory = std::env::var_os("OVF_DOCK_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                ProjectDirs::from("app", "OVFDock", "OVF Dock")
                    .expect("Профиль пользователя недоступен")
                    .data_local_dir()
                    .to_owned()
            });
        fs::create_dir_all(&directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        }
        Ok(Self {
            directory,
            log_lock: Arc::new(Mutex::new(())),
        })
    }
    pub fn load(&self) -> Result<Settings> {
        let path = self.directory.join("settings.json");
        if !path.exists() {
            return Ok(Settings {
                reserve_gib: 2,
                download_directory: directories::UserDirs::new()
                    .and_then(|d| d.download_dir().map(Path::to_owned))
                    .unwrap_or_default(),
                ..Default::default()
            });
        }
        serde_json::from_slice(&fs::read(path)?)
            .context("Не удалось прочитать настройки; исходный файл сохранён")
    }
    pub fn save(&self, settings: &Settings) -> Result<()> {
        let path = self.directory.join("settings.json");
        if let Ok(existing) = fs::read(&path)
            && serde_json::from_slice::<Settings>(&existing).is_err()
        {
            let backup = self.directory.join(format!(
                "settings-recovered-{}.json",
                Uuid::new_v4().simple()
            ));
            atomic_write(&backup, &existing)
                .context("Не удалось сохранить копию повреждённых настроек")?;
        }
        atomic_write(&path, &serde_json::to_vec_pretty(settings)?)
    }
    fn entry(&self, id: Uuid) -> Result<keyring::Entry> {
        Ok(keyring::Entry::new("OVF Dock", &id.to_string())?)
    }
    pub fn set_secret(&self, id: Uuid, secret: &Secret) -> Result<()> {
        let text = Zeroizing::new(serde_json::to_string(secret)?);
        self.entry(id)?.set_password(&text).context("Системное хранилище секретов недоступно. В Linux разблокируйте Secret Service (GNOME Keyring / KWallet).")
    }
    pub fn secret(&self, id: Uuid) -> Result<Secret> {
        let text = Zeroizing::new(self.entry(id)?.get_password().context("Секрет не найден в системном хранилище. Откройте настройки подключения и сохраните его снова.")?);
        Ok(serde_json::from_str(&text)?)
    }
    pub fn log(&self, level: &str, message: &str) {
        let _guard = self.log_lock.lock().unwrap_or_else(|e| e.into_inner());
        let path = self.directory.join("ovf-dock.log");
        if fs::metadata(&path).is_ok_and(|m| m.len() > 4 * 1024 * 1024) {
            let _ = fs::rename(&path, self.directory.join("ovf-dock.previous.log"));
        }
        if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(path) {
            let _ = writeln!(
                f,
                "{} [{level}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                redact(message)
            );
        }
    }
    pub fn export(&self, settings: &Settings, path: &Path, password: &str) -> Result<()> {
        ensure!(
            password.chars().count() >= 12,
            "Пароль экспорта должен содержать минимум 12 символов"
        );
        let mut secrets = BTreeMap::new();
        for id in settings
            .endpoints
            .iter()
            .flat_map(|e| e.tenants.iter().map(|t| t.id))
            .chain(settings.s3_profiles.iter().map(|s| s.id))
        {
            secrets.insert(id.to_string(), self.secret(id)?);
        }
        let plaintext = Zeroizing::new(serde_json::to_vec(&Backup {
            settings: settings.clone(),
            secrets,
        })?);
        atomic_write(path, &seal(&plaintext, password)?)
    }
    pub fn import(&self, current: &Settings, path: &Path, password: &str) -> Result<Settings> {
        ensure!(
            fs::metadata(path)?.len() <= 16 * 1024 * 1024,
            "Файл экспорта слишком большой"
        );
        let decrypted = Zeroizing::new(unseal(&fs::read(path)?, password)?);
        let mut backup: Backup = serde_json::from_slice(&decrypted)?;
        let mut result = current.clone();
        for endpoint in &mut backup.settings.endpoints {
            crate::api::endpoint_url(&endpoint.url)?;
            endpoint.id = Uuid::new_v4();
            for tenant in &mut endpoint.tenants {
                let secret = backup
                    .secrets
                    .get(&tenant.id.to_string())
                    .context("В архиве отсутствует секрет тенанта")?;
                tenant.id = Uuid::new_v4();
                self.set_secret(tenant.id, secret)?;
            }
            result.endpoints.push(endpoint.clone());
        }
        for mut s3 in backup.settings.s3_profiles {
            crate::api::endpoint_url(&s3.endpoint)?;
            let secret = backup
                .secrets
                .get(&s3.id.to_string())
                .context("В архиве отсутствует ключ S3")?;
            s3.id = Uuid::new_v4();
            self.set_secret(s3.id, secret)?;
            result.s3_profiles.push(s3);
        }
        self.save(&result)?;
        Ok(result)
    }
}

#[derive(Serialize, Deserialize)]
struct Backup {
    settings: Settings,
    secrets: BTreeMap<String, Secret>,
}

#[derive(Serialize, Deserialize)]
struct EncryptedBackup {
    format: String,
    salt: String,
    nonce: String,
    ciphertext: String,
}

fn key(password: &str, salt: &[u8]) -> Result<Zeroizing<[u8; 32]>> {
    let mut key = Zeroizing::new([0u8; 32]);
    let params = Params::new(65536, 3, 1, Some(32)).map_err(|_| anyhow::anyhow!("Ошибка KDF"))?;
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into(password.as_bytes(), salt, &mut *key)
        .map_err(|_| anyhow::anyhow!("Ошибка KDF"))?;
    Ok(key)
}

pub fn seal(data: &[u8], password: &str) -> Result<Vec<u8>> {
    let mut salt = [0u8; 16];
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut salt);
    rand::thread_rng().fill_bytes(&mut nonce);
    let key = key(password, &salt)?;
    let encrypted = Aes256Gcm::new_from_slice(&*key)
        .unwrap()
        .encrypt(
            (&nonce).into(),
            Payload {
                msg: data,
                aad: b"ovf-dock-backup-v1",
            },
        )
        .map_err(|_| anyhow::anyhow!("Ошибка шифрования"))?;
    Ok(serde_json::to_vec_pretty(&EncryptedBackup {
        format: "ovf-dock-backup-v1".into(),
        salt: STANDARD.encode(salt),
        nonce: STANDARD.encode(nonce),
        ciphertext: STANDARD.encode(encrypted),
    })?)
}

pub fn unseal(data: &[u8], password: &str) -> Result<Vec<u8>> {
    let b: EncryptedBackup = serde_json::from_slice(data)?;
    ensure!(
        b.format == "ovf-dock-backup-v1",
        "Неизвестный формат экспорта"
    );
    let salt = STANDARD.decode(b.salt)?;
    let nonce = STANDARD.decode(b.nonce)?;
    ensure!(
        salt.len() == 16 && nonce.len() == 12,
        "Повреждён заголовок экспорта"
    );
    let key = key(password, &salt)?;
    Aes256Gcm::new_from_slice(&*key)
        .unwrap()
        .decrypt(
            nonce.as_slice().into(),
            Payload {
                msg: &STANDARD.decode(b.ciphertext)?,
                aad: b"ovf-dock-backup-v1",
            },
        )
        .map_err(|_| anyhow::anyhow!("Неверный пароль или повреждённый файл экспорта"))
}

pub fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path.parent().context("Не указана папка")?;
    fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(data)?;
    file.as_file().sync_all()?;
    file.persist(path).map_err(|e| e.error)?;
    Ok(())
}

pub fn redact(message: &str) -> String {
    static URL: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static SECRET: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static AUTH: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    static JWT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let url =
        URL.get_or_init(|| regex::Regex::new(r#"(?i)(?:https?|vcloud)://[^\s<>\"]+"#).unwrap());
    let s = url.replace_all(message, "[адрес скрыт]");
    // Mask the complete scheme + credential before the generic key/value rule.
    // Otherwise `Authorization: Bearer opaque` would leave `opaque` in the log.
    let auth = AUTH.get_or_init(|| {
        regex::Regex::new(
        r#"(?i)\b((?:proxy-)?authorization)["']?\s*[:=]\s*["']?(?:bearer|basic)\s+[^\s,;<>"'}]+"#
    ).unwrap()
    });
    let s = auth.replace_all(&s, "$1=[скрыто]");
    let secret=SECRET.get_or_init(||regex::Regex::new(r#"(?i)\b(authorization|password|passwd|access_token|refresh_token|sessionticket|x-vcloud-authorization|x-vmware-vcloud-access-token|secret_access_key|session_token|api_token|token)["']?\s*[:=]\s*(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s,;<>"'}]+)"#).unwrap());
    let s = secret.replace_all(&s, "$1=[скрыто]");
    let jwt = JWT.get_or_init(|| {
        regex::Regex::new(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+").unwrap()
    });
    jwt.replace_all(&s, "[токен скрыт]")
        .chars()
        .take(4000)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn encrypted_backup_authenticates_and_hides_secrets() {
        let blob = seal(b"cloud-password", "test password 1234").unwrap();
        assert!(!String::from_utf8_lossy(&blob).contains("cloud-password"));
        assert_eq!(
            unseal(&blob, "test password 1234").unwrap(),
            b"cloud-password"
        );
        assert!(unseal(&blob, "wrong").is_err());
    }
    #[test]
    fn log_removes_signed_urls() {
        let s = redact("GET https://s3.test/a?X-Amz-Signature=secret password=secret");
        assert!(!s.contains("secret"));
    }
    #[test]
    fn log_hides_complete_authorization_and_quoted_credentials() {
        for message in [
            "Authorization: Bearer opaque-private-value",
            "Proxy-Authorization: Basic opaque-private-value",
            r#"{"Authorization":"Bearer opaque-private-value"}"#,
            r#"{"refresh_token":"opaque-private-value"}"#,
            r#"password="opaque-private-value with spaces""#,
            "session_token='opaque-private-value with spaces'",
            "x-vmware-vcloud-access-token: opaque-private-value",
        ] {
            let sanitized = redact(message);
            assert!(!sanitized.contains("opaque-private-value"));
            assert!(!sanitized.contains("with spaces"));
            assert_eq!(redact(&sanitized), sanitized);
        }
        assert_eq!(
            redact("Import finished: 25 files"),
            "Import finished: 25 files"
        );
    }
}
