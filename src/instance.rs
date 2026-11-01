use anyhow::{Context, Result};
use std::fs::{File, OpenOptions};
use std::path::Path;

/// OS releases this lock even after a crash; all versions share the same user lock.
pub struct InstanceGuard(File);
impl InstanceGuard {
    pub fn acquire() -> Result<Self> {
        let directory = directories::ProjectDirs::from("app", "OVFDock", "OVF Dock")
            .context("Профиль пользователя недоступен")?;
        Self::at(directory.data_local_dir())
    }
    fn at(directory: &Path) -> Result<Self> {
        std::fs::create_dir_all(directory)?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(directory.join("ovf-ui.instance.lock"))?;
        fs2::FileExt::try_lock_exclusive(&file).context(
            "OVF UI уже открыт. Используйте открытое окно приложения. Второй экземпляр не запущен, чтобы сохранить очередь, настройки и резервы свободного места.")?;
        Ok(Self(file))
    }
}
impl Drop for InstanceGuard {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn excludes_second_instance_and_releases() {
        let dir = tempfile::tempdir().unwrap();
        let first = InstanceGuard::at(dir.path()).unwrap();
        assert!(InstanceGuard::at(dir.path()).is_err());
        drop(first);
        assert!(InstanceGuard::at(dir.path()).is_ok());
    }
}
