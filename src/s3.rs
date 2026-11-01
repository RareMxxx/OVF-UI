use crate::{
    bridge::{Limiter, Traffic},
    model::{S3Profile, Secret},
};
use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath};
use std::{
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

pub async fn upload_file(
    profile: &S3Profile,
    secret: Secret,
    path: &Path,
    key: &str,
    traffic: Arc<Traffic>,
    cancel: CancellationToken,
) -> Result<()> {
    crate::api::endpoint_url(&profile.endpoint)?;
    ensure!(
        !profile.bucket.trim().is_empty() && !profile.region.trim().is_empty(),
        "Укажите бакет и регион S3"
    );
    let mut builder = AmazonS3Builder::new()
        .with_endpoint(&profile.endpoint)
        .with_region(&profile.region)
        .with_bucket_name(&profile.bucket)
        .with_access_key_id(&profile.access_key)
        .with_secret_access_key(&secret.value)
        .with_virtual_hosted_style_request(false);
    if !secret.session_token.is_empty() {
        builder = builder.with_token(&secret.session_token);
    }
    let store = builder.build().context("Не удалось настроить S3")?;
    upload_to_store(&store, path, key, traffic, cancel).await
}

fn part_layout(size: u64) -> (u64, usize) {
    let part_size =
        ((size.div_ceil(9990)).max(16 * 1024 * 1024)).div_ceil(1024 * 1024) * 1024 * 1024;
    let concurrency = (64 * 1024 * 1024 / part_size).clamp(1, 4) as usize;
    (part_size, concurrency)
}

async fn upload_to_store(
    store: &dyn ObjectStore,
    path: &Path,
    key: &str,
    traffic: Arc<Traffic>,
    cancel: CancellationToken,
) -> Result<()> {
    let location = ObjectPath::parse(key)?;
    let size = tokio::fs::metadata(path).await?.len();
    ensure!(
        size > 0 && size <= 5 * 1024u64.pow(4),
        "Неподдерживаемый размер объекта S3"
    );
    // Four 16 MiB parts for normal images. Reduce concurrency when large S3 objects
    // need bigger parts to remain under the 10,000-part protocol limit.
    let (part_size, concurrency) = part_layout(size);
    ensure!(!cancel.is_cancelled(), "Передача отменена");
    let mut upload = store
        .put_multipart(&location)
        .await
        .context("Не удалось начать multipart-загрузку S3")?;
    let result:Result<()>=async {
        let mut file=tokio::fs::File::open(path).await?;
        let limiter=Arc::new(Limiter::new(traffic.clone()));
        let mut tasks=tokio::task::JoinSet::new();
        let mut remaining=size;
        while remaining>0 {
            while tasks.len()>=concurrency {
                tokio::select! {
                    _=cancel.cancelled()=>anyhow::bail!("Передача отменена"),
                    result=tasks.join_next()=>{ result.context("Поток S3 не найден")?.context("Поток S3 прерван")??; }
                }
            }
            let len=remaining.min(part_size) as usize;
            let mut data=vec![0u8;len];
            tokio::select! {_=cancel.cancelled()=>anyhow::bail!("Передача отменена"), r=file.read_exact(&mut data)=>{r?;}}
            let part=upload.put_part(Bytes::from(data).into());
            let limiter=limiter.clone();let traffic=traffic.clone();let cancel=cancel.clone();
            tasks.spawn(async move {
                tokio::select! {_=cancel.cancelled()=>anyhow::bail!("Передача отменена"),_=limiter.consume(len)=>{}}
                tokio::select! {_=cancel.cancelled()=>anyhow::bail!("Передача отменена"),r=part=>{r.context("Ошибка передачи части в S3")?;}}
                traffic.sent.fetch_add(len as u64,Ordering::Relaxed);
                Ok::<(),anyhow::Error>(())
            });
            remaining-=len as u64;
        }
        while !tasks.is_empty() {
            tokio::select! {_=cancel.cancelled()=>anyhow::bail!("Передача отменена"),result=tasks.join_next()=>{result.context("Поток S3 не найден")?.context("Поток S3 прерван")??;}}
        }
        ensure!(!cancel.is_cancelled(),"Передача отменена");
        upload.complete().await.context("Не удалось завершить multipart-загрузку S3")?;
        let meta=store.head(&location).await.context("Не удалось проверить объект после загрузки")?;
        ensure!(meta.size==size,"Размер объекта S3 не совпадает с локальным файлом");
        Ok(())
    }.await;
    if result.is_err() {
        let _ = tokio::time::timeout(Duration::from_secs(30), upload.abort()).await;
    }
    result
}

pub fn object_key(profile: &S3Profile, job_id: uuid::Uuid, filename: &str) -> String {
    let prefix = profile.prefix.trim_matches('/');
    format!(
        "{}{}/{filename}",
        if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        },
        job_id.simple()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn multipart_concurrency_and_memory_are_bounded() {
        assert_eq!(part_layout(1024 * 1024 * 1024), (16 * 1024 * 1024, 4));
        for size in [1, 64 * 1024 * 1024, 1024u64.pow(4), 5 * 1024u64.pow(4)] {
            let (part, concurrency) = part_layout(size);
            assert!(size.div_ceil(part) <= 9990);
            assert!((1..=4).contains(&concurrency));
            assert!(concurrency == 1 || part * concurrency as u64 <= 64 * 1024 * 1024);
        }
    }
    #[tokio::test]
    async fn concurrent_parts_keep_order_and_count_only_completed_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("image.ova");
        let data = (0..40 * 1024 * 1024)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&path, &data).unwrap();
        let store = object_store::memory::InMemory::new();
        let traffic = Arc::new(Traffic::default());
        upload_to_store(
            &store,
            &path,
            "export",
            traffic.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let actual = store
            .get(&ObjectPath::from("export"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(actual.as_ref(), data.as_slice());
        assert_eq!(traffic.sent.load(Ordering::Relaxed), data.len() as u64);
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(
            upload_to_store(&store, &path, "cancelled", traffic, cancel)
                .await
                .is_err()
        );
        assert!(store.head(&ObjectPath::from("cancelled")).await.is_err());
    }
    #[test]
    fn unique_job_prefix_prevents_accidental_overwrite() {
        let p = S3Profile {
            id: uuid::Uuid::new_v4(),
            name: String::new(),
            endpoint: String::new(),
            region: String::new(),
            bucket: String::new(),
            prefix: "/exports/".into(),
            access_key: String::new(),
        };
        let a = object_key(&p, uuid::Uuid::new_v4(), "vm.ova");
        let b = object_key(&p, uuid::Uuid::new_v4(), "vm.ova");
        assert!(a.starts_with("exports/"));
        assert_ne!(a, b);
    }
}
