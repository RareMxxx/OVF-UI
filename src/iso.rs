//! Bounded structural validation, not a filesystem checker or an OS/boot validator.
//! Layouts: ECMA-119 (ISO9660), ECMA-167 part 3 (UDF volume descriptors).
use anyhow::{Context, Result, bail, ensure};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};
use tokio_util::sync::CancellationToken;

const SECTOR: u64 = 2048;

fn sector(file: &mut File, block: u64) -> Result<[u8; 2048]> {
    file.seek(SeekFrom::Start(
        block
            .checked_mul(SECTOR)
            .context("Переполнение адреса ISO")?,
    ))?;
    let mut data = [0; 2048];
    file.read_exact(&mut data)
        .context("ISO усечён: не удалось прочитать дескриптор")?;
    Ok(data)
}
fn le16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().unwrap())
}
fn le32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}
fn both16(b: &[u8], at: usize) -> Result<u16> {
    let value = le16(b, at);
    ensure!(
        value == u16::from_be_bytes(b[at + 2..at + 4].try_into().unwrap()),
        "ISO: копии числового поля не совпадают"
    );
    Ok(value)
}
fn both32(b: &[u8], at: usize) -> Result<u32> {
    let value = le32(b, at);
    ensure!(
        value == u32::from_be_bytes(b[at + 4..at + 8].try_into().unwrap()),
        "ISO: копии числового поля не совпадают"
    );
    Ok(value)
}
fn extent(start: u64, bytes: u64, limit: u64) -> Result<()> {
    ensure!(
        bytes > 0 && start.checked_add(bytes).is_some_and(|end| end <= limit),
        "ISO усечён или повреждён: область данных выходит за границы образа"
    );
    Ok(())
}

fn iso_primary(file: &mut File, pvd: &[u8], image_len: u64) -> Result<()> {
    let blocks = u64::from(both32(pvd, 80)?);
    let block_size = u64::from(both16(pvd, 128)?);
    ensure!(
        matches!(block_size, 512 | 1024 | 2048),
        "ISO: неподдерживаемый размер логического блока"
    );
    let volume_len = blocks
        .checked_mul(block_size)
        .context("ISO: переполнение размера тома")?;
    ensure!(
        volume_len >= 18 * SECTOR && volume_len <= image_len,
        "ISO усечён: заявленный размер тома больше файла или некорректен"
    );
    let path_size = u64::from(both32(pvd, 132)?);
    let little_table = u64::from(le32(pvd, 140));
    let big_table = u64::from(u32::from_be_bytes(pvd[148..152].try_into().unwrap()));
    ensure!(
        path_size >= 10 && little_table > 0 && big_table > 0,
        "ISO: отсутствуют обязательные таблицы путей"
    );
    extent(little_table * block_size, path_size, volume_len)?;
    extent(big_table * block_size, path_size, volume_len)?;
    ensure!(
        pvd[156] >= 34 && pvd[181] & 2 != 0 && pvd[188] == 1 && pvd[189] == 0,
        "ISO: некорректная запись корневого каталога"
    );
    let root_start = u64::from(both32(pvd, 158)?) * block_size;
    let root_size = u64::from(both32(pvd, 166)?);
    extent(root_start, root_size, volume_len)?;
    ensure!(root_size >= 68, "ISO: корневой каталог слишком мал");
    // Inspect the first directory block; recursive traversal is deliberately out of scope.
    file.seek(SeekFrom::Start(root_start))?;
    let mut root = vec![0; root_size.min(block_size) as usize];
    file.read_exact(&mut root)?;
    let mut offset = 0;
    let mut entries = 0;
    while offset < root.len() && root[offset] != 0 {
        let size = root[offset] as usize;
        ensure!(
            size >= 34 && offset + size <= root.len(),
            "ISO: повреждена запись корневого каталога"
        );
        let item = &root[offset..offset + size];
        ensure!(
            33 + item[32] as usize <= item.len(),
            "ISO: повреждено имя в корневом каталоге"
        );
        let length = u64::from(both32(item, 10)?);
        if length > 0 {
            extent(u64::from(both32(item, 2)?) * block_size, length, volume_len)?;
        }
        if entries < 2 {
            ensure!(
                item[32] == 1 && item[33] == entries && item[25] & 2 != 0,
                "ISO: отсутствуют записи . и .. корневого каталога"
            );
        }
        entries += 1;
        offset += size;
    }
    ensure!(entries >= 2, "ISO: корневой каталог пуст или повреждён");
    Ok(())
}

fn crc16(bytes: &[u8]) -> u16 {
    let mut crc = 0u16;
    for byte in bytes {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}
fn udf_tag(data: &[u8], location: u64) -> Result<u16> {
    ensure!(
        matches!(le16(data, 2), 2 | 3),
        "UDF: неподдерживаемая версия descriptor tag"
    );
    let checksum = data[..16]
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != 4)
        .fold(0u8, |sum, (_, b)| sum.wrapping_add(*b));
    ensure!(
        checksum == data[4],
        "UDF: повреждена контрольная сумма descriptor tag"
    );
    ensure!(
        u64::from(le32(data, 12)) == location,
        "UDF: неверный адрес descriptor tag"
    );
    let length = usize::from(le16(data, 10));
    ensure!(
        length > 0 && length <= data.len() - 16,
        "UDF: некорректная длина CRC"
    );
    ensure!(
        crc16(&data[16..16 + length]) == le16(data, 8),
        "UDF: контрольная сумма дескриптора не совпала"
    );
    Ok(le16(data, 0))
}

fn udf_volume(file: &mut File, image_len: u64, cancel: &CancellationToken) -> Result<()> {
    ensure!(
        image_len >= 257 * SECTOR,
        "UDF усечён: отсутствует anchor в секторе 256"
    );
    let anchor = sector(file, 256)?;
    ensure!(
        udf_tag(&anchor, 256)? == 2 && le16(&anchor, 10) >= 496,
        "UDF: отсутствует корректный Anchor Volume Descriptor Pointer"
    );
    let length = u64::from(le32(&anchor, 16));
    let first = u64::from(le32(&anchor, 20));
    extent(first * SECTOR, length, image_len)?;
    ensure!(
        length % SECTOR == 0 && length <= 16 * 1024 * 1024,
        "UDF: неподдерживаемый размер последовательности дескрипторов"
    );
    let (mut primary, mut partition, mut logical, mut terminated) = (false, false, false, false);
    for block in first..first + length / SECTOR {
        ensure!(!cancel.is_cancelled(), "Проверка отменена");
        let data = sector(file, block)?;
        match udf_tag(&data, block)? {
            1 => {
                ensure!(
                    le16(&data, 10) >= 496,
                    "UDF: усечён Primary Volume Descriptor"
                );
                primary = true;
            }
            5 => {
                ensure!(le16(&data, 10) >= 496, "UDF: усечён Partition Descriptor");
                extent(
                    u64::from(le32(&data, 188)) * SECTOR,
                    u64::from(le32(&data, 192)) * SECTOR,
                    image_len,
                )?;
                partition = true;
            }
            6 => {
                ensure!(
                    le16(&data, 10) >= 256 && le32(&data, 212) == 2048,
                    "UDF: повреждён Logical Volume Descriptor или размер блока не поддерживается"
                );
                let maps_len = le32(&data, 264) as usize;
                ensure!(
                    maps_len > 0 && maps_len <= 2048 - 440 && le32(&data, 268) > 0,
                    "UDF: повреждена таблица разделов"
                );
                ensure!(
                    usize::from(le16(&data, 10)) >= 424 + maps_len,
                    "UDF: CRC не покрывает Logical Volume Descriptor"
                );
                logical = true;
            }
            8 => {
                terminated = true;
                break;
            }
            4 | 7 => {}
            3 => bail!("UDF: цепочки Volume Descriptor Pointer пока не поддерживаются"),
            _ => bail!("UDF: неизвестный дескриптор в последовательности тома"),
        }
    }
    ensure!(
        primary && partition && logical && terminated,
        "UDF: последовательность тома неполна (PVD/Partition/LVD/Terminator)"
    );
    Ok(())
}

pub(super) fn validate(path: &Path, cancel: &CancellationToken) -> Result<String> {
    let mut file = File::open(path)?;
    let len = file.metadata()?.len();
    ensure!(
        len >= 18 * SECTOR && len % SECTOR == 0,
        "ISO пуст, усечён или не является образом с секторами 2048 байт"
    );
    let (mut primary, mut terminated, mut bea, mut nsr, mut tea) =
        (false, false, false, false, false);
    for block in 16..(len / SECTOR).min(256) {
        ensure!(!cancel.is_cancelled(), "Проверка отменена");
        let data = sector(&mut file, block)?;
        match &data[1..6] {
            b"CD001" if !terminated => {
                ensure!(data[6] == 1, "ISO: неверная версия volume descriptor");
                match data[0] {
                    1 => {
                        iso_primary(&mut file, &data, len)?;
                        primary = true;
                    }
                    255 => terminated = true,
                    0 | 2 | 3 => {}
                    _ => bail!("ISO: некорректный тип volume descriptor"),
                }
            }
            b"BEA01" => {
                ensure!(
                    data[0] == 0 && data[6] == 1,
                    "UDF: повреждён BEA descriptor"
                );
                bea = true;
            }
            b"NSR02" | b"NSR03" if bea => {
                ensure!(
                    data[0] == 0 && data[6] == 1,
                    "UDF: повреждён NSR descriptor"
                );
                nsr = true;
            }
            b"TEA01" if nsr => {
                tea = true;
                break;
            }
            _ => {
                if primary && !terminated {
                    bail!("ISO: отсутствует завершающий volume descriptor");
                }
                if terminated || bea {
                    break;
                }
            }
        }
    }
    ensure!(
        primary || nsr,
        "Не найдена структура ISO9660 или UDF; файл не является поддерживаемым ISO-образом"
    );
    if primary {
        ensure!(terminated, "ISO: отсутствует завершающий volume descriptor");
    }
    if nsr {
        ensure!(tea, "UDF: отсутствует завершающий TEA descriptor");
        udf_volume(&mut file, len, cancel)?;
    }
    Ok(format!(
        "Структура {}: дескрипторы и границы данных проверены",
        if primary && nsr {
            "ISO9660 + UDF"
        } else if nsr {
            "UDF"
        } else {
            "ISO9660"
        }
    ))
}

#[cfg(test)]
pub(super) fn fixture_iso() -> Vec<u8> {
    fn pair16(b: &mut [u8], at: usize, n: u16) {
        b[at..at + 2].copy_from_slice(&n.to_le_bytes());
        b[at + 2..at + 4].copy_from_slice(&n.to_be_bytes());
    }
    fn pair32(b: &mut [u8], at: usize, n: u32) {
        b[at..at + 4].copy_from_slice(&n.to_le_bytes());
        b[at + 4..at + 8].copy_from_slice(&n.to_be_bytes());
    }
    fn directory(id: u8) -> [u8; 34] {
        let mut b = [0; 34];
        b[0] = 34;
        pair32(&mut b, 2, 20);
        pair32(&mut b, 10, 2048);
        b[18..25].copy_from_slice(&[126, 11, 1, 0, 0, 0, 0]);
        b[25] = 2;
        pair16(&mut b, 28, 1);
        b[32] = 1;
        b[33] = id;
        b
    }
    let mut image = vec![0; 24 * 2048];
    let pvd = &mut image[16 * 2048..17 * 2048];
    pvd[0] = 1;
    pvd[1..6].copy_from_slice(b"CD001");
    pvd[6] = 1;
    pvd[8..40].fill(b' ');
    pvd[40..72].fill(b' ');
    pvd[40..50].copy_from_slice(b"OVF_UI_QA ");
    pair32(pvd, 80, 24);
    pair16(pvd, 120, 1);
    pair16(pvd, 124, 1);
    pair16(pvd, 128, 2048);
    pair32(pvd, 132, 10);
    pvd[140..144].copy_from_slice(&18u32.to_le_bytes());
    pvd[148..152].copy_from_slice(&19u32.to_be_bytes());
    pvd[156..190].copy_from_slice(&directory(0));
    pvd[881] = 1;
    for offset in [813, 830, 847, 864] {
        pvd[offset..offset + 16].fill(b'0');
    }
    image[17 * 2048] = 255;
    image[17 * 2048 + 1..17 * 2048 + 6].copy_from_slice(b"CD001");
    image[17 * 2048 + 6] = 1;
    for (block, big) in [(18, false), (19, true)] {
        let p = &mut image[block * 2048..block * 2048 + 10];
        p[0] = 1;
        p[2..6].copy_from_slice(&if big {
            20u32.to_be_bytes()
        } else {
            20u32.to_le_bytes()
        });
        p[6..8].copy_from_slice(&if big {
            1u16.to_be_bytes()
        } else {
            1u16.to_le_bytes()
        });
    }
    image[20 * 2048..20 * 2048 + 34].copy_from_slice(&directory(0));
    image[20 * 2048 + 34..20 * 2048 + 68].copy_from_slice(&directory(1));
    image
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_nonbootable_iso_rejects_truncation_and_invalid_descriptors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.iso");
        let image = fixture_iso();
        std::fs::write(&path, &image).unwrap();
        assert!(validate(&path, &CancellationToken::new()).is_ok());
        for broken in [image[..image.len() - 2048].to_vec(), vec![0; image.len()]] {
            std::fs::write(&path, broken).unwrap();
            assert!(validate(&path, &CancellationToken::new()).is_err());
        }
        let mut broken = image.clone();
        broken[16 * 2048 + 84] ^= 1;
        std::fs::write(&path, &broken).unwrap();
        assert!(validate(&path, &CancellationToken::new()).is_err());
        let mut broken = image;
        broken[20 * 2048] = 33;
        std::fs::write(&path, &broken).unwrap();
        assert!(validate(&path, &CancellationToken::new()).is_err());
    }

    #[test]
    fn validates_udf_anchor_crc_and_partition_bounds() {
        fn tag(data: &mut [u8], id: u16, location: u32) {
            data[0..2].copy_from_slice(&id.to_le_bytes());
            data[2..4].copy_from_slice(&2u16.to_le_bytes());
            data[10..12].copy_from_slice(&496u16.to_le_bytes());
            data[12..16].copy_from_slice(&location.to_le_bytes());
            let crc = crc16(&data[16..512]);
            data[8..10].copy_from_slice(&crc.to_le_bytes());
            data[4] = data[..16]
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != 4)
                .fold(0u8, |sum, (_, b)| sum.wrapping_add(*b));
        }
        let mut image = vec![0u8; 300 * 2048];
        for (block, kind) in [(16, b"BEA01"), (17, b"NSR03"), (18, b"TEA01")] {
            image[block * 2048 + 1..block * 2048 + 6].copy_from_slice(kind);
            image[block * 2048 + 6] = 1;
        }
        let anchor = &mut image[256 * 2048..257 * 2048];
        anchor[16..20].copy_from_slice(&(4u32 * 2048).to_le_bytes());
        anchor[20..24].copy_from_slice(&32u32.to_le_bytes());
        tag(anchor, 2, 256);
        tag(&mut image[32 * 2048..33 * 2048], 1, 32);
        let partition = &mut image[33 * 2048..34 * 2048];
        partition[188..192].copy_from_slice(&257u32.to_le_bytes());
        partition[192..196].copy_from_slice(&43u32.to_le_bytes());
        tag(partition, 5, 33);
        let logical = &mut image[34 * 2048..35 * 2048];
        logical[212..216].copy_from_slice(&2048u32.to_le_bytes());
        logical[264..268].copy_from_slice(&6u32.to_le_bytes());
        logical[268..272].copy_from_slice(&1u32.to_le_bytes());
        tag(logical, 6, 34);
        tag(&mut image[35 * 2048..36 * 2048], 8, 35);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("udf.iso");
        std::fs::write(&path, &image).unwrap();
        assert!(
            validate(&path, &CancellationToken::new())
                .unwrap()
                .contains("UDF")
        );
        let mut corrupt = image.clone();
        corrupt[256 * 2048 + 16] ^= 1;
        std::fs::write(&path, &corrupt).unwrap();
        assert!(
            validate(&path, &CancellationToken::new())
                .unwrap_err()
                .to_string()
                .contains("сумма")
        );
        image.truncate(image.len() - 2048);
        std::fs::write(&path, &image).unwrap();
        assert!(validate(&path, &CancellationToken::new()).is_err());
    }
}
