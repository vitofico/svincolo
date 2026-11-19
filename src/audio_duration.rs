//! Container timing for common Spunto uploads, used only as an estimate when the
//! provider completes a transcription without reporting its billed duration.
pub fn milliseconds(file: &[u8]) -> Option<u64> {
    if file.starts_with(b"RIFF") {
        wav(file)
    } else if file.starts_with(b"OggS") {
        opus(file)
    } else {
        None
    }
}

fn le16(data: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(data.get(at..at + 2)?.try_into().ok()?))
}

fn le32(data: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(data.get(at..at + 4)?.try_into().ok()?))
}

fn wav(file: &[u8]) -> Option<u64> {
    if file.get(8..12)? != b"WAVE" || u64::from(le32(file, 4)?) + 8 != file.len() as u64 {
        return None;
    }
    let mut chunks = &file[12..];
    let mut format = None;
    let mut size = None;
    while !chunks.is_empty() {
        let tag = chunks.get(..4)?;
        let length = le32(chunks, 4)? as usize;
        let chunk = chunks.get(8..8 + length)?;
        match tag {
            b"fmt " => {
                if format.is_some() || le16(chunk, 0)? != 1 {
                    return None;
                }
                let channels = le16(chunk, 2)?;
                let rate = le32(chunk, 4)?;
                let align = le16(chunk, 12)?;
                let bits = le16(chunk, 14)?;
                if rate == 0
                    || align == 0
                    || !matches!(bits, 8 | 16 | 24 | 32)
                    || channels.checked_mul(bits / 8)? != align
                    || rate.checked_mul(u32::from(align))? != le32(chunk, 8)?
                {
                    return None;
                }
                format = Some((u64::from(rate), u64::from(align)));
            }
            b"data" if size.is_none() => size = Some(length as u64),
            b"data" => return None,
            _ => {}
        }
        chunks = chunks.get(8 + length + length % 2..)?;
    }
    let (rate, align) = format?;
    let size = size?;
    (size > 0 && size % align == 0).then(|| (size / align * 1000 + rate / 2) / rate)
}

// Spunto records a single Opus logical stream starting at granule zero.
// Opus granules are always measured at 48 kHz, independent of the encoder rate.
fn opus(mut pages: &[u8]) -> Option<u64> {
    let mut sequence = 0u32;
    let mut serial = None;
    let mut preskip = 0u64;
    let mut previous = 0u64;
    let mut packets = 0u64;
    let mut continued = false;
    while !pages.is_empty() {
        let header = pages.get(..27)?;
        let flags = header[5];
        if &header[..5] != b"OggS\0"
            || flags & !7 != 0
            || (flags & 1 != 0) != continued
            || (flags & 2 != 0) != (sequence == 0)
            || le32(header, 18)? != sequence
        {
            return None;
        }
        let current_serial = le32(header, 14)?;
        if serial
            .replace(current_serial)
            .is_some_and(|old| old != current_serial)
        {
            return None;
        }
        let granule = u64::from_le_bytes(header[6..14].try_into().ok()?);
        let laces = pages.get(27..27 + usize::from(header[26]))?;
        let length = laces.iter().map(|n| usize::from(*n)).sum::<usize>();
        let start = 27 + laces.len();
        let data = pages.get(start..start + length)?;
        pages = &pages[start + length..];
        if sequence == 0 {
            if laces != [19]
                || !data.starts_with(b"OpusHead\x01")
                || data[9] == 0
                || data[18] != 0
                || granule != 0
            {
                return None;
            }
            preskip = u64::from(le16(data, 10)?);
        } else if sequence == 1 && !data.starts_with(b"OpusTags") {
            return None;
        }
        packets += laces.iter().filter(|n| **n < 255).count() as u64;
        continued = laces.last().map_or(continued, |n| *n == 255);
        if granule != u64::MAX {
            if granule < previous || granule > packets.saturating_sub(2) * 5760 {
                return None;
            }
            previous = granule;
        }
        if flags & 4 != 0 {
            if !pages.is_empty()
                || continued
                || packets <= 2
                || granule == u64::MAX
                || granule <= preskip
            {
                return None;
            }
            return Some((granule - preskip + 24) / 48);
        }
        sequence = sequence.checked_add(1)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_opus_stream_uses_final_granule_minus_preskip() {
        let mut file = Vec::new();
        for (seq, (flag, granule, data)) in [
            (
                2,
                0u64,
                b"OpusHead\x01\x01\x38\x01\x80\xbb\0\0\0\0\0".as_slice(),
            ),
            (0, 0, b"OpusTags\0\0\0\0\0\0\0\0"),
            (4, 1272, b"audio"),
        ]
        .into_iter()
        .enumerate()
        {
            file.extend_from_slice(b"OggS\0");
            file.push(flag);
            file.extend(granule.to_le_bytes());
            file.extend(1u32.to_le_bytes());
            file.extend((seq as u32).to_le_bytes());
            file.extend([0, 0, 0, 0, 1, data.len() as u8]);
            file.extend(data);
        }
        assert_eq!(milliseconds(&file), Some(20));
        assert_eq!(milliseconds(&file[..file.len() - 1]), None);
        file.push(0);
        assert_eq!(milliseconds(&file), None);
    }
}
