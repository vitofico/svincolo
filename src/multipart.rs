use memchr::memmem::{self, Finder};

use crate::model::{Model, ModelError};

const MAX_PARTS: usize = 64;
const MAX_HEADERS: usize = 8192;

pub struct Boundary(Vec<u8>);

impl Boundary {
    pub fn from_content_type(header: &str) -> Option<Self> {
        let mime = header.parse::<mime::Mime>().ok()?;
        if mime.essence_str() != "multipart/form-data" {
            return None;
        }
        let mut params = mime.params();
        let (name, value) = params.next()?;
        let value = value.as_str();
        if name != mime::BOUNDARY
            || params.next().is_some()
            || value.is_empty()
            || value.len() > 70
            || !value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"'()+_,-./:=?".contains(&b))
        {
            return None;
        }
        Some(Self([b"--", value.as_bytes()].concat()))
    }

    pub fn allows_value(&self, value: &str) -> bool {
        memmem::find(value.as_bytes(), &self.0).is_none()
    }

    pub fn read_model(&self, body: &[u8]) -> Result<Model, ModelError> {
        self.scan(body, false)
    }

    pub fn read_model_prefix(&self, body: &[u8]) -> Option<String> {
        self.scan(body, true).ok().map(|model| model.name)
    }

    fn scan(&self, body: &[u8], prefix: bool) -> Result<Model, ModelError> {
        let delimiter = Finder::new(&self.0);
        let mut tail = body
            .strip_prefix(self.0.as_slice())
            .and_then(|tail| tail.strip_prefix(b"\r\n"))
            .ok_or(ModelError::Invalid)?;
        let mut model = None;
        let mut file_seen = false;
        for _ in 0..MAX_PARTS {
            let header_end = memmem::find(&tail[..tail.len().min(MAX_HEADERS)], b"\r\n\r\n")
                .ok_or(ModelError::Invalid)?;
            let (name, filename) = part_headers(&tail[..header_end]).ok_or(ModelError::Invalid)?;
            tail = &tail[header_end + 4..];
            let start = body.len() - tail.len();
            let marker = delimiter.find(tail).ok_or(ModelError::Invalid)?;
            // Reject other appearances of the boundary marker, including parser-dependent framing.
            let value = tail[..marker]
                .strip_suffix(b"\r\n")
                .ok_or(ModelError::Invalid)?;
            if name.eq_ignore_ascii_case("model") {
                if name != "model" || filename || value.len() > 128 {
                    return Err(ModelError::Invalid);
                }
                if model.is_some() {
                    return Err(ModelError::Duplicate);
                }
                let name = std::str::from_utf8(value)
                    .map_err(|_| ModelError::Invalid)?
                    .to_owned();
                model = Some(Model {
                    name,
                    span: start..start + value.len(),
                });
                if prefix {
                    return model.ok_or(ModelError::Missing);
                }
            }
            if name.eq_ignore_ascii_case("file") {
                if name != "file" || file_seen || !filename || value.is_empty() {
                    return Err(ModelError::Invalid);
                }
                file_seen = true;
            }
            tail = &tail[marker + self.0.len()..];
            if matches!(tail, b"--" | b"--\r\n") {
                if !file_seen {
                    return Err(ModelError::Invalid);
                }
                return model.ok_or(ModelError::Missing);
            }
            tail = tail.strip_prefix(b"\r\n").ok_or(ModelError::Invalid)?;
        }
        Err(ModelError::Invalid)
    }
}

fn part_headers(bytes: &[u8]) -> Option<(String, bool)> {
    let mut disposition = None;
    let mut content_type_seen = false;
    for line in std::str::from_utf8(bytes).ok()?.split("\r\n") {
        let (header, value) = line.split_once(':')?;
        if value
            .bytes()
            .any(|byte| byte.is_ascii_control() && byte != b'\t')
        {
            return None;
        }
        if header.eq_ignore_ascii_case("content-disposition") && disposition.is_none() {
            disposition = Some(value.trim_ascii());
        } else if header.eq_ignore_ascii_case("content-type") && !content_type_seen {
            let mime: mime::Mime = value.trim_ascii().parse().ok()?;
            if mime.type_() == mime::MULTIPART {
                return None;
            }
            content_type_seen = true;
        } else {
            return None;
        }
    }
    // Content-Disposition uses MIME parameter syntax without the leading media type.
    let disposition: mime::Mime = format!("application/{}", disposition?).parse().ok()?;
    if disposition.essence_str() != "application/form-data" {
        return None;
    }
    let mut name = None;
    let mut filename = false;
    for (key, value) in disposition.params() {
        match key.as_str() {
            "name" if name.is_none() => {
                let value = value.as_str();
                if value.is_empty()
                    || !value
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_.-[]".contains(&b))
                {
                    return None;
                }
                name = Some(value.to_owned());
            }
            "filename" if !filename => filename = true,
            _ => return None,
        }
    }
    Some((name?, filename))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multipart_prefix_can_find_model_before_file_without_validating_the_upload() {
        let boundary = Boundary::from_content_type("multipart/form-data; boundary=test").unwrap();
        let prefix =
            b"--test\r\nContent-Disposition: form-data; name=\"model\"\r\n\r\nlow\r\n--test\r\n";
        assert_eq!(boundary.read_model_prefix(prefix).as_deref(), Some("low"));
        assert_eq!(
            boundary.read_model(prefix).unwrap_err(),
            ModelError::Invalid
        );
        let incomplete = &prefix[..prefix.len() - b"\r\n--test\r\n".len()];
        assert_eq!(boundary.read_model_prefix(incomplete), None);
    }
}
