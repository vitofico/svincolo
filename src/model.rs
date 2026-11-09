use std::{fmt, ops::Range};

use serde::de::{Error, IgnoredAny, MapAccess, Visitor};
use serde_json::value::RawValue;

#[derive(Debug)]
pub struct Model {
    pub name: String,
    pub span: Range<usize>,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ModelError {
    Invalid,
    Missing,
    Duplicate,
}

#[derive(serde::Deserialize)]
enum Field {
    #[serde(rename = "model")]
    Model,
    #[serde(other)]
    Other,
}

struct ModelVisitor<'a>(Option<&'a mut Option<String>>);

impl<'de> Visitor<'de> for ModelVisitor<'_> {
    type Value = (Option<(String, &'de RawValue)>, bool);

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut model = None;
        let mut duplicate = false;
        while let Some(field) = map.next_key()? {
            match field {
                Field::Model if model.is_some() => {
                    duplicate = true;
                    map.next_value::<IgnoredAny>()?;
                }
                Field::Model => {
                    let raw = map.next_value::<&RawValue>()?;
                    let name = serde_json::from_str(raw.get()).map_err(A::Error::custom)?;
                    if let Some(found) = self.0.take() {
                        *found = Some(name);
                        // Stop before inspecting any fields after the model.
                        return Err(A::Error::custom("model prefix found"));
                    }
                    model = Some((name, raw));
                }
                Field::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok((model, duplicate))
    }
}

pub fn read_model_prefix(body: &[u8]) -> Option<String> {
    let mut found = None;
    let mut parser = serde_json::Deserializer::from_slice(body);
    let _ = serde::Deserializer::deserialize_map(&mut parser, ModelVisitor(Some(&mut found)));
    found
}

pub fn read_model(body: &[u8]) -> Result<Model, ModelError> {
    let text = std::str::from_utf8(body).map_err(|_| ModelError::Invalid)?;
    let mut parser = serde_json::Deserializer::from_str(text);
    let (model, duplicate) = serde::Deserializer::deserialize_map(&mut parser, ModelVisitor(None))
        .map_err(|_| ModelError::Invalid)?;
    parser.end().map_err(|_| ModelError::Invalid)?;
    if duplicate {
        return Err(ModelError::Duplicate);
    }
    let (name, raw) = model.ok_or(ModelError::Missing)?;

    // A borrowed RawValue is the exact token in the original body, including quotes.
    let token = raw.get();
    let start = token
        .as_ptr()
        .addr()
        .checked_sub(body.as_ptr().addr())
        .ok_or(ModelError::Invalid)?;
    let end = start
        .checked_add(token.len())
        .filter(|&end| end <= body.len())
        .ok_or(ModelError::Invalid)?;
    Ok(Model {
        name,
        span: start..end,
    })
}

#[cfg(test)]
mod tests {
    use super::{ModelError, read_model, read_model_prefix};

    #[test]
    fn prefix_finds_only_a_complete_top_level_model() {
        assert_eq!(
            read_model_prefix(br#"{"nested":{"model":"ignored"},"m\u006fdel":"lo\u0077","#)
                .as_deref(),
            Some("low"),
        );
        assert_eq!(read_model_prefix(br#"{"model":"unfinished"#), None);
        assert_eq!(read_model_prefix(br#"{"nested":{"model":"ignored"}"#), None);
    }

    #[test]
    fn isolates_the_original_top_level_string_token() {
        let body = br#" { "messages": [{"model": "low", "text": "l\u006fw"}], "m\u006fdel"  :  "l\u006fw" , "tail": "low" } "#;
        let model = read_model(body).unwrap();
        assert_eq!(model.name, "low");
        assert_eq!(&body[model.span.clone()], br#""l\u006fw""#);
        assert!(body[..model.span.start].ends_with(br#""m\u006fdel"  :  "#));
        assert_eq!(&body[model.span.end..], br#" , "tail": "low" } "#);
    }

    #[test]
    fn rejects_a_duplicate_top_level_model_even_with_an_escaped_key() {
        let body = br#"{"model":"low","nested":{"model":"allowed"},"mo\u0064el":"high"}"#;
        assert_eq!(read_model(body).unwrap_err(), ModelError::Duplicate);
    }

    #[test]
    fn rejects_invalid_bodies_and_reports_missing_model_separately() {
        for body in [
            b"{\"model\":\"low\",\"ignored\":\"\xff\"}".as_slice(),
            br#"{"model":"low"} null"#,
            br#"{"model":null}"#,
            br#"["low"]"#,
        ] {
            assert_eq!(read_model(body).unwrap_err(), ModelError::Invalid);
        }
        assert_eq!(
            read_model(br#"{"nested":{"model":"low"}}"#).unwrap_err(),
            ModelError::Missing
        );
    }
}
