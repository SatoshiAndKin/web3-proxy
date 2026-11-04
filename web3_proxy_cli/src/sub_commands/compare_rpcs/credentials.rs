//! Inspect all JSON strings without accepting a response for comparison.
use serde::de::{self, DeserializeSeed, Deserializer, Visitor};
use std::fmt;

#[derive(Clone, Copy)]
struct Scan<'a>(&'a [String]);

impl Scan<'_> {
    fn contains(self, bytes: &[u8]) -> bool {
        self.0.iter().any(|secret| {
            !secret.is_empty()
                && bytes
                    .windows(secret.len())
                    .any(|part| part == secret.as_bytes())
        })
    }
}

impl<'de> DeserializeSeed<'de> for Scan<'_> {
    type Value = bool;

    fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> Result<bool, D::Error> {
        decoder.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Scan<'_> {
    type Value = bool;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("JSON that can be checked for credentials")
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<bool, E> {
        Ok(self.contains(value.as_bytes()))
    }

    fn visit_unit<E: de::Error>(self) -> Result<bool, E> {
        Ok(false)
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<bool, E> {
        Ok(false)
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<bool, E> {
        Ok(false)
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<bool, E> {
        Ok(false)
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<bool, E> {
        Ok(false)
    }

    fn visit_seq<A: de::SeqAccess<'de>>(self, mut sequence: A) -> Result<bool, A::Error> {
        let mut found = false;
        while let Some(reflected) = sequence.next_element_seed(self)? {
            found |= reflected;
        }
        Ok(found)
    }

    fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
        let mut found = false;
        // Do not collect into a map: each occurrence of a duplicate key and its
        // value must be checked, even when strict comparison rejects the object.
        while let Some(key_reflected) = map.next_key_seed(self)? {
            found |= key_reflected;
            found |= map.next_value_seed(self)?;
        }
        Ok(found)
    }
}

/// An error means the scan could not finish; callers must withhold the bytes.
/// Keep parser errors opaque so they cannot expose response contents in logs.
pub(super) fn reflects_secret(bytes: &[u8], secrets: &[String]) -> Result<bool, ()> {
    if secrets.is_empty() {
        return Ok(false);
    }
    let scan = Scan(secrets);
    if scan.contains(bytes) {
        return Ok(true);
    }
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let found = scan.deserialize(&mut decoder).map_err(|_| ())?;
    decoder.end().map_err(|_| ())?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scans_all_duplicate_keys_and_values_before_comparison() {
        let secrets = vec!["fixture-secret".into()];
        for body in [
            r#"{"result":"\u0066ixture-secret","result":"public"}"#,
            r#"{"result":"public","result":{"nested":["\u0066ixture-secret"]}}"#,
            r#"{"\u0066ixture-secret":null,"other":true}"#,
        ] {
            assert_eq!(reflects_secret(body.as_bytes(), &secrets), Ok(true));
        }
    }

    #[test]
    fn ordinary_values_and_duplicate_public_fields_are_clear() {
        let secrets = vec!["fixture-secret".into()];
        for body in [
            r#"{"id":0,"id":0,"result":"0x1"}"#,
            r#"[null,true,false,1,-1,1.25,90071992547409930000000000000001,1e400,"application/json"]"#,
        ] {
            assert_eq!(reflects_secret(body.as_bytes(), &secrets), Ok(false));
        }
    }

    #[test]
    fn malformed_or_partial_json_cannot_be_declared_clear() {
        let secrets = vec!["fixture-secret".into()];
        for bytes in [
            &b"<html>upstream unavailable</html>"[..],
            &b"{} {}"[..],
            &b"{\"result\":\"\\u0066ixture-secret"[..],
            &b"{\"value\":\"\xff\"}"[..],
        ] {
            assert_eq!(reflects_secret(bytes, &secrets), Err(()));
        }
    }

    #[test]
    fn escaped_quotes_backslashes_and_surrogate_pairs_are_checked() {
        let secrets = vec!["fixture\"\\🔒".into()];
        let bytes = br#""fixture\u0022\u005c\ud83d\udd12""#;
        assert_eq!(reflects_secret(bytes, &secrets), Ok(true));
    }
}
