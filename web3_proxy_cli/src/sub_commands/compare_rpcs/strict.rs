//! Independent, lossless response decoding. Never decode through proxy types.
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::{value::RawValue, Value};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
}

// A Value parser normally overwrites duplicate keys. Reject them, including
// escaped spellings of the same key, before comparing or interpreting a reply.
struct RawObject(BTreeMap<String, Box<RawValue>>);
impl<'de> Deserialize<'de> for RawObject {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> de::Visitor<'de> for Visitor {
            type Value = RawObject;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an object without duplicate keys")
            }
            fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut fields = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, Box<RawValue>>()? {
                    if fields.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                }
                Ok(RawObject(fields))
            }
        }
        decoder.deserialize_map(Visitor)
    }
}

impl Json {
    pub fn parse(bytes: &[u8]) -> Result<Self, &'static str> {
        let raw: Box<RawValue> = serde_json::from_slice(bytes).map_err(|_| "invalid JSON")?;
        Self::from_raw(&raw, 0)
    }

    fn from_raw(raw: &RawValue, depth: usize) -> Result<Self, &'static str> {
        if depth > 128 {
            return Err("JSON nesting limit exceeded");
        }
        let text = raw.get();
        Ok(match text.as_bytes()[0] {
            b'{' => {
                let object: RawObject = serde_json::from_str(text)
                    .map_err(|_| "invalid or duplicate JSON object key")?;
                Self::Object(
                    object
                        .0
                        .into_iter()
                        .map(|(k, v)| Ok((k, Self::from_raw(&v, depth + 1)?)))
                        .collect::<Result<_, &'static str>>()?,
                )
            }
            b'[' => {
                let items: Vec<Box<RawValue>> =
                    serde_json::from_str(text).map_err(|_| "invalid JSON array")?;
                Self::Array(
                    items
                        .iter()
                        .map(|v| Self::from_raw(v, depth + 1))
                        .collect::<Result<_, &'static str>>()?,
                )
            }
            b'"' => Self::String(serde_json::from_str(text).map_err(|_| "invalid JSON string")?),
            b'n' => Self::Null,
            b't' => Self::Bool(true),
            b'f' => Self::Bool(false),
            _ => Self::Number(text.to_owned()),
        })
    }

    pub fn field(&self, key: &str) -> Option<&Json> {
        match self {
            Self::Object(fields) => fields.get(key),
            _ => None,
        }
    }

    pub fn string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn number(&self) -> Option<&str> {
        match self {
            Self::Number(value) => Some(value),
            _ => None,
        }
    }

    pub fn value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(v) => Value::Bool(*v),
            Self::Number(v) => serde_json::from_str(v).expect("validated number"),
            Self::String(v) => Value::String(v.clone()),
            Self::Array(v) => Value::Array(v.iter().map(Self::value).collect()),
            Self::Object(v) => {
                Value::Object(v.iter().map(|(k, v)| (k.clone(), v.value())).collect())
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct Difference {
    pub path: String,
    // None means missing; Some(Null) remains distinct.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<Value>,
}

pub(super) fn differences(left: &Json, right: &Json) -> Vec<Difference> {
    fn walk(a: Option<&Json>, b: Option<&Json>, path: String, out: &mut Vec<Difference>) {
        if a == b || out.len() >= 100 {
            return;
        }
        match (a, b) {
            (Some(Json::Object(a)), Some(Json::Object(b))) => {
                for key in a.keys().chain(b.keys().filter(|k| !a.contains_key(*k))) {
                    let escaped = key.replace('~', "~0").replace('/', "~1");
                    walk(a.get(key), b.get(key), format!("{path}/{escaped}"), out);
                }
            }
            (Some(Json::Array(a)), Some(Json::Array(b))) => {
                for i in 0..a.len().max(b.len()) {
                    walk(a.get(i), b.get(i), format!("{path}/{i}"), out);
                    if out.len() >= 100 {
                        break;
                    }
                }
            }
            _ => out.push(Difference {
                path,
                left: a.map(Json::value),
                right: b.map(Json::value),
            }),
        }
    }
    let mut out = Vec::new();
    walk(Some(left), Some(right), String::new(), &mut out);
    out
}

pub(super) fn envelope(value: &Json, id: &Json) -> Result<(), &'static str> {
    if value.field("jsonrpc").and_then(Json::string) != Some("2.0") {
        return Err("invalid JSON-RPC version");
    }
    if value.field("id") != Some(id) {
        return Err("wrong or missing response ID");
    }
    match (value.field("result"), value.field("error")) {
        (Some(_), None) => Ok(()),
        (None, Some(error)) => {
            let code = error
                .field("code")
                .and_then(Json::number)
                .and_then(|v| v.parse::<i64>().ok());
            if code.is_none() || error.field("message").and_then(Json::string).is_none() {
                return Err("invalid JSON-RPC error");
            }
            Ok(())
        }
        _ => Err("response must contain exactly one of result or error"),
    }
}

pub(super) fn is_revert(error: &Json) -> bool {
    error.field("code").and_then(Json::number) == Some("3")
        || error
            .field("message")
            .and_then(Json::string)
            .is_some_and(|message| message.to_ascii_lowercase().contains("revert"))
}

/// Missing revert data and an empty byte string represent the same outcome.
/// Apply only to comparison copies; captured response bytes stay unchanged.
pub(super) fn normalize_empty_revert_data(reply: &mut Json) {
    let Json::Object(reply) = reply else {
        return;
    };
    let Some(error) = reply.get_mut("error") else {
        return;
    };
    if !is_revert(error) {
        return;
    }
    let Json::Object(error) = error else {
        return;
    };
    if error.get("data").and_then(Json::string) == Some("0x") {
        error.remove("data");
    }
}
