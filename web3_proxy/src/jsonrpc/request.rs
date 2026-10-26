use super::LooseId;
use derive_more::From;
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_inline_default::serde_inline_default;
use sonic_rs::{OwnedLazyValue, Value};
use std::borrow::Cow;
use std::fmt;

// TODO: &str here instead of String should save a lot of allocations
// TODO: generic type for params?
#[serde_inline_default]
#[derive(Clone, Deserialize, Serialize)]
pub struct SingleRequest {
    pub jsonrpc: Cow<'static, str>,
    /// id could be a stricter type, but many rpcs do things against the spec
    /// TODO: this gets cloned into the response object often. would an Arc be better? That has its own overhead and these are short strings
    pub id: OwnedLazyValue,
    pub method: Cow<'static, str>,
    #[serde_inline_default(Value::default())]
    #[serde(deserialize_with = "deserialize_params")]
    pub params: Value,
}

// Sonic's nested Value decoder sizes its node scratch buffer from all remaining
// input. A batch repeats that oversized allocation for every params field.
// Limit DOM parsing to this field while retaining the existing owned Value.
fn deserialize_params<'de, D>(deserializer: D) -> Result<Value, D::Error>
where
    D: Deserializer<'de>,
{
    let params = sonic_rs::LazyValue::deserialize(deserializer)?;
    sonic_rs::from_str(params.as_raw_str()).map_err(de::Error::custom)
}

impl SingleRequest {
    // TODO: Web3ProxyResult? can this even fail?
    pub fn new(id: LooseId, method: Cow<'static, str>, params: Value) -> anyhow::Result<Self> {
        let x = Self {
            jsonrpc: "2.0".into(),
            id: id.into_lazy_value(),
            method,
            params,
        };

        Ok(x)
    }

    /// TODO: not sure how to do this without wasting a ton of allocations
    pub fn num_bytes(&self) -> usize {
        sonic_rs::to_string(self)
            .expect("this should always be valid json")
            .len()
    }

    pub fn validate_method(&self) -> bool {
        self.method
            .chars()
            .all(|x| x.is_ascii_alphanumeric() || x == '_' || x == '(' || x == ')')
    }
}

impl fmt::Debug for SingleRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: the default formatter takes forever to write. this is too quiet though
        // TODO: how should we include params in this? maybe just the length?
        f.debug_struct("JsonRpcRequest")
            .field("id", &self.id)
            .field("method", &self.method)
            .field("params", &self.params)
            .finish()
    }
}

/// Requests can come in multiple formats
#[derive(Debug, From, Serialize)]
pub enum JsonRpcRequestEnum {
    Batch(Vec<SingleRequest>),
    Single(SingleRequest),
}

impl JsonRpcRequestEnum {
    pub fn first_id(&self) -> Option<OwnedLazyValue> {
        match self {
            Self::Batch(x) => x.first().map(|x| x.id.clone()),
            Self::Single(x) => Some(x.id.clone()),
        }
    }

    /// returns the id of the first invalid result (if any). None is good
    pub fn validate(&self) -> Option<OwnedLazyValue> {
        match self {
            Self::Batch(x) => x
                .iter()
                .find_map(|x| (!x.validate_method()).then_some(x.id.clone())),
            Self::Single(x) => {
                if x.validate_method() {
                    None
                } else {
                    Some(x.id.clone())
                }
            }
        }
    }
}

impl<'de> Deserialize<'de> for JsonRpcRequestEnum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            JsonRpc,
            Id,
            Method,
            Params,
        }

        struct JsonRpcBatchVisitor;

        impl<'de> Visitor<'de> for JsonRpcBatchVisitor {
            type Value = JsonRpcRequestEnum;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("JsonRpcRequestEnum")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<JsonRpcRequestEnum, V::Error>
            where
                V: SeqAccess<'de>,
            {
                // TODO: what size should we use as the default?
                let mut batch: Vec<SingleRequest> =
                    Vec::with_capacity(seq.size_hint().unwrap_or(10));

                while let Some(s) = seq.next_element::<SingleRequest>()? {
                    batch.push(s);
                }

                Ok(JsonRpcRequestEnum::Batch(batch))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                // TODO: i feel like this should be easier
                let mut jsonrpc = None;
                let mut id = None;
                let mut method = None;
                let mut params = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::JsonRpc => {
                            // throw away the value
                            // TODO: should we check that it's 2.0?
                            // TODO: how do we skip over this value entirely?
                            jsonrpc = Some(map.next_value()?);
                        }
                        Field::Id => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }
                            id = Some(map.next_value()?);
                        }
                        Field::Method => {
                            if method.is_some() {
                                return Err(de::Error::duplicate_field("method"));
                            }
                            method = Some(map.next_value()?);
                        }
                        Field::Params => {
                            if params.is_some() {
                                return Err(de::Error::duplicate_field("params"));
                            }
                            params = Some(map.next_value()?);
                        }
                    }
                }

                // some providers don't follow the spec and dont include the jsonrpc key
                // i think "2.0" should be a fine default to handle these incompatible clones
                let jsonrpc = jsonrpc.unwrap_or_else(|| "2.0".into());
                // TODO: Errors returned by the try operator get shown in an ugly way
                let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
                let method = method.ok_or_else(|| de::Error::missing_field("method"))?;

                let single = SingleRequest {
                    jsonrpc,
                    id,
                    method,
                    params: params.unwrap_or_default(),
                };

                Ok(JsonRpcRequestEnum::Single(single))
            }
        }

        let batch_visitor = JsonRpcBatchVisitor {};

        deserializer.deserialize_any(batch_visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::JsonRpcRequestEnum;
    use sonic_rs::{json, JsonValueTrait};

    #[test]
    fn batch_params_preserve_nested_values_and_following_requests() {
        let payload = r#"[
            {"jsonrpc":"2.0","id":"line\n雪","method":"eth_call","params":[{"text":"quote\" and \\ and \u96ea","values":[null,true,-1.25e3]},"latest"]},
            {"params":{"blockHash":"0xab","requireCanonical":true},"jsonrpc":"2.0","id":7,"method":"eth_call"},
            {"jsonrpc":"2.0","id":null,"method":"web3_clientVersion","params":null},
            {"jsonrpc":"2.0","id":7,"method":"web3_clientVersion"},
            {"jsonrpc":"2.0","id":"7","method":"eth_call","params":[]}
        ]"#;
        let JsonRpcRequestEnum::Batch(requests) = sonic_rs::from_str(payload).unwrap() else {
            panic!("expected a batch");
        };
        assert_eq!(requests.len(), 5);
        assert_eq!(requests[0].id.as_str(), Some("line\n雪"));
        assert_eq!(
            requests[0].params,
            json!([{"text":"quote\" and \\ and 雪", "values":[null,true,-1250.0]}, "latest"])
        );
        assert_eq!(requests[1].id.as_u64(), Some(7));
        assert_eq!(
            requests[1].params,
            json!({"blockHash":"0xab", "requireCanonical":true})
        );
        assert_eq!(requests[2].params, json!(null));
        assert_eq!(requests[3].params, json!(null));
        assert_eq!(requests[4].id.as_str(), Some("7"));
        assert_eq!(requests[4].params, json!([]));
    }

    #[test]
    fn invalid_batch_entries_do_not_discard_other_requests() {
        let valid = r#"{"jsonrpc":"2.0","id":7,"method":"eth_call","params":[{},"latest"]}"#;
        for invalid in [
            "null",
            "1",
            "{}",
            r#"{"jsonrpc":"2.0","id":8,"params":[]}"#,
            r#"{"jsonrpc":"2.0","id":8,"method":42,"params":[]}"#,
            r#"{"jsonrpc":"2.0","id":8,"id":9,"method":"eth_call","params":[]}"#,
        ] {
            for payload in [
                format!("[{invalid}]"),
                format!("[{valid},{invalid}]"),
                format!("[{invalid},{valid}]"),
                format!("[{valid},{invalid},{valid}]"),
            ] {
                assert!(
                    sonic_rs::from_str::<JsonRpcRequestEnum>(&payload).is_err(),
                    "invalid batch entry was discarded: {payload}"
                );
            }
        }
    }

    #[test]
    fn batch_params_reject_malformed_nested_json() {
        for params in ["[1,]", "{\"a\":}", "[true false]", "[\"unterminated]"] {
            let payload = format!(
                r#"[{{"jsonrpc":"2.0","id":1,"method":"eth_call","params":{params}}},{{"jsonrpc":"2.0","id":2,"method":"eth_call","params":[]}}]"#
            );
            assert!(
                sonic_rs::from_str::<JsonRpcRequestEnum>(&payload).is_err(),
                "{params}"
            );
        }
    }
}
