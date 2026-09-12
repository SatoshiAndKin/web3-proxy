use super::JsonRpcErrorData;
use crate::errors::{Web3ProxyError, Web3ProxyResult};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use serde::{de, Deserialize, Serialize};
use sonic_rs::{JsonValueTrait, OwnedLazyValue, Value};
use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

pub(crate) fn json_response<T>(status: StatusCode, value: &T) -> axum::response::Response
where
    T: Serialize,
{
    let body = sonic_rs::to_vec(value).expect("JSON response values must serialize");
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

/// A JSON-RPC result or error without a request ID.
#[derive(Clone, Debug)]
pub enum ResponseData<T> {
    Result {
        value: T,
        num_bytes: u64,
    },
    RpcError {
        error_data: JsonRpcErrorData,
        num_bytes: u64,
    },
}

impl<T> ResponseData<T> {
    pub fn num_bytes(&self) -> u64 {
        match self {
            Self::Result { num_bytes, .. } | Self::RpcError { num_bytes, .. } => *num_bytes,
        }
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::RpcError { .. })
    }
}

impl<T> ResponseData<Option<T>> {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Result { value: None, .. })
    }
}

impl ResponseData<Arc<OwnedLazyValue>> {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Result { value, .. } if value.is_null())
    }
}

impl From<Value> for ResponseData<Arc<OwnedLazyValue>> {
    fn from(value: Value) -> Self {
        sonic_rs::to_lazyvalue(&value)
            .expect("JSON values must serialize")
            .into()
    }
}

impl From<Arc<OwnedLazyValue>> for ResponseData<Arc<OwnedLazyValue>> {
    fn from(value: Arc<OwnedLazyValue>) -> Self {
        let num_bytes = sonic_rs::to_string(&value)
            .expect("JSON values must serialize")
            .len() as u64;
        Self::Result { value, num_bytes }
    }
}

impl From<OwnedLazyValue> for ResponseData<Arc<OwnedLazyValue>> {
    fn from(value: OwnedLazyValue) -> Self {
        Arc::new(value).into()
    }
}

impl From<ParsedResponse<Arc<OwnedLazyValue>>> for ResponseData<Arc<OwnedLazyValue>> {
    fn from(response: ParsedResponse<Arc<OwnedLazyValue>>) -> Self {
        match response.payload {
            ResponsePayload::Success { result } => result.into(),
            ResponsePayload::Error { error } => error.into(),
        }
    }
}

impl<T> From<JsonRpcErrorData> for ResponseData<T> {
    fn from(error_data: JsonRpcErrorData) -> Self {
        let num_bytes = sonic_rs::to_string(&error_data)
            .expect("JSON-RPC errors must serialize")
            .len() as u64;
        Self::RpcError {
            error_data,
            num_bytes,
        }
    }
}

pub trait JsonRpcParams = fmt::Debug + serde::Serialize + Send + Sync + 'static;
pub trait JsonRpcResultData =
    serde::Serialize + serde::de::DeserializeOwned + fmt::Debug + Send + Sync + Unpin + 'static;

/// TODO: borrow values to avoid allocs if possible
/// TODO: reduce overlap with `SingleResponse`.
#[derive(Debug, Serialize)]
pub struct ParsedResponse<T = Arc<OwnedLazyValue>> {
    pub jsonrpc: Cow<'static, str>,
    pub id: OwnedLazyValue,
    #[serde(flatten)]
    pub payload: ResponsePayload<T>,
}

impl ParsedResponse {
    #[inline]
    pub fn from_value(value: Value, id: OwnedLazyValue) -> Self {
        let result = sonic_rs::to_lazyvalue(&value)
            .expect("this should not fail")
            .into();
        Self::from_result(result, id)
    }
}

impl ParsedResponse<Arc<OwnedLazyValue>> {
    #[inline]
    pub fn from_response_data(data: ResponseData<Arc<OwnedLazyValue>>, id: OwnedLazyValue) -> Self {
        match data {
            ResponseData::RpcError { error_data, .. } => Self::from_error(error_data, id),
            ResponseData::Result { value, .. } => Self::from_result(value, id),
        }
    }
}

impl<T> ParsedResponse<T> {
    #[inline]
    pub fn from_result(result: T, id: OwnedLazyValue) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            payload: ResponsePayload::Success { result },
        }
    }

    #[inline]
    pub fn from_error(error: JsonRpcErrorData, id: OwnedLazyValue) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            payload: ResponsePayload::Error { error },
        }
    }

    #[inline]
    pub fn result(&self) -> Option<&T> {
        match &self.payload {
            ResponsePayload::Success { result } => Some(result),
            ResponsePayload::Error { .. } => None,
        }
    }

    #[inline]
    pub fn into_result(self) -> Web3ProxyResult<T> {
        match self.payload {
            ResponsePayload::Success { result } => Ok(result),
            ResponsePayload::Error { error } => Err(Web3ProxyError::JsonRpcErrorData(error)),
        }
    }
}

impl<'de, T> Deserialize<'de> for ParsedResponse<T>
where
    T: de::DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ResponseVisitor<T>(PhantomData<T>);
        impl<'de, T> de::Visitor<'de> for ResponseVisitor<T>
        where
            T: de::DeserializeOwned,
        {
            type Value = ParsedResponse<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a valid jsonrpc 2.0 response object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut jsonrpc = None;

                // response & error
                let mut id = None;
                // only response
                let mut result = None;
                // only error
                let mut error = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        "jsonrpc" => {
                            if jsonrpc.is_some() {
                                return Err(de::Error::duplicate_field("jsonrpc"));
                            }

                            let value = map.next_value()?;
                            if value != "2.0" {
                                return Err(de::Error::invalid_value(
                                    de::Unexpected::Str(value),
                                    &"2.0",
                                ));
                            }

                            jsonrpc = Some(value);
                        }
                        "id" => {
                            if id.is_some() {
                                return Err(de::Error::duplicate_field("id"));
                            }

                            let value: OwnedLazyValue = map.next_value()?;
                            id = Some(value);
                        }
                        "result" => {
                            if result.is_some() {
                                return Err(de::Error::duplicate_field("result"));
                            }

                            let value: T = map.next_value()?;
                            result = Some(value);
                        }
                        "error" => {
                            if error.is_some() {
                                return Err(de::Error::duplicate_field("Error"));
                            }

                            let value: JsonRpcErrorData = map.next_value()?;
                            error = Some(value);
                        }
                        key => {
                            return Err(de::Error::unknown_field(
                                key,
                                &["jsonrpc", "id", "result", "error"],
                            ));
                        }
                    }
                }

                let id = id.ok_or_else(|| de::Error::missing_field("id"))?;
                if !(id.is_null() || id.is_str() || id.is_number()) {
                    return Err(de::Error::custom(
                        "response ID must be a string, number, or null",
                    ));
                }

                // jsonrpc version must be present in all responses
                let jsonrpc = jsonrpc
                    .ok_or_else(|| de::Error::missing_field("jsonrpc"))?
                    .to_string()
                    .into();

                let payload = match (result, error) {
                    (Some(result), None) => ResponsePayload::Success { result },
                    (None, Some(error)) => ResponsePayload::Error { error },
                    _ => {
                        return Err(de::Error::custom(
                            "response must be either a success or error object",
                        ))
                    }
                };

                Ok(ParsedResponse {
                    jsonrpc,
                    id,
                    payload,
                })
            }
        }

        deserializer.deserialize_map(ResponseVisitor(PhantomData))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum ResponsePayload<T> {
    Success { result: T },
    Error { error: JsonRpcErrorData },
}

#[derive(Debug)]
pub enum SingleResponse<T = Arc<OwnedLazyValue>> {
    /// TODO: save the size here so repeated serialization is not necessary.
    Parsed(ParsedResponse<T>),
}

impl<T> SingleResponse<T>
where
    T: de::DeserializeOwned + Serialize,
{
    pub fn is_jsonrpc_err(&self) -> bool {
        match self {
            Self::Parsed(resp, ..) => matches!(resp.payload, ResponsePayload::Error { .. }),
        }
    }

    /// Return the fully read and validated response.
    pub async fn parsed(self) -> Web3ProxyResult<ParsedResponse<T>> {
        let Self::Parsed(response) = self;
        Ok(response)
    }

    pub fn num_bytes(&self) -> u64 {
        match self {
            Self::Parsed(response) => sonic_rs::to_string(response)
                .expect("this should always serialize")
                .len() as u64,
        }
    }

    pub fn set_id(&mut self, id: OwnedLazyValue) {
        match self {
            SingleResponse::Parsed(x, ..) => {
                x.id = id;
            }
        }
    }
}

impl<T> From<ParsedResponse<T>> for SingleResponse<T> {
    fn from(response: ParsedResponse<T>) -> Self {
        Self::Parsed(response)
    }
}

impl<T> IntoResponse for SingleResponse<T>
where
    T: Serialize,
{
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Parsed(resp, ..) => json_response(StatusCode::OK, &resp),
        }
    }
}

#[derive(Debug)]
pub enum Response<T = Arc<OwnedLazyValue>> {
    Single(SingleResponse<T>),
    Batch(Vec<ParsedResponse<T>>),
}

impl Response<Arc<OwnedLazyValue>> {
    pub async fn to_json_string(self) -> Web3ProxyResult<String> {
        let x = match self {
            Self::Single(resp) => {
                let parsed = resp.parsed().await?;

                sonic_rs::to_string(&parsed)
            }
            Self::Batch(resps) => sonic_rs::to_string(&resps),
        };

        let x = x.expect("to_string should always work");

        Ok(x)
    }
}

impl<T> From<ParsedResponse<T>> for Response<T> {
    fn from(response: ParsedResponse<T>) -> Self {
        Self::Single(SingleResponse::Parsed(response))
    }
}

impl<T> IntoResponse for Response<T>
where
    T: Serialize,
{
    fn into_response(self) -> axum::response::Response {
        match self {
            Self::Single(resp) => resp.into_response(),
            Self::Batch(resps) => json_response(StatusCode::OK, &resps),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ParsedResponse, ResponseData};
    use crate::jsonrpc::JsonRpcErrorData;
    use sonic_rs::OwnedLazyValue;
    use std::sync::Arc;

    #[test]
    fn parsed_success_converts_to_response_data() {
        let result = Arc::new(sonic_rs::from_str::<OwnedLazyValue>("42").unwrap());
        let response = ParsedResponse::from_result(result, Default::default());

        let response_data: ResponseData<Arc<OwnedLazyValue>> = response.into();

        match response_data {
            ResponseData::Result { value, num_bytes } => {
                assert_eq!(sonic_rs::to_string(&value).unwrap(), "42");
                assert_eq!(num_bytes, 2);
            }
            ResponseData::RpcError { .. } => panic!("expected successful response data"),
        }
    }

    #[test]
    fn parsed_error_converts_to_response_data() {
        let error = JsonRpcErrorData {
            code: -32000,
            message: "request failed".into(),
            data: Some(sonic_rs::json!({ "retryable": false })),
        };
        let expected_num_bytes = error.num_bytes();
        let response = ParsedResponse::<Arc<OwnedLazyValue>>::from_error(error, Default::default());

        let response_data: ResponseData<Arc<OwnedLazyValue>> = response.into();

        match response_data {
            ResponseData::RpcError {
                error_data,
                num_bytes,
            } => {
                assert_eq!(error_data.code, -32000);
                assert_eq!(error_data.message, "request failed");
                assert_eq!(
                    sonic_rs::to_string(&error_data.data).unwrap(),
                    r#"{"retryable":false}"#
                );
                assert_eq!(num_bytes, expected_num_bytes);
            }
            ResponseData::Result { .. } => panic!("expected error response data"),
        }
    }
}
