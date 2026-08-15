use ethers::providers::{HttpClientError, JsonRpcError, ProviderError, WsClientError};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

// TODO: impl Error on this?
/// All jsonrpc errors use this structure
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct JsonRpcErrorData {
    /// The error code
    pub code: i64,
    /// The error message
    pub message: Cow<'static, str>,
    /// Additional data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl JsonRpcErrorData {
    pub fn num_bytes(&self) -> u64 {
        serde_json::to_string(self)
            .expect("should always serialize")
            .len() as u64
    }

    // pub fn is_retryable(&self) -> bool {
    //     // TODO: move stuff from request to here
    //     todo!()
    // }
}

impl From<&'static str> for JsonRpcErrorData {
    fn from(value: &'static str) -> Self {
        Self {
            code: -32000,
            message: value.into(),
            data: None,
        }
    }
}

impl From<String> for JsonRpcErrorData {
    fn from(value: String) -> Self {
        Self {
            code: -32000,
            message: value.into(),
            data: None,
        }
    }
}

impl From<&JsonRpcError> for JsonRpcErrorData {
    fn from(value: &JsonRpcError) -> Self {
        Self {
            code: value.code,
            message: value.message.clone().into(),
            data: value.data.clone(),
        }
    }
}

impl<'a> TryFrom<&'a ProviderError> for JsonRpcErrorData {
    type Error = &'a ProviderError;

    fn try_from(error: &'a ProviderError) -> Result<Self, Self::Error> {
        match error {
            provider_error @ ProviderError::JsonRpcClientError(client_error) => client_error
                .as_error_response()
                .map(Self::from)
                .ok_or(provider_error),
            error => Err(error),
        }
    }
}

impl<'a> TryFrom<&'a HttpClientError> for JsonRpcErrorData {
    type Error = &'a HttpClientError;

    fn try_from(error: &'a HttpClientError) -> Result<Self, Self::Error> {
        match error {
            HttpClientError::JsonRpcError(error) => Ok(error.into()),
            error => Err(error),
        }
    }
}

impl<'a> TryFrom<&'a WsClientError> for JsonRpcErrorData {
    type Error = &'a WsClientError;

    fn try_from(error: &'a WsClientError) -> Result<Self, Self::Error> {
        match error {
            WsClientError::JsonRpcError(error) => Ok(error.into()),
            error => Err(error),
        }
    }
}
