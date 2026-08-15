use alloy::transports::TransportError;
use serde::{Deserialize, Serialize};
use sonic_rs::Value;
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
    pub data: Option<Value>,
}

impl JsonRpcErrorData {
    pub fn num_bytes(&self) -> u64 {
        sonic_rs::to_string(self)
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

impl<'a> TryFrom<&'a TransportError> for JsonRpcErrorData {
    type Error = &'a TransportError;

    fn try_from(error: &'a TransportError) -> Result<Self, Self::Error> {
        if let Some(payload) = error.as_error_resp() {
            let data = payload.data.as_deref().map(|raw| {
                sonic_rs::from_str(raw.get()).expect("Alloy error data must contain valid JSON")
            });

            Ok(Self {
                code: payload.code,
                message: payload.message.clone(),
                data,
            })
        } else {
            Err(error)
        }
    }
}
