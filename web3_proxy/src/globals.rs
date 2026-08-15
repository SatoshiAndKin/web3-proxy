use crate::app::App;
use std::sync::{Arc, OnceLock};

pub static APP: OnceLock<Arc<App>> = OnceLock::new();
