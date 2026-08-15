use derive_more::From;
use sonic_rs::OwnedLazyValue;

/// being strict on id doesn't really help much. just accept anything
#[derive(From)]
pub enum LooseId {
    None,
    Number(u64),
    String(String),
    Raw(OwnedLazyValue),
}

impl LooseId {
    pub fn into_lazy_value(self) -> OwnedLazyValue {
        match self {
            Self::None => Default::default(),
            Self::Number(x) => sonic_rs::to_lazyvalue(&x).expect("number id should always work"),
            Self::String(x) => sonic_rs::to_lazyvalue(&x).expect("string id should always work"),
            Self::Raw(x) => x,
        }
    }
}
