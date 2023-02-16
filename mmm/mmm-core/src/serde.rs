use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::string::ToString;

pub mod empty_string_is_none {
    use super::*;
    /// Serialize a string from `Option<T>` using `AsRef<str>` or using the empty string if `None`.
    pub fn serialize<T, S>(opt_t: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: ToString + Serialize,
        S: Serializer,
    {
        opt_t.serialize(serializer)
    }

    pub use serde_with::rust::string_empty_as_none::deserialize;
}

pub fn deny_empty_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s.is_empty() {
        Err(serde::de::Error::missing_field(
            "empty string is not allowed.",
        ))
    } else {
        Ok(s)
    }
}
