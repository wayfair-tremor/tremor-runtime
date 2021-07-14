// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::errors::Result;
use tremor_script::Value;
pub(crate) mod binary;
pub(crate) mod binflux;
pub(crate) mod influx;
pub(crate) mod json;
pub(crate) mod msgpack;
pub(crate) mod null;
pub(crate) mod statsd;
pub(crate) mod string;
pub(crate) mod syslog;
pub(crate) mod yaml;

const MIME_TYPES: [&str; 8] = [
    "application/json",
    "application/yaml",
    "text/plain",
    "text/html",
    "application/msgpack",
    "application/x-msgpack",
    "application/vnd.msgpack",
    "application/octet-stream",
];

mod prelude {
    pub use super::Codec;
    pub use crate::errors::*;
    pub use tremor_script::prelude::*;
    pub use tremor_script::{Object, Value};
}

/// The codec trait, to encode and decode data
pub trait Codec: Send + Sync {
    /// The canonical name for this codec
    fn name(&self) -> &str;

    /// supported mime types
    /// as <base>/<subtype>
    ///
    /// e.g. application/json
    ///
    /// The returned mime types should be unique to this codec
    #[cfg(not(tarpaulin_include))]
    fn mime_types(&self) -> Vec<&str> {
        vec![]
    }

    /// Decode a binary, into an Value
    /// If `None` is returned, no data could be encoded, but we don't exactly triggered an error condition.
    ///
    /// # Errors
    ///  * if we can't decode the data
    fn decode<'input>(
        &mut self,
        data: &'input mut [u8],
        ingest_ns: u64,
    ) -> Result<Option<Value<'input>>>;
    /// Encodes a Value into a binary
    ///
    /// # Errors
    ///  * If the encoding fails
    fn encode(&self, data: &Value) -> Result<Vec<u8>>;

    /// Encodes into an existing buffer
    ///
    /// # Errors
    ///  * when we can't write encode to the given vector
    #[cfg(not(tarpaulin_include))]
    fn encode_into(&self, data: &Value, dst: &mut Vec<u8>) -> Result<()> {
        let mut res = self.encode(data)?;
        std::mem::swap(&mut res, dst);
        Ok(())
    }

    /// special clone method for getting clone functionality
    /// into a this trait referenced as trait object
    /// otherwise we cannot use this type inside structs that need to be `Clone`.
    /// See: `crate::codec::rest::BoxedCodec`
    fn boxed_clone(&self) -> Box<dyn Codec>;
}

/// Codec lookup function
///
/// # Errors
///  * if the codec doesn't exist
pub fn lookup(name: &str) -> Result<Box<dyn Codec>> {
    match name {
        "json" => Ok(Box::new(json::Json::<json::Unsorted>::default())),
        "json-sorted" => Ok(Box::new(json::Json::<json::Sorted>::default())),
        "msgpack" => Ok(Box::new(msgpack::MsgPack {})),
        "influx" => Ok(Box::new(influx::Influx {})),
        "binflux" => Ok(Box::new(binflux::BInflux {})),
        "null" => Ok(Box::new(null::Null {})),
        "string" => Ok(Box::new(string::String {})),
        "statsd" => Ok(Box::new(statsd::StatsD {})),
        "yaml" => Ok(Box::new(yaml::Yaml {})),
        "binary" => Ok(Box::new(binary::Binary {})),
        "syslog" => Ok(Box::new(syslog::Syslog::utcnow())),
        _ => Err(format!("Codec '{}' not found.", name).into()),
    }
}

/// Map from Mime types to codecs for all builtin codecs mappable to Mime types
/// these are all safe mappings
/// if you have a specific codec to be used for a more unspecific mime type
/// like statsd for text/plain
/// these must be specified in a source specific `codec_map`
#[must_use]
pub fn builtin_codec_map() -> halfbrown::HashMap<String, Box<dyn Codec>> {
    MIME_TYPES
        .iter()
        .filter_map(|t| Some(((*t).to_string(), by_mime_type(t).ok()?)))
        .collect()
}

/// lookup a codec by mime type
///
/// # Errors
/// if no codec could be found for the given mime type
pub fn by_mime_type(mime: &str) -> Result<Box<dyn Codec>> {
    match mime {
        "application/json" => Ok(Box::new(json::Json::<json::Unsorted>::default())),
        "application/yaml" => Ok(Box::new(yaml::Yaml {})),
        "text/plain" | "text/html" => Ok(Box::new(string::String {})),
        "application/msgpack" | "application/x-msgpack" | "application/vnd.msgpack" => {
            Ok(Box::new(msgpack::MsgPack {}))
        }
        "application/octet-stream" => Ok(Box::new(binary::Binary {})),
        _ => Err(format!("No codec found for mime type '{}'", mime).into()),
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn lookup() {
        assert!(super::lookup("json").is_ok());
        assert!(super::lookup("json-sorted").is_ok());
        assert!(super::lookup("msgpack").is_ok());
        assert!(super::lookup("influx").is_ok());
        assert!(super::lookup("binflux").is_ok());
        assert!(super::lookup("null").is_ok());
        assert!(super::lookup("string").is_ok());
        assert!(super::lookup("statsd").is_ok());
        assert!(super::lookup("yaml").is_ok());
        assert!(super::lookup("syslog").is_ok());
        assert_eq!(
            super::lookup("snot").err().unwrap().to_string(),
            "Codec 'snot' not found."
        )
    }

    #[test]
    fn builtin_codec_map() {
        let map = super::builtin_codec_map();

        for t in super::MIME_TYPES.iter() {
            assert!(map.contains_key(*t));
        }
    }
    #[test]
    fn by_mime_type() {
        for t in super::MIME_TYPES.iter() {
            assert!(super::by_mime_type(t).is_ok());
        }
        assert_eq!(
            super::by_mime_type("application/badger")
                .err()
                .unwrap()
                .to_string(),
            "No codec found for mime type 'application/badger'"
        );
    }
}
