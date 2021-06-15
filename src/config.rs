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

use crate::url::TremorUrl;
use either::Either;
use hashbrown::HashMap;

pub(crate) type Id = String;
pub(crate) type OnRampVec = Vec<OnRamp>;
pub(crate) type OffRampVec = Vec<OffRamp>;
pub(crate) type ConnectorVec = Vec<Connector>;
pub(crate) type BindingVec = Vec<Binding>;
pub(crate) type BindingMap = HashMap<TremorUrl, Vec<TremorUrl>>;
pub(crate) type MappingMap = HashMap<TremorUrl, HashMap<String, String>>;

/// A full tremor config
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "Default::default")]
    pub(crate) onramp: OnRampVec,
    #[serde(default = "Default::default")]
    pub(crate) offramp: OffRampVec,
    #[serde(default = "Default::default")]
    pub(crate) connector: ConnectorVec,
    #[serde(default = "Default::default")]
    pub(crate) binding: Vec<Binding>,
    #[serde(default = "Default::default")]
    pub(crate) mapping: MappingMap,
}

/// Configuration for an onramp
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OnRamp {
    /// ID of the onramp
    pub id: Id,
    #[serde(rename = "type")]
    pub(crate) binding_type: String,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    /// whether to enable linked transport
    #[serde(rename = "linked", default = "Default::default")]
    // TODO validate that this is turned on only for supported onramps (rest, ws)
    pub(crate) is_linked: bool,
    #[serde(default = "Default::default")]
    pub(crate) err_required: bool,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
    /// mapping from mime-type to codec used to handle requests/responses
    /// with this mime-type
    ///
    /// e.g.:
    ///       codec_map:
    ///         "application/json": "json"
    ///         "text/plain": "string"
    ///
    /// A default builtin codec mapping is defined
    /// for msgpack, json, yaml and plaintext codecs with the common mime-types
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec_map: Option<halfbrown::HashMap<String, String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) preprocessors: Option<Vec<String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) postprocessors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics_interval_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: tremor_pipeline::ConfigMap,
}

/// Configuration of an offramp
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OffRamp {
    /// ID of the offramp
    pub id: Id,
    #[serde(rename = "type")]
    pub(crate) binding_type: String,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    /// whether to enable linked transport
    #[serde(rename = "linked", default = "Default::default")]
    // TODO validate that this is turned on only for supported offramps (rest, ws)
    pub(crate) is_linked: bool,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec: Option<String>,
    /// mapping from mime-type to codec used to handle requests/responses
    /// with this mime-type
    ///
    /// e.g.:
    ///       codec_map:
    ///         "application/json": "json"
    ///         "text/plain": "string"
    ///
    /// A default builtin codec mapping is defined
    /// for msgpack, json, yaml and plaintext codecs with the common mime-types
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec_map: Option<halfbrown::HashMap<String, String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) preprocessors: Option<Vec<String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) postprocessors: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metrics_interval_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: tremor_pipeline::ConfigMap,
}

/// reconnect configuration, controlling the intervals and amound of retries
/// if a connection attempt fails
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconnectConfig {
    #[serde(default = "ReconnectConfig::default_interval_ms")]
    pub(crate) interval_ms: u64,
    #[serde(default = "ReconnectConfig::default_growth_rate")]
    pub(crate) growth_rate: f64,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) max_retry: Option<u64>,
}

impl ReconnectConfig {
    const DEFAULT_INTERVAL_MS: u64 = 1000;
    const DEFAULT_GROWTH_RATE: f64 = 1.2;

    fn new(interval_ms: u64) -> Self {
        Self {
            interval_ms,
            ..Self::default()
        }
    }

    fn default_interval_ms() -> u64 {
        Self::DEFAULT_INTERVAL_MS
    }

    fn default_growth_rate() -> f64 {
        Self::DEFAULT_GROWTH_RATE
    }
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            interval_ms: Self::DEFAULT_INTERVAL_MS,
            growth_rate: Self::DEFAULT_GROWTH_RATE,
            max_retry: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodecConfig {
    pub(crate) name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: tremor_pipeline::ConfigMap,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Connector {
    pub id: Id,
    #[serde(rename = "type")]
    pub(crate) binding_type: String,

    #[serde(default = "Default::default")]
    pub(crate) description: String,

    #[serde(
        with = "either::serde_untagged_optional",
        skip_serializing_if = "Option::is_none",
        default = "Default::default"
    )]
    pub(crate) codec: Option<Either<String, CodecConfig>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) config: tremor_pipeline::ConfigMap,

    // TODO: interceptor chain or pre- and post-processors
    /// mapping from mime-type to codec used to handle requests/responses
    /// with this mime-type
    ///
    /// e.g.:
    ///       codec_map:
    ///         "application/json": "json"
    ///         "text/plain": "string"
    ///
    /// A default builtin codec mapping is defined
    /// for msgpack, json, yaml and plaintext codecs with the common mime-types
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) codec_map: Option<halfbrown::HashMap<String, String>>,

    // TODO: interceptors or configurable processors
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) preprocessors: Option<Vec<String>>,
    #[serde(default = "Default::default", skip_serializing_if = "Option::is_none")]
    pub(crate) postprocessors: Option<Vec<String>>,

    #[serde(default)]
    pub(crate) reconnect: ReconnectConfig,
}

/// Configuration for a Binding
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    /// ID of the binding
    pub id: Id,
    #[serde(default = "Default::default")]
    pub(crate) description: String,
    pub(crate) links: BindingMap, // is this right? this should be url to url?
}
