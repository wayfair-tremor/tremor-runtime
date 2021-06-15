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

use crate::errors::{Error, Result};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// The type or resource the url references
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum ResourceType {
    /// This is a pipeline
    Pipeline,
    /// This is an onramp
    Onramp,
    /// This is an offramp
    Offramp,
    /// connector
    Connector,
    /// This is a binding
    Binding,
}

/// The scrope of the URL
#[derive(Clone, Copy, Eq, PartialEq, Hash, Debug)]
pub enum Scope {
    /// This URL identifies a specific port
    Port,
    /// This URL identifies a servant (a running instance)
    Servant,
    /// This URL identifes an artefact (a non running configuration)
    Artefact,
    /// This URL identifies a type of artefact
    Type,
}

/// module for standard port names
pub mod ports {
    use beef::Cow;

    /// standard input port
    pub const IN: Cow<'static, str> = Cow::const_str("in");

    /// standard output port
    pub const OUT: Cow<'static, str> = Cow::const_str("out");

    /// standard err port
    pub const ERR: Cow<'static, str> = Cow::const_str("err");

    /// standard metrics port
    pub const METRICS: Cow<'static, str> = Cow::const_str("metrics");
}

/// A tremor URL identifying an entity in tremor

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct TremorUrl {
    scope: Scope,
    host: String,
    resource_type: Option<ResourceType>,
    artefact: Option<String>,
    instance: Option<String>,
    instance_port: Option<String>,
}

fn decode_type(t: &str) -> Result<ResourceType> {
    match t {
        "pipeline" => Ok(ResourceType::Pipeline),
        "onramp" => Ok(ResourceType::Onramp),
        "offramp" => Ok(ResourceType::Offramp),
        "binding" => Ok(ResourceType::Binding),
        "connector" => Ok(ResourceType::Connector),
        other => Err(format!("Bad Resource type: {}", other).into()),
    }
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Pipeline => write!(f, "pipeline"),
            Self::Onramp => write!(f, "onramp"),
            Self::Offramp => write!(f, "offramp"),
            Self::Connector => write!(f, "connector"),
            Self::Binding => write!(f, "binding"),
        }
    }
}

impl fmt::Display for TremorUrl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let r = write!(f, "tremor://{}", self.host);
        if let Some(resource_type) = &self.resource_type {
            write!(f, "/{}", resource_type)?;
            if let Some(artefact) = &self.artefact {
                write!(f, "/{}", artefact)?;
                if let Some(instance) = &self.instance {
                    write!(f, "/{}", instance)?;
                    if let Some(instance_port) = &self.instance_port {
                        write!(f, "/{}", instance_port)?;
                    };
                };
            };
        };
        r
    }
}

impl TremorUrl {
    /// Generates a minimal id of the form "{pfx}-{artefact}.{instance}"
    #[must_use]
    pub fn short_id(&self, pfx: &str) -> String {
        let artefact_id = self.artefact().unwrap_or("-");
        let instance_id = self.instance().unwrap_or("-");
        format!("{}-{}.{}", pfx, artefact_id, instance_id)
    }
    /// Creates an URL from a given onramp id
    ///
    /// # Errors
    ///  * if the id isn't a valid onramp id
    pub fn from_onramp_id(id: &str) -> Result<Self> {
        Self::parse(&format!("/onramp/{}", id))
    }

    /// Creates an URL from a given offramp id
    ///
    /// # Errors
    ///  * if the passed ID isn't a valid offramp id
    pub fn from_offramp_id(id: &str) -> Result<Self> {
        Self::parse(&format!("/offramp/{}", id))
    }

    pub fn from_connector_id(id: &str) -> Result<Self> {
        Self::parse(&format!("/connector/{}", id))
    }

    /// Parses a string into a Trmeor URL
    ///
    /// # Errors
    ///  * If the url is not a valid tremor url
    pub fn parse(url: &str) -> Result<Self> {
        let (r, relative) = Self::parse_url(url, false)?;

        if let Some(parts) = r
            .path_segments()
            .map(std::iter::Iterator::collect::<Vec<_>>)
        {
            let (scope, resource_type, artefact, instance, instance_port) = if relative {
                // TODO: This is not correct!
                match parts.as_slice() {
                    [port] => (Scope::Servant, None, None, None, Some((*port).to_string())),
                    [instance, port] => (
                        Scope::Type,
                        None,
                        None,
                        Some((*instance).to_string()),
                        Some((*port).to_string()),
                    ),
                    [artefact, instance, port] => (
                        Scope::Artefact,
                        None,
                        Some((*artefact).to_string()),
                        Some((*instance).to_string()),
                        Some((*port).to_string()),
                    ),
                    [resource_type, artefact, instance, port] => (
                        Scope::Port,
                        Some(decode_type(resource_type)?),
                        Some((*artefact).to_string()),
                        Some((*instance).to_string()),
                        Some((*port).to_string()),
                    ),

                    _ => return Err(format!("Bad URL: {}", url).into()),
                }
            } else {
                match parts.as_slice() {
                    [resource_type, artefact, instance, port] => (
                        Scope::Port,
                        Some(decode_type(resource_type)?),
                        Some((*artefact).to_string()),
                        Some((*instance).to_string()),
                        Some((*port).to_string()),
                    ),
                    [resource_type, artefact, instance] => (
                        Scope::Servant,
                        Some(decode_type(resource_type)?),
                        Some((*artefact).to_string()),
                        Some((*instance).to_string()),
                        None,
                    ),
                    [resource_type, artefact] => (
                        Scope::Artefact,
                        Some(decode_type(resource_type)?),
                        Some((*artefact).to_string()),
                        None,
                        None,
                    ),
                    [resource_type] => (
                        Scope::Type,
                        Some(decode_type(resource_type)?),
                        None,
                        None,
                        None,
                    ),

                    _ => return Err(format!("Bad URL: {}", url).into()),
                }
            };

            let host = r.host_str().unwrap_or("localhost").to_owned();
            Ok(Self {
                scope,
                host,
                resource_type,
                artefact,
                instance,
                instance_port,
            })
        } else {
            Err(format!("Bad URL: {}", url).into())
        }
    }

    fn parse_url(url: &str, relative: bool) -> Result<(url::Url, bool)> {
        match url::Url::parse(url) {
            Ok(r) => Ok((r, relative)),
            Err(url::ParseError::RelativeUrlWithoutBase) => match url.get(0..1) {
                Some("/") => Self::parse_url(&format!("tremor://{}", url), false),
                _ => Self::parse_url(&format!("tremor:///{}", url), true),
            },
            Err(e) => Err(e.into()),
        }
    }

    /// Trims the url to the instance
    pub fn trim_to_instance(&mut self) {
        self.instance_port = None;
        self.scope = Scope::Servant;
    }

    /// Trims the url to the artefact
    pub fn trim_to_artefact(&mut self) {
        self.instance_port = None;
        self.instance = None;
        self.scope = Scope::Artefact;
    }

    /// Sets the instance of the URL, will extend
    /// the scope to `Scope::Servant` if it was
    /// `Artefact` before.
    pub fn set_instance<S>(&mut self, i: &S)
    where
        S: ToString + ?Sized,
    {
        self.instance = Some(i.to_string());
        if self.scope == Scope::Artefact {
            self.scope = Scope::Servant;
        }
    }

    /// Sets the port of the URL, will extend
    /// the scope to `Scope::Port` if it was
    /// `Servant` before.
    pub fn set_port<S>(&mut self, i: &S)
    where
        S: ToString + ?Sized,
    {
        self.instance_port = Some(i.to_string());
        if self.scope == Scope::Servant {
            self.scope = Scope::Port;
        }
    }
    /// Retrives the instance
    #[must_use]
    pub fn instance(&self) -> Option<&str> {
        self.instance.as_deref()
    }
    /// Retrives the artefact
    #[must_use]
    pub fn artefact(&self) -> Option<&str> {
        self.artefact.as_deref()
    }
    /// Retrives the port
    #[must_use]
    pub fn instance_port(&self) -> Option<&str> {
        self.instance_port.as_deref()
    }
    /// Retrives the port
    ///
    /// # Errors
    ///  * if the URL has no port
    pub fn instance_port_required(&self) -> Result<&str> {
        self.instance_port()
            .ok_or_else(|| Error::from(format!("{} is missing an instance port", self)))
    }
    /// Retrives the type
    #[must_use]
    pub fn resource_type(&self) -> Option<ResourceType> {
        self.resource_type
    }
    /// Retrives the scope
    #[must_use]
    pub fn scope(&self) -> Scope {
        self.scope
    }

    /// returns true if `self` and `other` refer to the same instance, ignoring the port
    #[must_use]
    pub fn same_instance_as(&self, other: &Self) -> bool {
        self.host == other.host
            && self.resource_type == other.resource_type
            && self.artefact == other.artefact
            && self.instance == other.instance
    }
}

impl<'de> Deserialize<'de> for TremorUrl {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(|err| Error::custom(err.to_string()))
    }
}

impl Serialize for TremorUrl {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("{}", self))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn bad_url() {
        assert!(TremorUrl::parse("snot://").is_err())
    }

    #[test]
    fn bad_url2() {
        assert!(TremorUrl::parse("foo/bar/baz/bogo/mips/snot").is_err())
    }

    #[test]
    fn url() -> Result<()> {
        let url = TremorUrl::parse("tremor://127.0.0.1:1234/pipeline/main/01/in?format=json")?;

        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Pipeline), url.resource_type());
        assert_eq!(Some("main"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some("in"), url.instance_port());
        Ok(())
    }

    #[test]
    fn short_url() -> Result<()> {
        let url = TremorUrl::parse("/pipeline/main/01/in")?;

        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Pipeline), url.resource_type());
        assert_eq!(Some("main"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());
        Ok(())
    }

    #[test]
    fn from_onramp_id() -> Result<()> {
        let url = TremorUrl::from_onramp_id("test")?;
        assert_eq!(Some(ResourceType::Onramp), url.resource_type());
        assert_eq!(Some("test"), url.artefact());
        Ok(())
    }

    #[test]
    fn from_offramp_id() -> Result<()> {
        let url = TremorUrl::from_offramp_id("test")?;
        assert_eq!(Some(ResourceType::Offramp), url.resource_type());
        assert_eq!(Some("test"), url.artefact());
        Ok(())
    }

    #[test]
    fn from_connector_id() -> Result<()> {
        let url = TremorUrl::from_connector_id("test")?;
        assert_eq!(Some(ResourceType::Connector), url.resource_type());
        assert_eq!(Some("test"), url.artefact());
        Ok(())
    }

    #[test]
    fn test_servant_scope() -> Result<()> {
        let url = TremorUrl::parse("in")?;
        assert_eq!(Scope::Servant, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(None, url.artefact());
        Ok(())
    }

    #[test]
    fn test_type_scope() -> Result<()> {
        let url = TremorUrl::parse("01/in")?;
        assert_eq!(Scope::Type, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(None, url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());
        Ok(())
    }

    #[test]
    fn test_artefact_scope() -> Result<()> {
        let url = TremorUrl::parse("pipe/01/in")?;
        assert_eq!(Scope::Artefact, url.scope());
        assert_eq!(None, url.resource_type());
        assert_eq!(Some("pipe"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());
        Ok(())
    }

    #[test]
    fn test_port_scope() -> Result<()> {
        let url = TremorUrl::parse("binding/pipe/01/in")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Binding), url.resource_type());
        assert_eq!(Some("pipe"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());

        let url = TremorUrl::parse("onramp/id/01/out")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Onramp), url.resource_type());
        assert_eq!(Some("id"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::OUT.as_ref()), url.instance_port());

        let url = TremorUrl::parse("offramp/id/01/in")?;
        assert_eq!(Scope::Port, url.scope());
        assert_eq!(Some(ResourceType::Offramp), url.resource_type());
        assert_eq!(Some("id"), url.artefact());
        assert_eq!(Some("01"), url.instance());
        assert_eq!(Some(ports::IN.as_ref()), url.instance_port());

        Ok(())
    }

    #[test]
    fn test_set_instance() -> Result<()> {
        let mut url = TremorUrl::parse("tremor://127.0.0.1:1234/pipeline/main")?;
        assert_eq!(Scope::Artefact, url.scope());
        assert_eq!(Some(ResourceType::Pipeline), url.resource_type());
        assert_eq!(Some("main"), url.artefact());
        assert_eq!(None, url.instance());
        url.set_instance("inst");
        assert_eq!(Scope::Servant, url.scope());
        assert_eq!(Some("inst"), url.instance());

        Ok(())
    }
}
