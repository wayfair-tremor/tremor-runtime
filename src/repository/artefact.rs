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

use crate::errors::{Error, ErrorKind, Result};
use crate::metrics::RampReporter;
use crate::onramp;
use crate::pipeline;
use crate::registry::ServantId;
use crate::system::{self, World};
use crate::url::{ResourceType, TremorUrl};
use crate::{codec, pipeline::ConnectTarget};
use crate::{connectors, offramp, url};
use beef::Cow;
use hashbrown::HashMap;
use std::collections::HashSet;
use tremor_pipeline::query;
pub(crate) type Id = TremorUrl;
pub(crate) use crate::Connector as ConnectorArtefact;
pub(crate) use crate::OffRamp as OfframpArtefact;
pub(crate) use crate::OnRamp as OnrampArtefact;
use async_channel::bounded;
use async_trait::async_trait;

/// A Binding
#[derive(Clone, Debug)]
pub struct Binding {
    /// The binding itself
    pub binding: crate::Binding,
    /// The mappings
    pub mapping: Option<crate::config::MappingMap>,
}

/// A Pipeline
pub type Pipeline = query::Query;

#[async_trait]
pub trait Artefact: Clone {
    //    type Configuration;
    type SpawnResult: Clone;
    type LinkResult: Clone;
    type LinkLHS: Clone;
    type LinkRHS: Clone;

    /// Move from Repository to Registry
    async fn spawn(&self, system: &World, servant_id: ServantId) -> Result<Self::SpawnResult>;
    /// Move from Registry(instantiated) to Registry(Active) or from one form of active to another
    /// This acts differently on bindings and the rest. Where the binding takers a mapping of string
    /// replacements, the others take a from and to id
    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult>;

    async fn unlink(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool>;

    fn resource_type() -> url::ResourceType;

    fn artefact_id(id: &TremorUrl) -> Result<Id> {
        let mut id = id.clone();
        id.trim_to_artefact();
        let rt = Self::resource_type();
        match (id.resource_type(), id.artefact()) {
            (Some(id_rt), Some(_id)) if id_rt == rt => Ok(id),
            _ => Err(ErrorKind::InvalidTremorUrl(format!(
                "URL does not contain a {} artifact id: {}",
                rt, id
            ))
            .into()),
        }
    }
    fn servant_id(id: &TremorUrl) -> Result<ServantId> {
        let mut id = id.clone();
        id.trim_to_instance();
        let rt = Self::resource_type();
        match (id.resource_type(), id.instance()) {
            (Some(id_rt), Some(_id)) if id_rt == rt => Ok(id),
            _ => Err(ErrorKind::InvalidTremorUrl(format!(
                "URL does not contain a {} servant id: {}",
                rt, id
            ))
            .into()),
        }
    }
}

#[async_trait]
impl Artefact for Pipeline {
    type SpawnResult = pipeline::Addr;
    type LinkResult = bool;
    type LinkLHS = String;
    type LinkRHS = TremorUrl;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Pipeline
    }

    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        world.start_pipeline(self.clone(), servant_id).await
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        if let Some(pipeline) = system.reg.find_pipeline(id).await? {
            let mut msgs = Vec::with_capacity(mappings.len());
            for (from, to) in mappings {
                let target = match to.resource_type() {
                    Some(ResourceType::Offramp) => {
                        if let Some(offramp) = system.reg.find_offramp(&to).await? {
                            ConnectTarget::Offramp(offramp)
                        } else {
                            return Err(format!("Offramp {} not found", to).into());
                        }
                    }
                    Some(ResourceType::Pipeline) => {
                        // TODO: connect both ways?
                        if let Some(p) = system.reg.find_pipeline(&to).await? {
                            ConnectTarget::Pipeline(Box::new(p))
                        } else {
                            return Err(format!("Pipeline {:?} not found", to).into());
                        }
                    }
                    Some(ResourceType::Onramp) => {
                        if let Some(onramp) = system.reg.find_onramp(&to).await? {
                            ConnectTarget::Onramp(onramp)
                        } else {
                            return Err(format!("Onramp {} not found", to).into());
                        }
                    }
                    _ => {
                        return Err(format!("Cannot link Pipeline to: {}.", to).into());
                    }
                };
                // link an output to this pipeline via outgoing port
                msgs.push(pipeline::MgmtMsg::ConnectOutput {
                    port: Cow::owned(from),
                    output_url: to,
                    target,
                });
            }
            for msg in msgs {
                pipeline.send_mgmt(msg).await.map_err(|e| -> Error {
                    format!("Could not send to pipeline: {}", e).into()
                })?;
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    async fn unlink(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Unlinking pipeline {} ..", id);
        if let Some(pipeline) = system.reg.find_pipeline(id).await? {
            for (from, to) in mappings {
                match to.resource_type() {
                    Some(ResourceType::Offramp | ResourceType::Pipeline | ResourceType::Onramp) => {
                        pipeline
                            .send_mgmt(pipeline::MgmtMsg::DisconnectOutput(from.clone().into(), to))
                            .await
                            .map_err(|_e| Error::from("Failed to unlink pipeline"))?;
                    }
                    _ => {
                        return Err("Source isn't an Offramp or Pipeline".into());
                    }
                }
            }
            info!("Pipeline {} unlinked.", id);
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }
}

#[async_trait]
impl Artefact for OfframpArtefact {
    type SpawnResult = offramp::Addr;
    type LinkResult = bool;
    type LinkLHS = TremorUrl;
    type LinkRHS = TremorUrl;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Offramp
    }

    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        //TODO: define offramp by config!
        let offramp = offramp::lookup(&self.binding_type, &self.config)?;
        // lookup codecs already here
        // this will bail out early if something is mistyped or so
        let codec = if let Some(codec) = &self.codec {
            codec::lookup(&codec)?
        } else {
            codec::lookup(offramp.default_codec())?
        };
        let mut resolved_codec_map = codec::builtin_codec_map();
        // override the builtin map
        if let Some(codec_map) = &self.codec_map {
            for (k, v) in codec_map {
                resolved_codec_map.insert(k.to_string(), codec::lookup(v.as_str())?);
            }
        }

        let preprocessors = if let Some(preprocessors) = &self.preprocessors {
            preprocessors.clone()
        } else {
            vec![]
        };

        let postprocessors = if let Some(postprocessors) = &self.postprocessors {
            postprocessors.clone()
        } else {
            vec![]
        };
        let metrics_reporter = RampReporter::new(servant_id.clone(), self.metrics_interval_s);

        let (tx, rx) = bounded(1);

        world
            .system
            .send(system::ManagerMsg::CreateOfframp(
                tx,
                Box::new(offramp::Create {
                    id: servant_id,
                    codec,
                    codec_map: resolved_codec_map,
                    offramp,
                    preprocessors,
                    postprocessors,
                    metrics_reporter,
                    is_linked: self.is_linked,
                }),
            ))
            .await?;
        rx.recv().await?
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Linking offramp {} ..", id);
        if let Some(offramp) = system.reg.find_offramp(id).await? {
            for (pipeline_id, this) in mappings {
                let port = Cow::from(this.instance_port_required()?.to_string());
                info!("Linking offramp {} to {}", this, pipeline_id);
                if let Some(pipeline) = system.reg.find_pipeline(&pipeline_id).await? {
                    offramp
                        .send(offramp::Msg::Connect {
                            port,
                            id: pipeline_id,
                            addr: Box::new(pipeline),
                        })
                        .await?;
                };
            }
            Ok(true)
        } else {
            Err(format!("Offramp {} not found for linking,", id).into())
        }
    }

    async fn unlink(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        info!("Unlinking offramp {} ..", id);
        if let Some(offramp) = system.reg.find_offramp(id).await? {
            let (tx, rx) = bounded(mappings.len());
            let mut expect_answers = mappings.len();
            for (_this, pipeline_id) in mappings {
                let port = Cow::from(id.instance_port_required()?.to_string());
                offramp
                    .send(offramp::Msg::Disconnect {
                        port,
                        id: pipeline_id,
                        tx: tx.clone(),
                    })
                    .await?;
            }
            let mut empty = false;
            while expect_answers > 0 {
                empty |= rx.recv().await?;
                expect_answers -= 1;
            }
            Ok(empty)
        } else {
            Err(format!("Offramp {} not found for unlinking,", id).into())
        }
    }
}
#[async_trait]
impl Artefact for OnrampArtefact {
    type SpawnResult = onramp::Addr;
    type LinkResult = bool;
    type LinkLHS = String;
    type LinkRHS = TremorUrl;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Onramp
    }

    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        let stream = onramp::lookup(&self.binding_type, &servant_id, &self.config)?;
        let codec = self.codec.as_ref().map_or_else(
            || stream.default_codec().to_string(),
            std::clone::Clone::clone,
        );
        let codec_map = self
            .codec_map
            .clone()
            .unwrap_or_else(|| halfbrown::HashMap::with_capacity(0));
        let preprocessors = if let Some(preprocessors) = &self.preprocessors {
            preprocessors.clone()
        } else {
            vec![]
        };
        let postprocessors = if let Some(postprocessors) = &self.postprocessors {
            postprocessors.clone()
        } else {
            vec![]
        };

        let metrics_reporter = RampReporter::new(servant_id.clone(), self.metrics_interval_s);
        let (tx, rx) = bounded(1);

        world
            .system
            .send(system::ManagerMsg::CreateOnramp(
                tx,
                Box::new(onramp::Create {
                    id: servant_id,
                    preprocessors,
                    postprocessors,
                    codec,
                    codec_map,
                    stream,
                    metrics_reporter,
                    is_linked: self.is_linked,
                    err_required: self.err_required,
                }),
            ))
            .await?;
        rx.recv().await?
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        // check if we have the right onramp
        if let Some(artefact) = id.artefact() {
            if self.id.as_str() != artefact {
                return Err(format!(
                    "Onramp for linking ({}) is not from this artifact: {}.",
                    id, self.id
                )
                .into());
            }
        }
        if let Some(onramp) = system.reg.find_onramp(id).await? {
            let mut msgs = Vec::with_capacity(mappings.len());
            for (from, to) in mappings {
                // TODO: validate that `from` - the port name - is valid (OUT, ERR, METRICS)
                if let Some(ResourceType::Pipeline) = to.resource_type() {
                    if let Some(pipeline) = system.reg.find_pipeline(&to).await? {
                        msgs.push(onramp::Msg::Connect(
                            from.into(),
                            vec![(to.clone(), pipeline)],
                        ));
                    } else {
                        return Err(format!("Pipeline {:?} not found", to).into());
                    }
                } else {
                    return Err("Destination isn't a Pipeline".into());
                }
            }
            for msg in msgs {
                onramp.send(msg).await?;
            }
            Ok(true)
        } else {
            Err(format!("Pipeline {:?} not found", id).into())
        }
    }

    async fn unlink(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        info!("Unlinking onramp {} ..", id);
        if let Some(onramp) = system.reg.find_onramp(id).await? {
            let mut links = Vec::new();
            let (tx, rx) = bounded(mappings.len());

            for to in mappings.values() {
                links.push(to.clone())
            }
            let mut expect_answers = mappings.len();
            for (_port, pipeline_id) in mappings {
                onramp
                    .send(onramp::Msg::Disconnect {
                        id: pipeline_id,
                        tx: tx.clone(),
                    })
                    .await?;
            }
            let mut empty = false;
            while expect_answers > 0 {
                empty |= rx.recv().await?;
                expect_answers -= 1;
            }

            info!("Onramp {} unlinked.", id);
            Ok(empty)
        } else {
            Err(format!("Unlinking failed Onramp {} not found ", id).into())
        }
    }
}

#[async_trait]
impl Artefact for ConnectorArtefact {
    type SpawnResult = connectors::Addr;

    type LinkResult = bool;

    type LinkLHS = TremorUrl;

    type LinkRHS = TremorUrl;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Connector
    }

    async fn spawn(&self, world: &World, servant_id: ServantId) -> Result<Self::SpawnResult> {
        let create = connectors::Create::new(servant_id.clone(), self.clone());
        let (tx, rx) = bounded(1);
        world
            .system
            .send(system::ManagerMsg::CreateConnector(tx, create))
            .await?;
        rx.recv().await?
    }

    async fn link(
        &self,
        _system: &World,
        _id: &TremorUrl,
        _mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        todo!()
    }

    async fn unlink(
        &self,
        _system: &World,
        _id: &TremorUrl,
        _mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        todo!()
    }
}

impl Binding {
    const LINKING_ERROR: &'static str = "links require the form of onramp -> pipeline or pipeline -> offramp or pipeline -> pipeline or pipeline -> onramp or offramp -> pipeline";
}

#[async_trait]
impl Artefact for Binding {
    type SpawnResult = Self;
    type LinkResult = Self;
    type LinkLHS = String;
    type LinkRHS = String;

    fn resource_type() -> url::ResourceType {
        url::ResourceType::Binding
    }

    async fn spawn(&self, _: &World, _: ServantId) -> Result<Self::SpawnResult> {
        //TODO: Validate
        Ok(self.clone())
    }

    async fn link(
        &self,
        system: &World,
        id: &TremorUrl,
        mappings: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<Self::LinkResult> {
        use ResourceType::{Offramp, Onramp, Pipeline};
        let mut pipelines: Vec<(TremorUrl, TremorUrl)> = Vec::new(); // pipeline -> {onramp, offramp, pipeline}
        let mut onramps: Vec<(TremorUrl, TremorUrl)> = Vec::new(); // onramp -> pipeline
        let mut offramps: Vec<(TremorUrl, TremorUrl)> = Vec::new(); // linked offramps -> pipeline
        let mut res = self.clone();
        res.binding.links.clear();
        for (src, dsts) in self.binding.links.clone() {
            // TODO: It should be validated ahead of time that every mapping has an instance!
            // * is a port
            // *  is a combination of on and offramp
            if let Some(inst) = src.instance() {
                let mut instance = String::new();
                for (map_name, map_replace) in &mappings {
                    instance = inst.replace(&format!("%7B{}%7D", map_name), &map_replace);
                }
                let mut from = src.clone();
                from.set_instance(&instance);
                let mut tos: Vec<TremorUrl> = Vec::new();
                for dst in dsts {
                    // TODO: we should be able to replace any part of the tremor url with mapping values, not just the instance
                    // TODO: It should be validated ahead of time that every mapping has an instance!
                    if let Some(inst) = dst.instance() {
                        let mut instance = String::new();

                        // This is because it is an URL and we have to use escape codes
                        for (map_name, map_replace) in &mappings {
                            instance = inst.replace(&format!("%7B{}%7D", map_name), &map_replace);
                        }
                        let mut to = dst.clone();
                        to.set_instance(&instance);
                        tos.push(to.clone());
                        match (from.resource_type(), to.resource_type()) {
                            (Some(Onramp), Some(Pipeline)) => {
                                onramps.push((from.clone(), to));
                            }
                            (Some(Pipeline), Some(Offramp | Pipeline | Onramp)) => {
                                pipelines.push((from.clone(), to))
                            }
                            // for linked offramps
                            // TODO improve this process: this should really be treated as onramps,
                            // or as a separate resource
                            (Some(Offramp), Some(Pipeline)) => offramps.push((from.clone(), to)),
                            (_, _) => return Err(Self::LINKING_ERROR.into()),
                        };
                    }
                }
                res.binding.links.insert(from, tos);
            }
        }

        // first start the linked offramps
        // so they
        for (from, to) in offramps {
            system.ensure_pipeline(&to).await?;
            system.ensure_offramp(&from).await?;
            system
                .link_offramp(&from, vec![(to, from.clone())].into_iter().collect())
                .await?;
        }

        for (from, to) in pipelines {
            info!("Binding {} to {}", from, to);
            match to.resource_type() {
                Some(Offramp) => system.ensure_offramp(&to).await?,
                Some(Pipeline) => system.ensure_pipeline(&to).await?,
                Some(Onramp) => system.ensure_onramp(&to).await?,
                _ => (),
            };
            system.ensure_pipeline(&from).await?;
            system
                .link_pipeline(
                    &from,
                    vec![(from.instance_port_required()?.to_string(), to.clone())]
                        .into_iter()
                        .collect(),
                )
                .await?;
            match to.resource_type() {
                Some(Offramp) => {
                    system
                        .link_offramp(&to, vec![(from, to.clone())].into_iter().collect())
                        .await?;
                }
                Some(Pipeline) => {
                    // notify the pipeline we connect to that a pipeline has been connected to its 'in' port
                    warn!("Linking pipelines is highly experimental! You are on your own, watch your steps!");
                    // we do the reverse linking from within the pipeline
                }
                _ => (),
            }
        }

        for (from, to) in onramps {
            system.ensure_pipeline(&to).await?;
            system.ensure_onramp(&from).await?;
            system
                .link_onramp(
                    &from,
                    vec![(from.instance_port_required()?.to_string(), to)]
                        .into_iter()
                        .collect(),
                )
                .await?;
        }

        res.mapping = Some(vec![(id.clone(), mappings)].into_iter().collect());
        Ok(res)
    }

    async fn unlink(
        &self,
        system: &World,
        _: &TremorUrl,
        _: HashMap<Self::LinkLHS, Self::LinkRHS>,
    ) -> Result<bool> {
        // TODO Quiescence Protocol ( termination correctness checks )
        //
        // We should ensure any in-flight events in a pipeline are flushed
        // to completion before unlinking *OR* unlink should should block/wait
        // until the pipeline reaches quiescence before returning
        //
        // For now, we let this hang wet - May require an FSM
        //
        // For example, once shutdown has been signalled via on_signal
        // we should follow through with a Quiesce signal, when all outputs
        // have signalled Quiesce we are guaranteed ( ordering ) that the Quiesce
        // signal has propagated through all branches in a pipeline. At this point
        // we can latch a quiescence condition in the pipeline which unlink or other
        // post-quiescence cleanup logic can hook off / block on etc...
        //
        info!("Unlinking Binding {}", self.binding.id);

        for (from, tos) in &self.binding.links {
            if let Some(ResourceType::Onramp) = from.resource_type() {
                let mut mappings = HashMap::new();
                for p in tos {
                    mappings.insert(from.instance_port_required()?.to_string(), p.clone());
                }
                system.unlink_onramp(&from, mappings).await?;
            }
        }
        // keep track of already handled pipelines, so we don't unlink twice and run into errors
        let mut unlinked = HashSet::with_capacity(self.binding.links.len());
        for (from, tos) in &self.binding.links {
            let mut from_instance = from.clone();
            from_instance.trim_to_instance();

            if let Some(ResourceType::Pipeline) = from.resource_type() {
                if !unlinked.contains(&from_instance) {
                    for to in tos {
                        let mut mappings = HashMap::new();
                        mappings.insert(from.instance_port_required()?.to_string(), to.clone());
                        system.unlink_pipeline(&from, mappings).await?;
                        if let Some(ResourceType::Offramp) = to.resource_type() {
                            let mut mappings = HashMap::new();
                            mappings.insert(to.clone(), from.clone());
                            system.unlink_offramp(&to, mappings).await?;
                        }
                    }
                    unlinked.insert(from_instance);
                }
            }
        }
        for (from, tos) in &self.binding.links {
            if let Some(ResourceType::Offramp) = from.resource_type() {
                let mut mappings = HashMap::new();
                for to in tos {
                    mappings.insert(from.clone(), to.clone());
                }
                system.unlink_offramp(from, mappings).await?;
            }
        }

        info!("Binding {} unlinked.", self.binding.id);
        Ok(true)
    }
}
