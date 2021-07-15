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

// The repository workflow is tested through EQC as well as the cli tests,
// neither of them generate coverage :(
#![cfg(not(tarpaulin_include))]

mod artefact;

use crate::errors::{ErrorKind, Result};
use crate::url::TremorUrl;
use async_channel::bounded;
use async_std::task;
use hashbrown::{hash_map::Entry, HashMap};
use std::default::Default;
use std::fmt;

/// A Servant ID
pub use crate::registry::ServantId;
/// A binding artefact
pub use artefact::Binding as BindingArtefact;
pub(crate) use artefact::OfframpArtefact;
pub(crate) use artefact::OnrampArtefact;
pub(crate) use artefact::ConnectorArtefact;
/// A pipeline artefact
pub use artefact::Pipeline as PipelineArtefact;
pub(crate) use artefact::{Artefact, Id as ArtefactId};

/// Wrapper around a repository
#[derive(Serialize, Clone, Debug)]
pub struct RepoWrapper<A: Artefact> {
    /// The artefact
    pub artefact: A,
    /// Instances of this artefact
    pub instances: Vec<ServantId>,
    /// If this is a protected system artefact
    pub system: bool,
}

/// Repository for artefacts
#[derive(Default, Debug)]
pub(crate) struct Repository<A: Artefact> {
    map: HashMap<ArtefactId, RepoWrapper<A>>,
}

impl<A: Artefact> Repository<A> {
    // Retrives the wraped artefacts
    fn values(&self) -> Vec<A> {
        self.map
            .values()
            .filter_map(|a| {
                if a.system {
                    None
                } else {
                    Some(a.artefact.clone())
                }
            })
            .collect()
    }
    /// New repository
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    /// Retreives the artifact Id's
    fn keys(&self) -> Vec<ArtefactId> {
        self.map.keys().cloned().collect()
    }
    /// Finds an artefact by ID
    pub fn find(&self, mut id: ArtefactId) -> Option<&RepoWrapper<A>> {
        id.trim_to_artefact();
        self.map.get(&id)
    }

    /// Publishes an artefact
    pub fn publish(&mut self, mut id: ArtefactId, system: bool, artefact: A) -> Result<&A> {
        id.trim_to_artefact();
        match self.map.entry(id.clone()) {
            Entry::Occupied(_) => Err(ErrorKind::PublishFailedAlreadyExists(id.to_string()).into()),
            Entry::Vacant(e) => Ok(&e
                .insert(RepoWrapper {
                    instances: Vec::new(),
                    artefact,
                    system,
                })
                .artefact),
        }
    }
    /// Unpublishes an artefact
    pub fn unpublish(&mut self, mut id: ArtefactId) -> Result<A> {
        id.trim_to_artefact();
        match self.map.entry(id.clone()) {
            Entry::Vacant(_) => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
            Entry::Occupied(e) => {
                let wrapper = e.get();
                if wrapper.system {
                    Err(ErrorKind::UnpublishFailedSystemArtefact(id.to_string()).into())
                } else if wrapper.instances.is_empty() {
                    let (_, w) = e.remove_entry();
                    Ok(w.artefact)
                } else {
                    Err(ErrorKind::UnpublishFailedNonZeroInstances(id.to_string()).into())
                }
            }
        }
    }

    /// Binds an artefact to a given servant
    pub fn bind(&mut self, mut id: ArtefactId, mut sid: ServantId) -> Result<&A> {
        id.trim_to_artefact();
        sid.trim_to_instance();
        match self.map.get_mut(&id) {
            Some(w) => {
                w.instances.push(sid);
                Ok(&w.artefact)
            }
            None => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
        }
    }
    /// Unbinds an artefact with a given servant
    pub fn unbind(&mut self, mut id: ArtefactId, mut sid: ServantId) -> Result<&A> {
        id.trim_to_artefact();
        sid.trim_to_instance();
        match self.map.get_mut(&id) {
            Some(w) => {
                w.instances.retain(|x| x != &sid);
                Ok(&w.artefact)
            }
            None => Err(ErrorKind::ArtefactNotFound(id.to_string()).into()),
        }
    }
}

pub(crate) enum Msg<A: Artefact> {
    ListArtefacts(async_channel::Sender<Vec<ArtefactId>>),
    SerializeArtefacts(async_channel::Sender<Vec<A>>),
    FindArtefact(
        async_channel::Sender<Result<Option<RepoWrapper<A>>>>,
        ArtefactId,
    ),
    PublishArtefact(async_channel::Sender<Result<A>>, ArtefactId, bool, A),
    UnpublishArtefact(async_channel::Sender<Result<A>>, ArtefactId),
    RegisterInstance(async_channel::Sender<Result<A>>, ArtefactId, ServantId),
    UnregisterInstance(async_channel::Sender<Result<A>>, ArtefactId, ServantId),
}
impl<A: Artefact + Send + Sync + 'static> Repository<A> {
    fn start(mut self) -> async_channel::Sender<Msg<A>> {
        let (tx, rx) = bounded(crate::QSIZE);

        task::spawn::<_, Result<()>>(async move {
            while let Ok(msg) = rx.recv().await {
                match msg {
                    Msg::ListArtefacts(r) => r.send(self.keys()).await?,
                    Msg::SerializeArtefacts(r) => r.send(self.values()).await?,
                    Msg::FindArtefact(r, id) => {
                        r.send(A::artefact_id(&id).map(|id| self.find(id).cloned()))
                            .await?
                    }
                    Msg::PublishArtefact(r, id, sys, a) => {
                        r.send(
                            A::artefact_id(&id).and_then(|id| {
                                self.publish(id, sys, a).map(std::clone::Clone::clone)
                            }),
                        )
                        .await?
                    }
                    Msg::UnpublishArtefact(r, id) => {
                        r.send(A::artefact_id(&id).and_then(|id| self.unpublish(id)))
                            .await?
                    }
                    Msg::RegisterInstance(r, a_id, s_id) => {
                        r.send(
                            A::artefact_id(&a_id)
                                .and_then(|aid| Ok((aid, A::servant_id(&s_id)?)))
                                .and_then(|(aid, sid)| {
                                    self.bind(aid, sid).map(std::clone::Clone::clone)
                                }),
                        )
                        .await?
                    }
                    Msg::UnregisterInstance(r, a_id, s_id) => {
                        r.send(
                            A::artefact_id(&a_id)
                                .and_then(|a_id| Ok((a_id, A::servant_id(&s_id)?)))
                                .and_then(|(a_id, s_id)| {
                                    self.unbind(a_id, s_id).map(std::clone::Clone::clone)
                                }),
                        )
                        .await?
                    }
                }
            }
            Ok(())
        });
        tx
    }
}

/// Repositories
#[derive(Clone)]
pub struct Repositories {
    pipeline: async_channel::Sender<Msg<PipelineArtefact>>,
    onramp: async_channel::Sender<Msg<OnrampArtefact>>,
    offramp: async_channel::Sender<Msg<OfframpArtefact>>,
    binding: async_channel::Sender<Msg<BindingArtefact>>,
}

#[cfg(not(tarpaulin_include))]
impl fmt::Debug for Repositories {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Repositories {{ ... }}")
    }
}

#[cfg(not(tarpaulin_include))]
impl Default for Repositories {
    fn default() -> Self {
        Self::new()
    }
}

impl Repositories {
    /// Creates an empty repository
    #[must_use]
    pub fn new() -> Self {
        Self {
            pipeline: Repository::new().start(),
            onramp: Repository::new().start(),
            offramp: Repository::new().start(),
            binding: Repository::new().start(),
        }
    }

    /// List the pipelines
    ///
    /// # Errors
    ///  * if we can't list the pipelines
    pub async fn list_pipelines(&self) -> Result<Vec<ArtefactId>> {
        let (tx, rx) = bounded(1);
        self.pipeline.send(Msg::ListArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Serialises the pipelines
    ///
    /// # Errors
    ///  * if we can't serialize a pipeline
    pub async fn serialize_pipelines(&self) -> Result<Vec<PipelineArtefact>> {
        let (tx, rx) = bounded(1);
        self.pipeline.send(Msg::SerializeArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Find a pipeline
    ///
    /// # Errors
    ///  * if we can't find a pipeline
    pub async fn find_pipeline(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<RepoWrapper<PipelineArtefact>>> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::FindArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Publish a pipeline
    ///
    /// # Errors
    ///  * if we can't publish a pipeline
    pub async fn publish_pipeline(
        &self,
        id: &TremorUrl,
        system: bool,
        artefact: PipelineArtefact,
    ) -> Result<PipelineArtefact> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await?;
        rx.recv().await?
    }

    /// Unpublish a pipeline
    ///
    /// # Errors
    ///  * if we can't unpublish a pipeline
    pub async fn unpublish_pipeline(&self, id: &TremorUrl) -> Result<PipelineArtefact> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Bind a pipeline
    ///
    /// # Errors
    ///  * if we can't bind a pipeline
    pub async fn bind_pipeline(&self, id: &TremorUrl) -> Result<PipelineArtefact> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Unbinds a pipeline
    ///
    /// # Errors
    ///  * if we can't unbound a pipeline
    pub async fn unbind_pipeline(&self, id: &TremorUrl) -> Result<PipelineArtefact> {
        let (tx, rx) = bounded(1);
        self.pipeline
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// List onramps
    ///
    /// # Errors
    ///  * if we can't list onramps
    pub async fn list_onramps(&self) -> Result<Vec<ArtefactId>> {
        let (tx, rx) = bounded(1);
        self.onramp.send(Msg::ListArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// serializes onramps
    ///
    /// # Errors
    ///  * if we can't serialize onramp
    pub async fn serialize_onramps(&self) -> Result<Vec<OnrampArtefact>> {
        let (tx, rx) = bounded(1);
        self.onramp.send(Msg::SerializeArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// find an onramp
    ///
    /// # Errors
    ///  * if we can't find an onramp
    pub async fn find_onramp(&self, id: &TremorUrl) -> Result<Option<RepoWrapper<OnrampArtefact>>> {
        let (tx, rx) = bounded(1);
        self.onramp.send(Msg::FindArtefact(tx, id.clone())).await?;
        rx.recv().await?
    }

    /// Publish onramp
    ///
    /// # Errors
    ///  * if we can't publish the onramp
    pub async fn publish_onramp(
        &self,
        id: &TremorUrl,
        system: bool,
        artefact: OnrampArtefact,
    ) -> Result<OnrampArtefact> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await?;
        rx.recv().await?
    }

    /// Unpublish an onramp
    ///
    /// # Errors
    ///  * if we can't unpublish the onramp
    pub async fn unpublish_onramp(&self, id: &TremorUrl) -> Result<OnrampArtefact> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Binds an onramp
    ///
    /// # Errors
    ///  * if we can't bind the onrampo
    pub async fn bind_onramp(&self, id: &TremorUrl) -> Result<OnrampArtefact> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Unbinds an onramp
    ///
    /// # Errors
    ///  * if we can't unbind the onramp
    pub async fn unbind_onramp(&self, id: &TremorUrl) -> Result<OnrampArtefact> {
        let (tx, rx) = bounded(1);
        self.onramp
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// List offramps
    ///
    /// # Errors
    ///  * if we can't list offramp
    pub async fn list_offramps(&self) -> Result<Vec<ArtefactId>> {
        let (tx, rx) = bounded(1);
        self.offramp.send(Msg::ListArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Serialises offramps
    ///
    /// # Errors
    ///  * if we cna't serialize a offramp
    pub async fn serialize_offramps(&self) -> Result<Vec<OfframpArtefact>> {
        let (tx, rx) = bounded(1);
        self.offramp.send(Msg::SerializeArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Find an offramp
    ///
    /// # Errors
    ///  * if we can't find an offramp
    pub async fn find_offramp(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<RepoWrapper<OfframpArtefact>>> {
        let (tx, rx) = bounded(1);
        self.offramp.send(Msg::FindArtefact(tx, id.clone())).await?;
        rx.recv().await?
    }

    /// Publishes an offramp
    ///
    /// # Errors
    ///  * if we can't publish a offramp
    pub async fn publish_offramp(
        &self,
        id: &TremorUrl,
        system: bool,
        artefact: OfframpArtefact,
    ) -> Result<OfframpArtefact> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await?;
        rx.recv().await?
    }

    /// Unpublishes an offramp
    ///
    /// # Errors
    ///  * if we can't unpublish an onramp
    pub async fn unpublish_offramp(&self, id: &TremorUrl) -> Result<OfframpArtefact> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Binds an offramp
    ///
    /// # Errors
    ///  * if we can't bind an oframp
    pub async fn bind_offramp(&self, id: &TremorUrl) -> Result<OfframpArtefact> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Unbinds an offramp
    ///
    /// # Errors
    ///  * if we can't unbind the offramp
    pub async fn unbind_offramp(&self, id: &TremorUrl) -> Result<OfframpArtefact> {
        let (tx, rx) = bounded(1);
        self.offramp
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Lists bindings
    ///
    /// # Errors
    ///  * if we can't list all bindings
    pub async fn list_bindings(&self) -> Result<Vec<ArtefactId>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::ListArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Serialises bindings
    ///
    /// # Errors
    ///  * if we can't serialize the binding
    pub async fn serialize_bindings(&self) -> Result<Vec<BindingArtefact>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::SerializeArtefacts(tx)).await?;
        Ok(rx.recv().await?)
    }

    /// Find a binding
    ///
    /// # Errors
    ///  * if finding the binding failed
    pub async fn find_binding(
        &self,
        id: &TremorUrl,
    ) -> Result<Option<RepoWrapper<BindingArtefact>>> {
        let (tx, rx) = bounded(1);
        self.binding.send(Msg::FindArtefact(tx, id.clone())).await?;
        rx.recv().await?
    }

    /// Publish a binding
    ///
    /// # Errors
    ///  * if we can't publish the binding
    pub async fn publish_binding(
        &self,
        id: &TremorUrl,
        system: bool,
        artefact: BindingArtefact,
    ) -> Result<BindingArtefact> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::PublishArtefact(tx, id.clone(), system, artefact))
            .await?;
        rx.recv().await?
    }

    /// Unpublishes a binding
    ///
    /// # Errors
    ///  * if we can't unpublish the binding
    pub async fn unpublish_binding(&self, id: &TremorUrl) -> Result<BindingArtefact> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::UnpublishArtefact(tx, id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Binds a binding
    ///
    /// # Errors
    ///  * if we can't bind the binding
    pub async fn bind_binding(&self, id: &TremorUrl) -> Result<BindingArtefact> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::RegisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }

    /// Unbinds a binding
    ///
    /// # Errors
    ///  * if we can't unbound the binding
    pub async fn unbind_binding(&self, id: &TremorUrl) -> Result<BindingArtefact> {
        let (tx, rx) = bounded(1);
        self.binding
            .send(Msg::UnregisterInstance(tx, id.clone(), id.clone()))
            .await?;
        rx.recv().await?
    }
}
