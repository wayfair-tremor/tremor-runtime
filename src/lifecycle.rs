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
use crate::registry::ServantId;
use crate::repository::Artefact;
use crate::system::World;
use std::fmt;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum ActivationState {
    Deactivated,
    Activated,
    Zombie,
}

#[derive(Clone)]
pub struct ActivatorLifecycleFsm<A: Artefact> {
    pub artefact: A,
    world: World,
    pub state: ActivationState,
    pub resolution: Option<A::SpawnResult>,
    id: ServantId,
}

impl<A: Artefact> fmt::Debug for ActivatorLifecycleFsm<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LifecycleFsm {{ id: {}, state: {:?} }}",
            self.id, self.state
        )
    }
}

impl<A: Artefact> ActivatorLifecycleFsm<A> {
    pub async fn new(world: World, artefact: A, id: ServantId) -> Result<Self> {
        let mut fresh = Self {
            artefact,
            world,
            state: ActivationState::Deactivated,
            resolution: None,
            id,
        };
        let resoluion = fresh.on_spawn().await?; // Initial transition
        fresh.resolution = Some(resoluion);
        Ok(fresh)
    }

    async fn on_spawn(&self) -> Result<A::SpawnResult> {
        self.artefact.spawn(&self.world, self.id.clone()).await
    }

    fn on_activate(&self) {
        // ALLOW: this is for clippy
        let _clippy = self;
        debug!(
            "Lifecycle on_activate not implemented state is now: {:?}",
            ActivationState::Activated
        );
    }

    fn on_passivate(&self) {
        // ALLOW: this is for clippy
        let _clippy = self;
        debug!("Lifecycle on_passivate not implemented");
    }

    fn on_destroy(&self) {
        // ALLOW: this is for clippy
        let _clippy = self;
        debug!(
            "Lifecycle on_destroy not implemented state is now:: {:?}",
            ActivationState::Zombie
        );
    }
}

impl<A: Artefact> ActivatorLifecycleFsm<A> {
    pub fn transition(&mut self, to: ActivationState) -> Result<&mut Self> {
        loop {
            match (&self.state, &to) {
                (ActivationState::Deactivated, ActivationState::Activated) => {
                    self.state = ActivationState::Activated;
                    self.on_activate();
                    break;
                }
                (ActivationState::Deactivated, ActivationState::Zombie) => {
                    self.state = ActivationState::Zombie;
                    self.on_destroy();
                    break;
                }
                (ActivationState::Activated, ActivationState::Zombie) => {
                    self.state = ActivationState::Deactivated;
                    self.on_passivate();
                    continue; // NOTE by composition Active -> Deactive, Deactive -> Zombie
                }
                (ActivationState::Zombie, _) => break,
                _ => {
                    // NOTE Default transition is 'do nothing'
                    //    TODO dev mode -> defaults to panic ( not yet implemented )
                    //    TODO prod mode -> defaults to loopback / noop / do nothing
                    //    TODO convenience macro loopback!
                    return Err("Illegel State Transition".into());
                }
            };
        }

        Ok(self)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::config;
    use crate::repository::BindingArtefact;
    use crate::system::World;
    use crate::url::TremorUrl;
    use std::io::BufReader;
    use tremor_common::file as cfile;

    fn slurp(file: &str) -> config::Config {
        let file = cfile::open(file).expect("could not open file");
        let buffered_reader = BufReader::new(file);
        serde_yaml::from_reader(buffered_reader).expect("failed to parse file")
    }

    #[async_std::test]
    async fn onramp_activation_lifecycle() {
        let (world, _) = World::start(10, None).await.expect("failed to start world");

        let mut config = slurp("tests/configs/ut.passthrough.yaml");
        let artefact = config.onramp.pop().expect("artefact not found");
        let id = TremorUrl::parse("/onramp/blaster/00").expect("artefact not found");
        assert!(world
            .repo
            .find_onramp(&id)
            .await
            .expect("failed to communicate to repository")
            .is_none());

        assert!(world
            .repo
            .publish_onramp(&id, false, artefact)
            .await
            .is_ok());

        // Legal <initial> -> Deactivated
        assert_eq!(
            Ok(ActivationState::Deactivated),
            world.bind_onramp(&id).await,
        );

        // Legal Deactivated -> Activated
        assert_eq!(
            Ok(ActivationState::Activated),
            world
                .reg
                .transition_onramp(&id, ActivationState::Activated)
                .await
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_onramp(&id, ActivationState::Zombie)
                .await
        );

        // Zombies don't return from the dead
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_onramp(&id, ActivationState::Deactivated)
                .await
        );

        // Zombies don't return from the deady
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_onramp(&id, ActivationState::Activated)
                .await
        );
    }

    #[async_std::test]
    async fn offramp_activation_lifecycle() {
        let (world, _) = World::start(10, None).await.expect("failed to start world");

        let mut config = slurp("tests/configs/ut.passthrough.yaml");
        let artefact = config.offramp.pop().expect("artefact not found");
        let id = TremorUrl::parse("/offramp/test/out/00").expect("artefact not found");
        assert!(world
            .repo
            .find_offramp(&id)
            .await
            .expect("failed to communicate to repository")
            .is_none());

        assert!(world
            .repo
            .publish_offramp(&id, false, artefact)
            .await
            .is_ok());

        // Legal <initial> -> Deactivated
        assert_eq!(
            Ok(ActivationState::Deactivated),
            world.bind_offramp(&id).await
        );

        // Legal Deactivated -> Activated
        assert_eq!(
            Ok(ActivationState::Activated),
            world
                .reg
                .transition_offramp(&id, ActivationState::Activated)
                .await
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_offramp(&id, ActivationState::Zombie)
                .await
        );

        // Zombies don't return from the dead
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_offramp(&id, ActivationState::Deactivated)
                .await
        );

        // Zombies don't return from the deady
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_offramp(&id, ActivationState::Activated)
                .await
        );
    }

    #[async_std::test]
    async fn binding_activation_lifecycle() {
        let (world, _) = World::start(10, None).await.expect("failed to start world");

        let mut config = slurp("tests/configs/ut.passthrough.yaml");
        let artefact = BindingArtefact {
            binding: config.binding.pop().expect("artefact not found"),
            mapping: None,
        };
        let id = TremorUrl::parse("/binding/test/snot").expect("artefact not found");

        assert!(world
            .repo
            .find_binding(&id)
            .await
            .expect("failed to communicate to repository")
            .is_none());

        assert!(world
            .repo
            .publish_binding(&id, false, artefact.clone())
            .await
            .is_ok());

        assert!(world
            .reg
            .find_binding(&id)
            .await
            .expect("failed to communicate to registry")
            .is_none());

        // Legal <initial> -> Deactivated
        assert_eq!(
            Ok(ActivationState::Deactivated),
            world.bind_binding_a(&id, &artefact).await,
        );

        assert!(world
            .reg
            .find_binding(&id)
            .await
            .expect("failed to communicate to registry")
            .is_some());

        // Legal Deactivated -> Activated
        assert_eq!(
            Ok(ActivationState::Activated),
            world
                .reg
                .transition_binding(&id, ActivationState::Activated)
                .await
        );

        // Legal Activated -> Zombie ( via hidden transition trampoline )
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_binding(&id, ActivationState::Zombie)
                .await
        );

        // Zombies don't return from the dead
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_binding(&id, ActivationState::Deactivated)
                .await
        );

        // Zombies don't return from the deady
        assert_eq!(
            Ok(ActivationState::Zombie),
            world
                .reg
                .transition_binding(&id, ActivationState::Activated)
                .await
        );

        // TODO - full undeployment 'white-box' acceptance tests
        //        println!("TODO {:?}", world.repo.unpublish_binding(&id));
        let _r = world.unbind_binding_a(&id, &artefact).await;
        //        assert!(world.repo.unpublish_binding(&id).is_ok());
        println!(
            "TODO {:?}",
            world
                .reg
                .find_binding(&id)
                .await
                .expect("failed to communicate to registry")
        );
        assert!(world
            .reg
            .find_binding(&id)
            .await
            .expect("failed to communicate to registry")
            .is_none());
    }
}
