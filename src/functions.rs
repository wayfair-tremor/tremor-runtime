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
use crate::version::VERSION;
use tremor_pipeline::FN_REGISTRY;
use tremor_script::registry::Registry;
use tremor_script::tremor_fn;

/// Loads the function library
///
/// # Errors
///  * if we can't load the registry
pub fn load() -> Result<()> {
    let mut reg = FN_REGISTRY.lock()?;
    install(&mut reg)
}

/// Install's common functions into a registry
///
/// # Errors
///  * if we can't install extensions
pub fn install(reg: &mut Registry) -> Result<()> {
    crate::connectors::otel::load(reg);

    reg.insert(tremor_fn!(system|instance(_context) {
        Ok(Value::from(instance!()))
    }))
    .insert(tremor_fn!(system|version(_context) {
        Ok(Value::from(VERSION).into_static())
    }));

    Ok(())
}
