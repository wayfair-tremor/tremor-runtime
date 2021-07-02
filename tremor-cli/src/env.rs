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
use tremor_script::path::load as load_module_path;
use tremor_script::{path::ModulePath, registry, registry::Aggr, Registry};

/// Encapsulates state in a tremor runtime environment
pub(crate) struct TremorCliEnv {
    pub(crate) module_path: ModulePath,
    pub(crate) fun: Registry,
    pub(crate) aggr: Aggr,
}

/// Setup the tremor runtime environment
pub(crate) fn setup() -> Result<TremorCliEnv> {
    let module_path = load_module_path();
    let mut fun: Registry = registry::registry();
    let aggr = registry::aggr();

    // Install runtime extensions from a single source of truth
    tremor_runtime::functions::install(&mut fun)?;
    tremor_connectors::connectors::functions::install(&mut fun);

    Ok(TremorCliEnv {
        module_path,
        fun,
        aggr,
    })
}
