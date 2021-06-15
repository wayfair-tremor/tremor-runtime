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

// common id handling

/// we namespace onramp, offramp and operator ids differently in order to avoid clashes
const ONRAMP_ID_BASE: u64 =    0b0;
const OPERATOR_ID_BASE: u64 =  0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_u64;
const OFFRAMP_ID_BASE: u64 =   0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_u64;
const CONNECTOR_ID_BASE: u64 = 0b1000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_0000_u64;

#[derive(Debug)]
/// id generator
pub struct IdGen<const BASE: u64>(u64);

impl<const BASE: u64> IdGen<BASE> {
    #[must_use]
    /// constructor
    pub fn new() -> Self {
        Self(BASE)
    }
    /// return the next id for this generator
    pub fn next_id(&mut self) -> u64 {
        self.0 = self.0.checked_add(1).unwrap_or(BASE);
        self.0
    }
}

impl<const BASE: u64> Default for IdGen<BASE> {
    fn default() -> Self {
        Self::new()
    }
}

/// onramp id generator - generates consecutive u64 values
pub type OnrampIdGen = IdGen<ONRAMP_ID_BASE>;
/// offramp id generator - generates consecutive u64 values
pub type OfframpIdGen = IdGen<OFFRAMP_ID_BASE>;
/// operator id generator - generates consecutive u64 values
/// this one will be shared by all pipelines - so we ensure unique operator ids
pub type OperatorIdGen = IdGen<OPERATOR_ID_BASE>;
/// connector id generator - generates consecutive u64 values
pub type ConnectorIdGen = IdGen<CONNECTOR_ID_BASE>;



#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BASE: u64 = 42;
    #[test]
    fn id_gen() {
        let mut idgen = IdGen::<TEST_BASE>::default();
        let ids: Vec<u64> = std::iter::repeat_with(|| idgen.next_id())
            .take(100)
            .collect();

        for window in ids.windows(2) {
            match window {
                &[l, h] => {
                    assert!(l > TEST_BASE);
                    assert!(h > TEST_BASE);
                    assert!(l < h); // strictly monotonically increasing
                }
                _ => assert!(false, "invalid window"),
            }
        }
    }
}
