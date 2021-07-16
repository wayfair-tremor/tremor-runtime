// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache Lic ense, Version 2.0 (the "License");
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

use rdkafka::util::get_rdkafka_version;
/// Version of the tremor crate;
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(not(debug_assertions))]
/// Checks if a we are in a debug build
pub const DEBUG: bool = false;
#[cfg(debug_assertions)]
/// Checks if a we are in a debug build
pub const DEBUG: bool = true;

#[must_use]
/// Provides formatting for "long" version name of build
pub fn long_ver() -> String {
    #[cfg(not(debug_assertions))]
    const VERSION_LONG: &str = env!("CARGO_PKG_VERSION");
    #[cfg(debug_assertions)]
    const VERSION_LONG: &str = concat!(env!("CARGO_PKG_VERSION"), " (DEBUG)");
    return match option_env!("VERSION_BRANCH") {
        Some("main") | None => VERSION_LONG.to_string(),
        Some(branch) => {
            // provide additional version info
            let commit_hash: &str = option_env!("VERSION_HASH").map_or("", |hash| hash);
            format!("{} {}:{}", VERSION_LONG, branch, commit_hash)
        }
    };
}

/// Prints tremor and librdkafka version.
pub fn print() {
    eprintln!("tremor version: {}", long_ver().as_str());
    eprintln!("tremor instance: {}", instance!());
    let (version_n, version_s) = get_rdkafka_version();
    eprintln!("rd_kafka version: 0x{:08x}, {}", version_n, version_s);
}

/// Logs tremor and librdkafka version.
pub fn log() {
    info!("tremor version: {}", long_ver().as_str());
    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka version: 0x{:08x}, {}", version_n, version_s);
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn for_coverage_only() {
        print();
        log();
    }
}
