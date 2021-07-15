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

use port_scanner::scan_port_addr;

use crate::sink::prelude::*;

/// create an `ack` insight from the given event
/// The insight is reporting successful handling of the given event to upstream components (pipelines, operators, onramps, connectors).
pub fn ack(event: &mut Event) -> sink::Reply {
    sink::Reply::Insight(event.insight_ack())
}

/// create a `fail` insight from the given event
/// The insight is reporting failed handling of the given event to upstream components (pipelines, operators, onramps, connectors).
pub fn fail(event: &mut Event) -> sink::Reply {
    sink::Reply::Insight(event.insight_fail())
}

/// create a CB trigger/close insight for the given event
/// The insight is reporting that the downstream components should not receive any further events.
/// The circuit breaker for the following downstream components is _triggered_
pub fn close(event: &mut Event) -> sink::Reply {
    sink::Reply::Insight(event.insight_trigger())
}

/// create a CB restore/open insight for the given event
/// The insight is reporting that the downstream components can now again receive events.
/// The circuit breaker for the following downstream components is _restored_
pub fn open(event: &mut Event) -> sink::Reply {
    sink::Reply::Insight(event.insight_restore())
}

/// Canary probe
pub trait CanaryProbe: Send + Sync {
    /// Executes a canary probe returning:
    /// * true - if the remote endpoint is accessible
    /// * false - if the remote endpoint is not contactable or accessible
    fn chirp(&self) -> bool;

    /// A short text description of the endpoint / canary
    fn about(&self) -> String;
}

/// Canary probe for scanning a given `endpoint` for reachability
pub struct HostPortScanCanaryProbe {
    endpoint: String,
}

impl dyn CanaryProbe {
    /// Constructs a canary probe that is capable of `host:port` endpoint
    /// testing via a simple port address scan
    //
    fn with_host_port(endpoint: String) -> Box<HostPortScanCanaryProbe> {
        Box::new(HostPortScanCanaryProbe { endpoint })
    }
}

impl CanaryProbe for HostPortScanCanaryProbe {
    fn chirp(&self) -> bool {
        info!("Canary probe: {}", self.endpoint);
        scan_port_addr(&self.endpoint)
    }

    fn about(&self) -> String {
        format!("{} [port scan]", &self.endpoint)
    }
}

// struct BadCanary {}

// impl CanaryProbe for BadCanary {
//     fn chirp(&self) -> bool {
//         false
//     }

//     fn about(&self) -> String {
//         "<unknown> [bad canary]".into()
//     }
// }

/// Failure detector
pub trait FailureDetector: Send + Sync {
    /// check if the remote endpoint is healthy or exhibits a failure
    fn chirp(&self) -> bool;

    /// Register failure of the remote endpoint
    fn trigger(&mut self);

    /// Periodic canary probe hook
    fn probe(&mut self);

    /// Register recovery of the remote endpoint
    fn restore(&mut self);

    /// Is the current disposition indicative that we are `down`
    fn is_down(&self) -> bool;
}

impl dyn FailureDetector {
    /// creates a `FailureDetector` for a given GRPC endpoint
    #[allow(dead_code)]
    pub fn for_grpc_endpoint(hostport: String) -> GrpcEndpointSupervisor {
        GrpcEndpointSupervisor {
            canary: <dyn CanaryProbe>::with_host_port(hostport),
            is_down: false,
        }
    }
}

// struct BadSupervisor {}

// impl FailureDetector for BadSupervisor {
//     fn chirp(&self) -> bool {
//         false
//     }

//     fn trigger(&mut self) {
//         // Do nothing - we're bad
//     }

//     fn probe(&mut self) {
//         // Do nothing - we're lazy
//     }

//     fn restore(&mut self) {
//         // Do nothing - supervisor says no!
//     }

//     fn is_down(&self) -> bool {
//         false // We are always open for business
//     }
// }

/// Supervisor for a GRPC endpoint
pub struct GrpcEndpointSupervisor {
    canary: Box<dyn CanaryProbe>,
    is_down: bool,
}

impl FailureDetector for GrpcEndpointSupervisor {
    fn chirp(&self) -> bool {
        self.canary.chirp()
    }

    fn trigger(&mut self) {
        self.is_down = true;
        info!("Grpc Endpoint {} has failed", self.canary.about())
    }

    fn probe(&mut self) {
        if self.canary.chirp() {
            self.is_down = false;
            info!("Grpc Endpoint {} has recovered", self.canary.about())
        }
    }

    fn restore(&mut self) {
        self.is_down = false;
        info!("Grpc Endpoint {} force recovered", self.canary.about())
    }

    fn is_down(&self) -> bool {
        self.is_down
    }
}

/// QoS checker for sinks
pub trait SinkQoS: Send + Sync {
    /// Is this quality of service auto acknowledging?
    fn is_auto_acknowledging(&self) -> bool;

    /// Is this sink currently `down`
    fn is_down(&self) -> bool;

    /// Is this sink currently `up`
    fn is_up(&self) -> bool;

    /// Probe availability
    fn probe(&mut self, ingest_ns: u64) -> bool;
}
/// Gathering QoS Facilities
pub struct QoSFacilities {}
impl QoSFacilities {
    #[allow(dead_code)]
    fn best_effort() -> LossySinkQoS {
        LossySinkQoS {}
    }

    /// returns a `RecoverableSinkQoS` for the given endpoint
    #[allow(dead_code)]
    pub fn recoverable(endpoint: String) -> RecoverableSinkQoS {
        let canary: Box<dyn CanaryProbe> = Box::new(HostPortScanCanaryProbe { endpoint });
        let supervisor = Box::new(GrpcEndpointSupervisor {
            canary,
            is_down: false,
        });
        RecoverableSinkQoS {
            supervisor,
            interval: 1_000_000_000, // 1 sec
            epoch: tremor_common::time::nanotime(),
        }
    }
}

/// QoS implementation not providing any actial QoS
pub struct LossySinkQoS {}

impl SinkQoS for LossySinkQoS {
    /// Auto-acknowledge is enabled
    fn is_auto_acknowledging(&self) -> bool {
        true
    }

    fn probe(&mut self, _ingest_ns: u64) -> bool {
        true // Do nothing
    }

    fn is_down(&self) -> bool {
        false
    }

    fn is_up(&self) -> bool {
        !self.is_down()
    }
}

/// Recoverable Sink QoS
pub struct RecoverableSinkQoS {
    interval: u64,
    epoch: u64,
    supervisor: Box<dyn FailureDetector>,
}

impl SinkQoS for RecoverableSinkQoS {
    /// Auto-acknowledge is disabled
    fn is_auto_acknowledging(&self) -> bool {
        false
    }

    fn is_down(&self) -> bool {
        self.supervisor.is_down()
    }

    fn is_up(&self) -> bool {
        !self.supervisor.is_down()
    }

    fn probe(&mut self, ingest_ns: u64) -> bool {
        let now = nanotime();
        let check_now = (now - self.interval) > self.epoch;
        // if ingest_ns < now check_now will always be true
        self.epoch = ingest_ns + self.interval;
        if self.is_down() && check_now {
            self.supervisor.chirp()
        } else {
            self.is_up()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeCanary {
        is_reachable: bool,
    }

    impl FakeCanary {
        fn toggle(&mut self) {
            self.is_reachable = !self.is_reachable;
        }
    }

    #[cfg(not(tarpaulin_include))] // this is a test tool
    impl CanaryProbe for FakeCanary {
        fn chirp(&self) -> bool {
            self.is_reachable
        }

        fn about(&self) -> String {
            "test canary [fake]".into()
        }
    }

    struct FakeDetector {
        fake: FakeCanary,
    }

    #[cfg(not(tarpaulin_include))] // this is a test tool
    impl FailureDetector for FakeDetector {
        fn chirp(&self) -> bool {
            self.fake.is_reachable
        }

        fn trigger(&mut self) {
            // do nothing
        }

        fn probe(&mut self) {
            self.fake.toggle();
        }

        fn restore(&mut self) {
            // do nothing
        }

        fn is_down(&self) -> bool {
            !self.fake.is_reachable
        }
    }

    struct AlternatorQoS {
        fake: FakeDetector,
    }

    impl SinkQoS for AlternatorQoS {
        fn is_auto_acknowledging(&self) -> bool {
            false
        }

        fn is_down(&self) -> bool {
            self.fake.is_down()
        }

        fn is_up(&self) -> bool {
            !self.fake.is_down()
        }

        fn probe(&mut self, _ingest_ns: u64) -> bool {
            self.fake.fake.toggle();
            self.is_up()
        }
    }

    #[test]
    fn qos_lossy() {
        let qos: LossySinkQoS = QoSFacilities::best_effort();
        assert!(qos.is_auto_acknowledging());
        assert!(!qos.is_down());
        assert!(qos.is_up());
    }

    #[test]
    fn best_effort_endpoint_good() {
        let mut qos = QoSFacilities::recoverable("www.example.com:443".to_string());
        assert!(!qos.is_auto_acknowledging());
        assert!(!qos.is_down());
        assert!(qos.is_up());
        qos.probe(0);
        assert!(qos.is_up());
    }

    #[test]
    fn best_effort_endpoint_bad() {
        let mut qos = QoSFacilities::recoverable("www.snot.badger.com:443".to_string());
        assert!(!qos.is_auto_acknowledging());
        assert!(!qos.is_down());
        assert!(qos.is_up());
        qos.probe(0);
        assert!(qos.is_up());
    }

    #[test]
    fn test_up_down_fake() {
        let mut qos = AlternatorQoS {
            fake: FakeDetector {
                fake: FakeCanary {
                    is_reachable: false,
                },
            },
        };
        for i in 0..=100 {
            assert!(!qos.is_auto_acknowledging());
            if i & 1 == 0 {
                assert!(qos.is_down());
                assert!(!qos.is_up());
            } else {
                assert!(qos.is_up());
                assert!(!qos.is_down());
            }
            qos.probe(0);
        }
    }
}
