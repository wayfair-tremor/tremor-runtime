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

//NOTE: error_chain
#![allow(deprecated)]
#![allow(missing_docs)]
#![allow(clippy::large_enum_variant)]

use crate::async_sink;
use beef::Cow;
use error_chain::error_chain;

use hdrhistogram::{self, serialization as hdr_s};
use tremor_influx as influx;

impl From<sled::transaction::TransactionError<()>> for Error {
    fn from(e: sled::transaction::TransactionError<()>) -> Self {
        Self::from(format!("Sled Transaction Error: {:?}", e))
    }
}

impl From<hdr_s::DeserializeError> for Error {
    fn from(e: hdr_s::DeserializeError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<hdrhistogram::errors::CreationError> for Error {
    fn from(e: hdrhistogram::errors::CreationError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<hdrhistogram::RecordError> for Error {
    fn from(e: hdrhistogram::RecordError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<hdrhistogram::serialization::V2SerializeError> for Error {
    fn from(e: hdrhistogram::serialization::V2SerializeError) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<http_types::Error> for Error {
    fn from(e: http_types::Error) -> Self {
        Self::from(format!("{}", e))
    }
}

impl<F> From<rental::RentalError<F, Box<Vec<u8>>>> for Error {
    fn from(_e: rental::RentalError<F, Box<Vec<u8>>>) -> Self {
        Self::from("Rental Error".to_string())
    }
}

impl From<Box<dyn std::error::Error>> for Error {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<Box<dyn std::error::Error + Sync + Send>> for Error {
    fn from(e: Box<dyn std::error::Error + Sync + Send>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<glob::PatternError> for Error {
    fn from(e: glob::PatternError) -> Self {
        Self::from(format!("{}", e))
    }
}

impl<T> From<async_channel::SendError<T>> for Error {
    fn from(e: async_channel::SendError<T>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl<T> From<async_channel::TrySendError<T>> for Error {
    fn from(e: async_channel::TrySendError<T>) -> Self {
        Self::from(format!("{:?}", e))
    }
}

impl From<tremor_script::errors::CompilerError> for Error {
    fn from(e: tremor_script::errors::CompilerError) -> Self {
        e.error().into()
    }
}

impl<P> From<std::sync::PoisonError<P>> for Error {
    fn from(e: std::sync::PoisonError<P>) -> Self {
        Self::from(format!("Poison Error: {:?}", e))
    }
}

#[cfg(test)]
impl PartialEq for Error {
    fn eq(&self, _other: &Self) -> bool {
        // This might be Ok since we try to compare Result in tests
        false
    }
}

error_chain! {
    links {
        Script(tremor_script::errors::Error, tremor_script::errors::ErrorKind);
        Pipeline(tremor_pipeline::errors::Error, tremor_pipeline::errors::ErrorKind);
    }
    foreign_links {
        AddrParseError(std::net::AddrParseError);
        AsyncChannelRecvError(async_channel::RecvError);
        AsyncChannelTryRecvError(async_channel::TryRecvError);
        Base64Error(base64::DecodeError);
        ChannelReceiveError(std::sync::mpsc::RecvError);
        Common(tremor_common::Error);
        CronError(cron::error::Error);
        DateTimeParseError(chrono::ParseError);
        DnsError(async_std_resolver::ResolveError);
        ElasticError(elastic::Error);
        FromUtf8Error(std::string::FromUtf8Error);
        GoogleAuthError(gouth::Error);
        HttpHeaderError(http::header::InvalidHeaderValue);
        InfluxEncoderError(influx::EncoderError);
        Io(std::io::Error);
        JsonAccessError(value_trait::AccessError);
        JsonError(simd_json::Error);
        KafkaError(rdkafka::error::KafkaError);
        MsgPackDecoderError(rmp_serde::decode::Error);
        MsgPackEncoderError(rmp_serde::encode::Error);
        ParseIntError(std::num::ParseIntError);
        ParseFloatError(std::num::ParseFloatError);
        Postgres(postgres::Error);
        RegexError(regex::Error);
        ReqwestError(reqwest::Error);
        RustlsError(rustls::TLSError);
        Hex(hex::FromHexError);
        SinkDequeueError(async_sink::SinkDequeueError);
        SinkEnqueueError(async_sink::SinkEnqueueError);
        Sled(sled::Error);
        SnappyError(snap::Error);
        Timeout(async_std::future::TimeoutError);
        TonicStatusError(tonic::Status);
        TonicTransportError(tonic::transport::Error);
        TryFromIntError(std::num::TryFromIntError);
        UrlParserError(url::ParseError);
        Utf8Error(std::str::Utf8Error);
        ValueError(tremor_value::Error);
        WsError(async_tungstenite::tungstenite::Error);
        YamlError(serde_yaml::Error) #[doc = "Error during yaml parsing"];
    }

    errors {
        UnknownOp(n: String, o: String) {
            description("Unknown operator")
                display("Unknown operator: {}::{}", n, o)
        }
        UnknownOnrampType(t: String) {
            description("Unknown onramp type")
                display("Unknown onramp type {}", t)
        }
        UnknownOfframpType(t: String) {
            description("Unknown offramp type")
                display("Unknown offramp type {}", t)
        }
        UnknownConnector(n: String) {
            description("Unknown connector")
                display("Unknown connector {}", n)
        }
        UnknownConnectorType(t: String) {
            description("Unknown connector type")
                display("Unknown connector type {}", t)
        }
        ArtefactNotFound(id: String) {
            description("The artifact was not found")
                display("The artifact was not found: {}", id)
        }
        CodecNotFound(name: String) {
            description("Codec not found")
                display("The codec was not found: {}", name)
        }
        PublishFailedAlreadyExists(key: String) {
            description("The published artefact already exists")
                display("The published {} already exists.", key)
        }

        UnpublishFailedDoesNotExist(key: String) {
            description("The unpublished artefact does not exist")
                display("The unpublished {} does not exist.", key)
        }
        UnpublishFailedNonZeroInstances(key: String) {
            description("The artefact has non-zero instances and cannot be unpublished")
                display("Cannot unpublish artefact {} which has non-zero instances.", key)
        }
        UnpublishFailedSystemArtefact(key: String) {
            description("The artefact is a system artefact and cannot be unpublished")
                display("Cannot unpublish system artefact {}.", key)
        }

        BindFailedAlreadyExists(key: String) {
            description("The binding already exists")
                display("The binding with the id {} already exists.", key)
        }

        BindFailedKeyNotExists(key: String) {
            description("Failed to bind non existand artefact")
                display("Failed to bind non existand {}.", key)
        }

        BadOpConfig(e: String) {
            description("Operator config has a bad syntax")
                display("Operator config has a bad syntax: {}", e)
        }

        UnknownNamespace(n: String) {
            description("Unknown namespace")
                display("Unknown namespace: {}", n)
        }

        InvalidGelfHeader(len: usize, initial: Option<[u8; 2]>) {
            description("Invalid GELF header")
                display("Invalid GELF header len: {}, prefix: {:?}", len, initial)
        }

        InvalidStatsD {
            description("Invalid statsd metric")
                display("Invalid statsd metric")
        }


        UnknownSubPipeline(p: String) {
            description("Reference to unknown sub-pipeline")
                display("The sub-pipelines '{}' is not defined", p)
        }

        UnknownPipeline(p: String) {
            description("Reference to unknown pipeline")
                display("The pipelines '{}' is not defined", p)
        }

        PipelineStartError(p: String) {
            description("Failed to start pipeline")
                display("Failed to start pipeline '{}'", p)
        }

        OnrampError(i: u64) {
            description("Error in onramp")
                display("Error in onramp '{}'", i)
        }

        OnrampMissingPipeline(o: String) {
            description("Onramp is missing a pipeline")
                display("The onramp '{}' is missing a pipeline", o)
        }
        InvalidInfluxData(s: String, e: influx::DecoderError) {
            description("Invalid Influx Line Protocol data")
                display("Invalid Influx Line Protocol data: {}\n{}", e, s)
        }
        InvalidBInfluxData(s: String) {
            description("Invalid BInflux Line Protocol data")
                display("Invalid BInflux Line Protocol data: {}", s)
        }
        InvalidSyslogData(s: &'static str) {
            description("Invalid Syslog Protocol data")
                display("Invalid Syslog Protocol data: {}", s)
        }
        BadOutputid(i: usize) {
            description("Bad output pipeline id.")
                display("Bad output pipeline id {}", i - 1)
        }
        BadUtF8InString {
            description("Bad UTF8 in input string")

        }
        InvalidCompression {
            description("Data can't be decompressed")
                display("The data did not contain a known magic header to identify a supported compression")
        }
        NoSocket {
            description("No socket available")
                display("No socket available")
        }
        KvError(s: String) {
            description("KV error")
                display("{}", s)
        }
        TLSError(s: String) {
            description("TLS error")
                display("{}", s)
        }
        InvalidTremorUrl(s: String) {
            description("Invalid Tremor URL")
                display("{}", s)
        }
        MissingConfiguration(s: String) {
            description("Missing Configuration")
                display("Missing Configuration for {}", s)
        }
        InvalidConnect(target: String, port: Cow<'static, str>) {
            description("Invalid Connect attempt")
                display("Invalid Connect to {} via port {}", target, port)
        }
        InvalidDisconnect(target: String, entity: String, port: Cow<'static, str>) {
            description("Invalid Disonnect attempt")
                display("Invalid Disconnect of {} from {} via port {}", entity, target, port)
        }
        MaxRetriesExceeded(max_retries: u64) {
            description("Max reconnect retries exceeded")
                display("Max reconnect retries ({}) exceeded", max_retries)
        }
    }
}
