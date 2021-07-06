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

mod gelf;
pub(crate) use gelf::Gelf;

use crate::errors::{Error, Result};
use byteorder::{BigEndian, WriteBytesExt};
use std::default::Default;
use tremor_common::time::nanotime;
/// Set of Postprocessors
pub type Postprocessors = Vec<Box<dyn Postprocessor>>;
use std::io::Write;
use std::mem;
use std::str;

/// Postprocessor trait
pub trait Postprocessor: Send {
    /// Canonical name of the postprocessor
    fn name(&self) -> &str;
    /// process data
    ///
    /// # Errors
    ///
    ///   * Errors if the data could not be processed
    fn process(&mut self, ingres_ns: u64, egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;
}

/// Lookup a postprocessor via its unique id
/// # Errors
///
///   * Errors if the postprocessor is not known
#[cfg(not(tarpaulin_include))]
pub fn lookup(name: &str) -> Result<Box<dyn Postprocessor>> {
    match name {
        "lines" => Ok(Box::new(Lines::default())),
        "base64" => Ok(Box::new(Base64::default())),
        "gzip" => Ok(Box::new(Gzip::default())),
        "zlib" => Ok(Box::new(Zlib::default())),
        "xz2" => Ok(Box::new(Xz2::default())),
        "snappy" => Ok(Box::new(Snappy::default())),
        "lz4" => Ok(Box::new(Lz4::default())),
        "ingest-ns" => Ok(Box::new(AttachIngresTs {})),
        "length-prefixed" => Ok(Box::new(LengthPrefix::default())),
        "gelf-chunking" => Ok(Box::new(Gelf::default())),
        "textual-length-prefix" => Ok(Box::new(TextualLength::default())),
        "zstd" => Ok(Box::new(Zstd::default())),
        _ => Err(format!("Postprocessor '{}' not found.", name).into()),
    }
}

/// Given the slice of postprocessor names: Lookup each of them and return them as `Postprocessors`
///
/// # Errors
///
///   * If any postprocessor is not known.
pub fn make_postprocessors(postprocessors: &[String]) -> Result<Postprocessors> {
    postprocessors.iter().map(|n| lookup(&n)).collect()
}

/// canonical way to process encoded data passed from a `Codec`
///
/// # Errors
///
///   * If a `Postprocessor` fails
pub fn postprocess(
    postprocessors: &mut [Box<dyn Postprocessor>], // We are borrowing a dyn box as we don't want to pass ownership.
    ingres_ns: u64,
    data: Vec<u8>,
) -> Result<Vec<Vec<u8>>> {
    let egress_ns = nanotime();
    let mut data = vec![data];
    let mut data1 = Vec::new();

    for pp in postprocessors {
        data1.clear();
        for d in &data {
            match pp.process(ingres_ns, egress_ns, d) {
                Ok(mut r) => data1.append(&mut r),
                Err(e) => {
                    return Err(format!("Postprocessor error {}", e).into());
                }
            }
        }
        mem::swap(&mut data, &mut data1);
    }

    Ok(data)
}

#[derive(Default)]
pub(crate) struct Lines {}
impl Postprocessor for Lines {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "lines"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        // padding capacity with 1 to account for the new line char we will be pushing
        let mut framed: Vec<u8> = Vec::with_capacity(data.len() + 1);
        framed.extend_from_slice(data);
        framed.push(b'\n');
        Ok(vec![framed])
    }
}

#[derive(Default)]
pub(crate) struct Base64 {}
impl Postprocessor for Base64 {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "base64"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::encode(&data).as_bytes().to_vec()])
    }
}

#[derive(Default)]
pub(crate) struct Gzip {}
impl Postprocessor for Gzip {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "gzip"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::Encoder;

        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

#[derive(Default)]
pub(crate) struct Zlib {}
impl Postprocessor for Zlib {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "zlib"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Encoder;
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().into_result()?])
    }
}

#[derive(Default)]
pub(crate) struct Xz2 {}
impl Postprocessor for Xz2 {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "xz2"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use xz2::write::XzEncoder as Encoder;
        let mut encoder = Encoder::new(Vec::new(), 9);
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish()?])
    }
}

#[derive(Default)]
pub(crate) struct Snappy {}
impl Postprocessor for Snappy {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "snappy"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use snap::write::FrameEncoder;
        let mut writer = FrameEncoder::new(vec![]);
        writer.write_all(data)?;
        let compressed = writer
            .into_inner()
            .map_err(|e| Error::from(format!("Snappy compression postprocessor error: {}", e)))?;
        Ok(vec![compressed])
    }
}

#[derive(Default)]
pub(crate) struct Lz4 {}
impl Postprocessor for Lz4 {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "lz4"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use lz4::EncoderBuilder;
        let buffer = Vec::<u8>::new();
        let mut encoder = EncoderBuilder::new().level(4).build(buffer)?;
        encoder.write_all(&data)?;
        Ok(vec![encoder.finish().0])
    }
}

pub(crate) struct AttachIngresTs {}
impl Postprocessor for AttachIngresTs {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "attach-ingress-ts"
    }

    fn process(&mut self, ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(ingres_ns)?;
        res.write_all(&data)?;

        Ok(vec![res])
    }
}

#[derive(Clone, Default)]
pub(crate) struct LengthPrefix {}
impl Postprocessor for LengthPrefix {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "length-prefix"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(data.len() + 8);
        res.write_u64::<BigEndian>(data.len() as u64)?;
        res.write_all(&data)?;
        Ok(vec![res])
    }
}

#[derive(Clone, Default)]
pub(crate) struct TextualLength {}
impl Postprocessor for TextualLength {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "textual-length-prefix"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let size = data.len();
        let mut digits: Vec<u8> = size.to_string().into_bytes();
        let mut res = Vec::with_capacity(digits.len() + 1 + size);
        res.append(&mut digits);
        res.push(32);
        res.write_all(&data)?;
        Ok(vec![res])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Zstd {}
impl Postprocessor for Zstd {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "zstd"
    }

    fn process(&mut self, _ingres_ns: u64, _egress_ns: u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        // Value of 0 indicates default level for encode.
        let compressed = zstd::encode_all(data, 0)?;
        Ok(vec![compressed])
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn line() {
        let mut line = Lines {};
        let data: [u8; 0] = [];
        assert_eq!(Ok(vec![vec![b'\n']]), line.process(0, 0, &data));
        assert_eq!(
            Ok(vec![vec![b'f', b'o', b'o', b'b', b'\n']]),
            line.process(0, 0, b"foob")
        );
    }

    #[test]
    fn base64() {
        let mut post = Base64 {};
        let data: [u8; 0] = [];

        assert_eq!(Ok(vec![vec![]]), post.process(0, 0, &data));

        assert_eq!(Ok(vec![b"Cg==".to_vec()]), post.process(0, 0, b"\n"));

        assert_eq!(Ok(vec![b"c25vdA==".to_vec()]), post.process(0, 0, b"snot"));
    }

    #[test]
    fn textual_length_prefix_postp() {
        let mut post = TextualLength {};
        let data = vec![1_u8, 2, 3];
        let encoded = post.process(42, 23, &data).unwrap().pop().unwrap();
        assert_eq!("3 \u{1}\u{2}\u{3}", str::from_utf8(&encoded).unwrap());
    }
}
