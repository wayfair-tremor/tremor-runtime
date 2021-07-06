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
pub(crate) mod lines;

use crate::errors::{Error, Result};
use crate::url::TremorUrl;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use bytes::buf::Buf;
use bytes::BytesMut;
use std::str;

use std::io::{self, Read};

//pub type Lines = lines::Lines;

/// A set of preprocessors
pub type Preprocessors = Vec<Box<dyn Preprocessor>>;

/// Preprocessor trait
pub trait Preprocessor: Sync + Send {
    /// Canonical name for this preprocessor
    fn name(&self) -> &str;
    /// process data
    ///
    /// # Errors
    ///
    /// * Errors if the data can not processed
    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>>;
}

/// Lookup a preprocessor implementation via its unique id
///
/// # Errors
///
///   * Errors if the preprocessor is not known
#[cfg(not(tarpaulin_include))]
pub fn lookup(name: &str) -> Result<Box<dyn Preprocessor>> {
    match name {
        // TODO once preprocessors allow configuration, remove multiple entries for lines here
        "lines" => Ok(Box::new(Lines::new('\n', 1_048_576, true))),
        "lines-null" => Ok(Box::new(Lines::new('\0', 1_048_576, true))),
        "lines-pipe" => Ok(Box::new(Lines::new('|', 1_048_576, true))),
        "lines-no-buffer" => Ok(Box::new(Lines::new('\n', 0, false))),
        "lines-cr-no-buffer" => Ok(Box::new(Lines::new('\r', 0, false))),
        "base64" => Ok(Box::new(Base64::default())),
        "gzip" => Ok(Box::new(Gzip::default())),
        "zlib" => Ok(Box::new(Zlib::default())),
        "xz2" => Ok(Box::new(Xz2::default())),
        "snappy" => Ok(Box::new(Snappy::default())),
        "lz4" => Ok(Box::new(Lz4::default())),
        "decompress" => Ok(Box::new(Decompress {})),
        "remove-empty" => Ok(Box::new(FilterEmpty::default())),
        "gelf-chunking" => Ok(Box::new(Gelf::default())),
        "gelf-chunking-tcp" => Ok(Box::new(Gelf::tcp())),
        "ingest-ns" => Ok(Box::new(ExtractIngresTs {})),
        "length-prefixed" => Ok(Box::new(LengthPrefix::default())),
        "textual-length-prefix" => Ok(Box::new(TextualLength::default())),
        "zstd" => Ok(Box::new(Zstd::default())),
        _ => Err(format!("Preprocessor '{}' not found.", name).into()),
    }
}

/// Given the slice of preprocessor names: Look them up and return them as `Preprocessors`.
///
/// # Errors
///
///   * If the preprocessor is not known.
pub fn make_preprocessors(preprocessors: &[String]) -> Result<Preprocessors> {
    preprocessors.iter().map(|n| lookup(&n)).collect()
}

/// Canonical way to preprocess data before it is fed to a codec for decoding.
///
/// Preprocessors might split up the given data in multiple chunks. Each of those
/// chunks must be seperately decoded by a `Codec`.
///
/// # Errors
///
///   * If a preprocessor failed
pub fn preprocess(
    preprocessors: &mut [Box<dyn Preprocessor>],
    ingest_ns: &mut u64,
    data: Vec<u8>,
    instance_id: &TremorUrl,
) -> Result<Vec<Vec<u8>>> {
    let mut data = vec![data];
    let mut data1 = Vec::new();
    for pp in preprocessors {
        data1.clear();
        for (i, d) in data.iter().enumerate() {
            match pp.process(ingest_ns, d) {
                Ok(mut r) => data1.append(&mut r),
                Err(e) => {
                    error!("[{}] Preprocessor [{}] error {}", instance_id, i, e);
                    return Err(e);
                }
            }
        }
        std::mem::swap(&mut data, &mut data1);
    }
    Ok(data)
}

trait SliceTrim {
    fn trim(&self) -> &Self;
}

impl SliceTrim for [u8] {
    fn trim(&self) -> &[u8] {
        fn is_not_whitespace(c: u8) -> bool {
            c != b'\t' && c != b' '
        }

        self.iter()
            .position(|v| is_not_whitespace(*v))
            .map_or(&[] as &[u8], |first| {
                self.iter()
                    .rposition(|v| is_not_whitespace(*v))
                    .map_or_else(
                        || self.get(first..).unwrap_or_default(),
                        |last| self.get(first..=last).unwrap_or_default(),
                    )
            })
    }
}

pub(crate) use lines::Lines;

#[derive(Default, Debug, Clone)]
pub(crate) struct FilterEmpty {}

impl Preprocessor for FilterEmpty {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "remove-empty"
    }
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        if data.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![data.to_vec()])
        }
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Nulls {}
impl Preprocessor for Nulls {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "nulls"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(data.split(|c| *c == b'\0').map(Vec::from).collect())
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct ExtractIngresTs {}
impl Preprocessor for ExtractIngresTs {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "extract-ingress-ts"
    }

    fn process(&mut self, ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use std::io::Cursor;
        if let Some(d) = data.get(8..) {
            *ingest_ns = Cursor::new(data).read_u64::<BigEndian>()?;
            Ok(vec![d.to_vec()])
        } else {
            Err(Error::from("Extract Ingest Ts Preprocessor: < 8 byte"))
        }
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Base64 {}
impl Preprocessor for Base64 {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "base64"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(vec![base64::decode(&data)?])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Gzip {}
impl Preprocessor for Gzip {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "gzip"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::gzip::MultiDecoder;
        let mut decoder = MultiDecoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Zlib {}
impl Preprocessor for Zlib {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "zlib"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use libflate::zlib::Decoder;
        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Xz2 {}
impl Preprocessor for Xz2 {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "xz2"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use xz2::read::XzDecoder as Decoder;
        let mut decoder = Decoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Snappy {}
impl Preprocessor for Snappy {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "snappy"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use snap::read::FrameDecoder;
        let mut rdr = FrameDecoder::new(data);
        let decompressed_len = snap::raw::decompress_len(data)?;
        let mut decompressed = Vec::with_capacity(decompressed_len);
        io::copy(&mut rdr, &mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Lz4 {}
impl Preprocessor for Lz4 {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "lz4"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        use lz4::Decoder;
        let mut decoder = Decoder::new(data)?;
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(vec![decompressed])
    }
}

#[derive(Clone, Default, Debug)]
pub(crate) struct Decompress {}
impl Preprocessor for Decompress {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "decompress"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let r = match data.get(0..6) {
            Some(&[0x1f, 0x8b, _, _, _, _]) => {
                use libflate::gzip::Decoder;
                let mut decoder = Decoder::new(data)?;
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                decompressed
            }
            // ZLib magic headers
            Some(&[0x78, 0x01 | 0x5e | 0x9c | 0xda, _, _, _, _]) => {
                use libflate::zlib::Decoder;
                let mut decoder = Decoder::new(data)?;
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                decompressed
            }
            Some(&[0xfd, b'7', b'z', b'X', b'Z', 0x00]) => {
                use xz2::read::XzDecoder as Decoder;
                let mut decoder = Decoder::new(data);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                decompressed
            }
            // Some(b"sNaPpY") => {
            Some(&[0xff, _, _, _, _, _]) => {
                use snap::read::FrameDecoder;
                let mut rdr = FrameDecoder::new(data);
                let decompressed_len = snap::raw::decompress_len(data)?;
                let mut decompressed = Vec::with_capacity(decompressed_len);
                io::copy(&mut rdr, &mut decompressed)?;
                decompressed
            }
            Some(&[0x04, 0x22, 0x4D, 0x18, _, _]) => {
                use lz4::Decoder;
                let mut decoder = Decoder::new(data)?;
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                decompressed
            }
            // Zstd Magic : 0xFD2FB528 (but little endian)
            Some(&[0x28, 0xb5, 0x2f, 0xfd, _, _]) => zstd::decode_all(data)?,
            _ => data.to_vec(),
        };
        Ok(vec![r])
    }
}
#[derive(Clone, Default, Debug)]
pub(crate) struct LengthPrefix {
    len: Option<usize>,
    buffer: BytesMut,
}
impl Preprocessor for LengthPrefix {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "length-prefix"
    }

    #[allow(clippy::cast_possible_truncation)]
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.buffer.extend(data);

        let mut res = Vec::new();
        loop {
            if let Some(l) = self.len {
                if self.buffer.len() >= l {
                    let mut part = self.buffer.split_off(l);
                    std::mem::swap(&mut part, &mut self.buffer);
                    res.push(part.to_vec());
                    self.len = None;
                } else {
                    break;
                }
            }
            if self.buffer.len() > 8 {
                self.len = Some(BigEndian::read_u64(&self.buffer) as usize);
                self.buffer.advance(8);
            } else {
                break;
            }
        }
        Ok(res)
    }
}
#[derive(Clone, Default, Debug)]
pub(crate) struct TextualLength {
    len: Option<usize>,
    buffer: BytesMut,
}
impl Preprocessor for TextualLength {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "textual-length-prefix"
    }

    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.buffer.extend(data);

        let mut res = Vec::new();
        loop {
            if let Some(l) = self.len {
                if self.buffer.len() >= l {
                    let mut part = self.buffer.split_off(l);
                    std::mem::swap(&mut part, &mut self.buffer);
                    res.push(part.to_vec());
                    self.len = None;
                } else {
                    break;
                }
            }

            // find the whitespace
            if let Some((i, _c)) = self
                .buffer
                .iter()
                .enumerate()
                .find(|(_i, c)| c.is_ascii_whitespace())
            {
                let mut buf = self.buffer.split_off(i);
                std::mem::swap(&mut buf, &mut self.buffer);
                // parse the textual length
                let l = str::from_utf8(&buf)?;
                self.len = Some(l.parse::<u32>()? as usize);
                self.buffer.advance(1); // advance beyond the whitespace delimiter
            } else {
                // no whitespace found
                break;
            }
        }
        Ok(res)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Zstd {}
impl Preprocessor for Zstd {
    #[cfg(not(tarpaulin_include))]
    fn name(&self) -> &str {
        "ztd"
    }
    fn process(&mut self, _ingest_ns: &mut u64, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let decoded: Vec<u8> = zstd::decode_all(data)?;
        Ok(vec![decoded])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::postprocessor::{self as post, Postprocessor};
    use crate::preprocessor::{self as pre, Preprocessor};
    #[test]
    fn ts() {
        let mut pre_p = pre::ExtractIngresTs {};
        let mut post_p = post::AttachIngresTs {};

        let data = vec![1_u8, 2, 3];

        let encoded = post_p.process(42, 23, &data).unwrap().pop().unwrap();

        let mut in_ns = 0u64;
        let decoded = pre_p.process(&mut in_ns, &encoded).unwrap().pop().unwrap();

        assert_eq!(data, decoded);
        assert_eq!(in_ns, 42);
    }

    fn textual_prefix(len: usize) -> String {
        format!("{} {}", len, String::from_utf8(vec![b'O'; len]).unwrap())
    }

    use proptest::prelude::*;

    // generate multiple chopped length-prefixed strings
    fn multiple_textual_lengths(max_elements: usize) -> BoxedStrategy<(Vec<usize>, Vec<String>)> {
        proptest::collection::vec(".+", 1..max_elements) // generator for Vec<String> of arbitrary strings, maximum length of vector: `max_elements`
            .prop_map(|ss| {
                let s: (Vec<usize>, Vec<String>) = ss
                    .into_iter()
                    .map(|s| (s.len(), format!("{} {}", s.len(), s))) // for each string, extract the length, and create a textual length prefix
                    .unzip();
                s
            })
            .prop_map(|tuple| (tuple.0, tuple.1.join(""))) // generator for a tuple of 1. the sizes of the length prefixed strings, 2. the concatenated length prefixed strings as one giant string
            .prop_map(|tuple| {
                // here we chop the big string into up to 4 bits
                let mut chopped = Vec::with_capacity(4);
                let mut giant_string: String = tuple.1.clone();
                while giant_string.len() > 0 && chopped.len() < 4 {
                    // verify we are at a char boundary
                    let indices = giant_string.char_indices();
                    let num_chars = giant_string.chars().count();
                    if let Some((index, _)) = indices.skip(num_chars / 2).next() {
                        let mut splitted = giant_string.split_off(index);
                        std::mem::swap(&mut splitted, &mut giant_string);
                        chopped.push(splitted);
                    } else {
                        break;
                    }
                }
                chopped.push(giant_string);
                (tuple.0, chopped)
            })
            .boxed()
    }

    proptest! {
        #[test]
        fn textual_length_prefix_prop((lengths, datas) in multiple_textual_lengths(5)) {
            let mut pre_p = pre::TextualLength::default();
            let mut in_ns = 0_u64;
            let res: Vec<Vec<u8>> = datas.into_iter().map(|data| {
                pre_p.process(&mut in_ns, data.as_bytes()).unwrap()
            }).flatten().collect();
            assert_eq!(lengths.len(), res.len());
            for (processed, expected_len) in res.iter().zip(lengths) {
                assert_eq!(expected_len, processed.len());
            }
        }

        #[test]
        fn textual_length_pre_post(length in 1..100_usize) {
            let data = vec![1_u8; length];
            let mut pre_p = pre::TextualLength::default();
            let mut post_p = post::TextualLength::default();
            let encoded = post_p.process(0, 0, &data).unwrap().pop().unwrap();
            let mut in_ns = 0_u64;
            let mut res = pre_p.process(&mut in_ns, &encoded).unwrap();
            assert_eq!(1, res.len());
            let payload = res.pop().unwrap();
            assert_eq!(length, payload.len());
        }
    }

    #[test]
    fn textual_prefix_length_loop() {
        let datas = vec![
            "24 \'?\u{d617e}ѨR\u{202e}\u{f8f7c}\u{ede29}\u{ac784}36 ?{¥?MȺ\r\u{bac41}9\u{5bbbb}\r\u{1c46c}\u{4ba79}¥\u{7f}*?:\u{0}$i",
            "60 %\u{a825a}\u{a4269}\u{39e0c}\u{b3e21}<ì\u{f6c20}ѨÛ`HW\u{9523f}V",
            "\u{3}\u{605fe}%Fq\u{89b5e}\u{93780}Q3",
            "¥?\u{feff}9",
            " \'�2\u{4269b}",
        ];
        let lengths: Vec<usize> = vec![24, 36, 60, 9];
        let mut pre_p = pre::TextualLength::default();
        let mut in_ns = 0_u64;
        let res: Vec<Vec<u8>> = datas
            .into_iter()
            .map(|data| pre_p.process(&mut in_ns, data.as_bytes()).unwrap())
            .flatten()
            .collect();
        assert_eq!(lengths.len(), res.len());
        for (processed, expected_len) in res.iter().zip(lengths) {
            assert_eq!(expected_len, processed.len());
        }
    }

    #[test]
    fn textual_length_prefix() {
        let mut pre_p = pre::TextualLength::default();
        let data = textual_prefix(42);
        let mut in_ns = 0_u64;
        let mut res = pre_p.process(&mut in_ns, data.as_bytes()).unwrap();
        assert_eq!(1, res.len());
        let payload = res.pop().unwrap();
        assert_eq!(42, payload.len());
    }

    #[test]
    fn empty_textual_prefix() {
        let data = ("").as_bytes();
        let mut pre_p = pre::TextualLength::default();
        let mut post_p = post::TextualLength::default();
        let mut in_ns = 0_u64;
        let res = pre_p.process(&mut in_ns, data).unwrap();
        assert_eq!(0, res.len());

        let data_empty = vec![];
        let encoded = post_p.process(42, 23, &data_empty).unwrap().pop().unwrap();
        assert_eq!("0 ", str::from_utf8(&encoded).unwrap());
        let mut res2 = pre_p.process(&mut in_ns, &encoded).unwrap();
        assert_eq!(1, res2.len());
        let payload = res2.pop().unwrap();
        assert_eq!(0, payload.len());
    }

    #[test]
    fn length_prefix() -> Result<()> {
        let mut it = 0;

        let pre_p = pre::LengthPrefix::default();
        let mut post_p = post::LengthPrefix::default();

        let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let wire = post_p.process(0, 0, &data)?;
        let (start, end) = wire[0].split_at(7);
        let id = TremorUrl::parse("/onramp/snot/00").unwrap();
        let mut pps: Vec<Box<dyn Preprocessor>> = vec![Box::new(pre_p)];
        let recv = preprocess(pps.as_mut_slice(), &mut it, start.to_vec(), &id)?;
        assert!(recv.is_empty());
        let recv = preprocess(pps.as_mut_slice(), &mut it, end.to_vec(), &id)?;
        assert_eq!(recv[0], data);
        Ok(())
    }

    const LOOKUP_TABLE: [&str; 17] = [
        "lines",
        "lines-null",
        "lines-pipe",
        "base64",
        "gzip",
        "zlib",
        "xz2",
        "snappy",
        "lz4",
        "decompress",
        "remove-empty",
        "gelf-chunking",
        "gelf-chunking-tcp",
        "ingest-ns",
        "length-prefixed",
        "textual-length-prefix",
        "zstd",
    ];

    #[test]
    fn test_lookup() -> Result<()> {
        for t in LOOKUP_TABLE.iter() {
            assert!(lookup(t).is_ok());
        }
        let t = "snot";
        assert!(lookup(&t).is_err());
        Ok(())
    }

    #[test]
    fn test_filter_empty() -> Result<()> {
        let mut pre = FilterEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &vec![]));
        Ok(())
    }

    #[test]
    fn test_filter_null() -> Result<()> {
        let mut pre = FilterEmpty::default();
        assert_eq!(Ok(vec![]), pre.process(&mut 0_u64, &vec![]));
        Ok(())
    }

    macro_rules! assert_decompress {
        ($internal:expr, $which:ident, $magic:expr) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = crate::preprocessor::Decompress::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let r = outbound.process(ingest_ns, egress_ns, $internal);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected ( magic code only )
            assert_eq!($magic, decode_magic(&ext));

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$internal, &out);
        };
    }
    macro_rules! assert_simple_symmetric {
        ($internal:expr, $which:ident, $magic:expr) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = crate::preprocessor::$which::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let r = outbound.process(ingest_ns, egress_ns, $internal);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected ( magic code only )
            assert_eq!($magic, decode_magic(&ext));

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$internal, &out);
        };
    }

    macro_rules! assert_symmetric {
        ($inbound:expr, $outbound:expr, $which:ident) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = crate::preprocessor::$which::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let enc = $outbound.clone();
            let r = outbound.process(ingest_ns, egress_ns, $inbound);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected
            assert_eq!(&enc, &ext);

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$inbound, &out);
        };
    }

    macro_rules! assert_not_symmetric {
        ($inbound:expr, $internal:expr, $outbound:expr, $which:ident) => {
            // Assert pre and post processors have a sensible default() ctor
            let mut inbound = crate::preprocessor::$which::default();
            let mut outbound = crate::postprocessor::$which::default();

            // Fake ingest_ns and egress_ns
            let mut ingest_ns = 0_u64;
            let egress_ns = 1_u64;

            let enc = $internal.clone();
            let r = outbound.process(ingest_ns, egress_ns, $inbound);
            let ext = &r?[0];
            let ext = ext.as_slice();
            // Assert actual encoded form is as expected
            assert_eq!(&enc, &ext);

            let r = inbound.process(&mut ingest_ns, &ext);
            let out = &r?[0];
            let out = out.as_slice();
            // Assert actual decoded form is as expected
            assert_eq!(&$outbound, &out);
        };
    }

    fn decode_magic(data: &[u8]) -> &'static str {
        match data.get(0..6) {
            Some(&[0x1f, 0x8b, _, _, _, _]) => "gzip",
            Some(&[0x78, _, _, _, _, _]) => "zlib",
            Some(&[0xfd, b'7', b'z', _, _, _]) => "xz2",
            Some(b"sNaPpY") => "snap",
            Some(&[0xff, 0x6, 0x0, 0x0, _, _]) => "snap",
            Some(&[0x04, 0x22, 0x4d, 0x18, _, _]) => "lz4",
            Some(&[0x28, 0xb5, 0x2f, 0xfd, _, _]) => "zstd",
            _ => "fail/unknown",
        }
    }

    #[test]
    fn test_lines() -> Result<()> {
        let int = "snot\nbadger".as_bytes();
        let enc = "snot\nbadger\n".as_bytes(); // First event ( event per line )
        let out = "snot".as_bytes();
        assert_not_symmetric!(int, enc, out, Lines);
        Ok(())
    }

    macro_rules! assert_lines_no_buffer {
        ($inbound:expr, $outbound1:expr, $outbound2:expr, $case_number:expr, $separator:expr) => {
            let mut ingest_ns = 0_u64;
            let r = crate::preprocessor::Lines::new($separator, 0, false)
                .process(&mut ingest_ns, $inbound);

            let out = &r?;
            // Assert preprocessor output is as expected
            assert!(
                2 == out.len(),
                "Test case : {} => expected output = {}, actual output = {}",
                $case_number,
                "2",
                out.len()
            );
            assert!(
                $outbound1 == out[0].as_slice(),
                "Test case : {} => expected output = \"{}\", actual output = \"{}\"",
                $case_number,
                std::str::from_utf8($outbound1).unwrap(),
                std::str::from_utf8(out[0].as_slice()).unwrap()
            );
            assert!(
                $outbound2 == out[1].as_slice(),
                "Test case : {} => expected output = \"{}\", actual output = \"{}\"",
                $case_number,
                std::str::from_utf8($outbound2).unwrap(),
                std::str::from_utf8(out[1].as_slice()).unwrap()
            );
        };
    }

    #[test]
    fn test_lines_no_buffer_no_maxlength() -> Result<()> {
        let test_data: [(&'static [u8], &'static [u8], &'static [u8], &'static str); 4] = [
            (b"snot\nbadger", b"snot", b"badger", "0"),
            (b"snot\n", b"snot", b"", "1"),
            (b"\nsnot", b"", b"snot", "2"),
            (b"\n", b"", b"", "3"),
        ];
        for case in &test_data {
            assert_lines_no_buffer!(case.0, case.1, case.2, case.3, '\n');
        }

        Ok(())
    }

    #[test]
    fn test_carriage_return_no_buffer_no_maxlength() -> Result<()> {
        let test_data: [(&'static [u8], &'static [u8], &'static [u8], &'static str); 4] = [
            (b"snot\rbadger", b"snot", b"badger", "0"),
            (b"snot\r", b"snot", b"", "1"),
            (b"\rsnot", b"", b"snot", "2"),
            (b"\r", b"", b"", "3"),
        ];
        for case in &test_data {
            assert_lines_no_buffer!(case.0, case.1, case.2, case.3, '\r');
        }

        Ok(())
    }

    #[test]
    fn test_base64() -> Result<()> {
        let int = "snot badger".as_bytes();
        let enc = "c25vdCBiYWRnZXI=".as_bytes();
        assert_symmetric!(int, enc, Base64);
        Ok(())
    }

    #[test]
    fn test_gzip() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Gzip, "gzip");
        assert_decompress!(int, Gzip, "gzip");
        Ok(())
    }

    #[test]
    fn test_zlib() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Zlib, "zlib");
        assert_decompress!(int, Zlib, "zlib");
        Ok(())
    }

    #[test]
    fn test_snappy() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Snappy, "snap");
        assert_decompress!(int, Snappy, "snap");
        Ok(())
    }

    #[test]
    fn test_xz2() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Xz2, "xz2");
        assert_decompress!(int, Xz2, "xz2");
        Ok(())
    }

    #[test]
    fn test_lz4() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Lz4, "lz4");
        assert_decompress!(int, Lz4, "lz4");
        Ok(())
    }
    #[test]
    fn test_zstd() -> Result<()> {
        let int = "snot".as_bytes();
        assert_simple_symmetric!(int, Zstd, "zstd");
        assert_decompress!(int, Zstd, "zstd");
        Ok(())
    }
}
