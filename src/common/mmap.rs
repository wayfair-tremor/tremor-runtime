// Copyright 2021, The Tremor Team
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
use mapr::MmapOptions;
use std::cmp;
use std::fs::OpenOptions;
use std::io;
use std::io::Write;
use std::ops::DerefMut;
use std::path::Path;
use tremor_pipeline::ConfigImpl;
use tremor_script::prelude::*;

pub trait Kv {
    fn get(&mut self) -> Result<simd_json::OwnedValue>;
    fn set(&mut self, obj: simd_json::OwnedValue) -> Result<()>;
}

pub struct MmapFile {
    pub config: Config,
    pub store: mapr::MmapMut,
    pub len: usize,
    pub end: usize,
}
pub struct MmapAnon {
    pub store: mapr::MmapMut,
    pub end: usize,
    pub len: usize,
}

impl MmapFile {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        // We read until it's end, so we know it's OK
        unsafe { self.store.get_unchecked_mut(..self.end) }
    }
}

impl MmapAnon {
    fn as_slice(&self) -> &[u8] {
        // We read until it's end, so we know it's OK
        unsafe { self.store.get_unchecked(..self.end) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // We read until it's end, so we know it's OK
        unsafe { self.store.get_unchecked_mut(..self.end) }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.store.flush()
    }
}

impl Kv for MmapFile {
    fn get(&mut self) -> Result<simd_json::OwnedValue> {
        let obj = simd_json::to_owned_value(self.as_mut_slice())?;

        Ok(obj)
    }

    fn set(&mut self, obj: simd_json::OwnedValue) -> Result<()> {
        let string = obj.encode();
        let bytes = string.as_bytes();
        if bytes.len() > self.len {
            return Err("object too large to store in memory-mapped file".into());
        }
        self.store.deref_mut().write_all(bytes)?;
        self.end = bytes.len();

        Ok(())
    }
}

impl Kv for MmapAnon {
    fn get(&mut self) -> Result<simd_json::OwnedValue> {
        let mmap = self.as_slice();
        let mut bytes: Vec<u8> = Vec::with_capacity(mmap.len());
        bytes.extend_from_slice(mmap);
        let obj = simd_json::to_owned_value(&mut bytes)?;

        Ok(obj)
    }

    fn set(&mut self, obj: simd_json::OwnedValue) -> Result<()> {
        let string = obj.encode();
        let bytes = string.as_bytes();
        if self.store.len() < bytes.len() {
            let new_size = bytes.len();
            let len = cmp::max(self.len + self.len, new_size);
            let new_store = MmapOptions::new().len(len).map_anon()?;
            let new_mmap = Self {
                store: new_store,
                len,
                end: 0,
            };
            *self = new_mmap;
        }
        self.end = bytes.len();
        self.as_mut_slice().copy_from_slice(bytes);
        self.flush()?;

        Ok(())
    }
}

impl ConfigImpl for Config {}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub path: String,
    pub size: usize,
}

impl MmapAnon {
    fn from_config(
        config: Option<Config>,
        obj: &simd_json::OwnedValue,
    ) -> Result<Box<dyn Kv + Send>> {
        if let Some(_config) = config {
            let string = obj.encode();
            let bytes = string.as_bytes();
            let len = bytes.len();
            let mut store = MmapOptions::new().len(len).map_anon()?;
            store.copy_from_slice(bytes);
            store.flush()?;

            Ok(Box::new(Self {
                store,
                end: len,
                len,
            }))
        } else {
            Err("Missing config for mmap".into())
        }
    }
}

impl MmapFile {
    fn from_config(
        config: Option<Config>,
        obj: &simd_json::OwnedValue,
    ) -> Result<Box<dyn Kv + Send>> {
        if let Some(config) = config {
            let p = Path::new(&config.path);
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(p)?;
            file.set_len(config.size as u64)?;
            let len = config.size as usize;
            let string = obj.encode();
            let bytes = string.as_bytes();
            let end = bytes.len();
            file.write_all(&bytes)?;
            let store = unsafe { MmapOptions::new().map(&file)? };
            let store = store.make_mut()?;

            Ok(Box::new(Self {
                config,
                store,
                len,
                end,
            }))
        } else {
            Err("Missing config for mmap".into())
        }
    }
}

pub fn lookup(
    name: &str,
    config: Option<Config>,
    obj: &simd_json::OwnedValue,
) -> Result<Box<dyn Kv + Send>> {
    match name {
        "mmap_file" => MmapFile::from_config(config, &obj),
        "mmap_anon" => MmapAnon::from_config(config, &obj),
        _ => Err(format!("Cache {} not known", name).into()),
    }
}

#[cfg(test)]

mod tests {

    use super::{Config, MmapAnon, MmapFile};
    use tempfile::tempdir;
    use tremor_common::file;

    #[test]
    fn test_mmap_anon() {
        let config = Config {
            path: "/this/is/never/used.json".to_string(),
            size: 4096,
        };
        let mut data = b"{\"foo\": \"bar\"}".to_vec();
        let bytes = data.as_mut_slice();
        let opt = Some(config);
        let obj = simd_json::to_owned_value(bytes).unwrap();
        let exp_obj = obj.clone();
        let mut mmap = MmapAnon::from_config(opt, &obj).expect("To create anon memory map");

        assert_eq!(mmap.get().expect("To retrieve object"), exp_obj);

        let mut data2 = b"{\"snot\": \"badger\"}".to_vec();
        let bytes2 = data2.as_mut_slice();
        let obj2 = simd_json::to_owned_value(bytes2).unwrap();
        let exp_obj2 = obj2.clone();
        mmap.set(obj2).expect("To set object in mmap");
        let ret_obj = mmap.get().expect("To retrieve object");

        assert_eq!(ret_obj, exp_obj2);
    }

    #[test]
    fn test_mmap_file() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("storage.json");
        let file = file::create(&file_path).unwrap();
        let config = Config {
            path: file_path.as_path().to_string_lossy().to_string(),
            size: 12,
        };
        let opt = std::option::Option::from(config);
        let mut data = b"[0,1,2,3,4]".to_vec();
        let bytes = data.as_mut_slice();
        let obj = simd_json::to_owned_value(bytes).unwrap();
        let exp_obj = obj.clone();
        let mut mmap = MmapFile::from_config(opt, &obj).expect("To create file-backed memory map");

        assert_eq!(mmap.get().expect("To retrieve object"), exp_obj);

        let mut data2 = b"[5,6,7,8,9]".to_vec();
        let bytes2 = data2.as_mut_slice();
        let obj2 = simd_json::to_owned_value(bytes2).unwrap();
        let exp_obj2 = obj2.clone();
        mmap.set(obj2).expect("To set object in mmap");

        assert_eq!(mmap.get().expect("To retrieve object"), exp_obj2);

        let mut data3 = b"{\"foozah\": \"barah\"}".to_vec();
        let bytes3 = data3.as_mut_slice();
        let obj3 = simd_json::to_owned_value(bytes3).unwrap();

        assert!(
            mmap.set(obj3).is_err(),
            "object too large to store in memory-mapped file"
        );

        drop(file);
        dir.close().unwrap();
    }
}
