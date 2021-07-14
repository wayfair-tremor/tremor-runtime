// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod cmp;
/// Conversions from other types to the value type
pub mod from;
mod serialize;

use crate::{Error, Result};
use beef::Cow;
use halfbrown::HashMap;
use simd_json::prelude::*;
use simd_json::{AlignedBuf, Deserializer, Node, StaticNode};
use std::{borrow::Borrow, convert::TryInto, fmt};
use std::{cmp::Ord, hash::Hash};
use std::{
    cmp::Ordering,
    ops::{Index, IndexMut},
};

pub use crate::serde::to_value;

/// Representation of a JSON object
pub type Object<'value> = HashMap<Cow<'value, str>, Value<'value>>;
/// Bytes
pub type Bytes<'value> = Cow<'value, [u8]>;

/// Parses a slice of bytes into a Value dom. This function will
/// rewrite the slice to de-escape strings.
/// As we reference parts of the input slice the resulting dom
/// has the same lifetime as the slice it was created from.
///
/// # Errors
///
/// Will return `Err` if `s` is invalid JSON.
pub fn parse_to_value(s: &mut [u8]) -> Result<Value> {
    match Deserializer::from_slice(s) {
        Ok(de) => Ok(ValueDeserializer::from_deserializer(de).parse()),
        Err(e) => Err(Error::SimdJson(e)),
    }
}

/// Parses a slice of bytes into a Value dom. This function will
/// rewrite the slice to de-escape strings.
/// As we reference parts of the input slice the resulting dom
/// has the same lifetime as the slice it was created from.
///
/// # Errors
///
/// Will return `Err` if `s` is invalid JSON.
pub fn parse_to_value_with_buffers<'value>(
    s: &'value mut [u8],
    input_buffer: &mut AlignedBuf,
    string_buffer: &mut [u8],
) -> Result<Value<'value>> {
    match Deserializer::from_slice_with_buffers(s, input_buffer, string_buffer) {
        Ok(de) => Ok(ValueDeserializer::from_deserializer(de).parse()),
        Err(e) => Err(Error::SimdJson(e)),
    }
}

/// Borrowed JSON-DOM Value, consider using the `ValueTrait`
/// to access its content
#[derive(Debug, Clone)]
pub enum Value<'value> {
    /// Static values
    Static(StaticNode),
    /// string type
    String(Cow<'value, str>),
    /// array type
    Array(Vec<Value<'value>>),
    /// object type
    Object(Box<Object<'value>>),
    /// A binary type
    Bytes(Bytes<'value>),
}

impl<'value> Eq for Value<'value> {}

impl<'value> Value<'value> {
    /// Creates an empty array value
    #[must_use]
    pub const fn array() -> Self {
        Value::Array(vec![])
    }

    /// Creates an empty array value
    #[must_use]
    pub const fn const_null() -> Self {
        Value::Static(StaticNode::Null)
    }
}

#[derive(PartialEq)]
struct Static(StaticNode);

impl Eq for Static {}

impl PartialOrd for Static {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Static {
    // We allow this since we check bounds before we cast
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_precision_loss,
        clippy::cast_sign_loss
    )]
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.0, other.0) {
            (StaticNode::Null, StaticNode::Null) => Ordering::Equal,
            (StaticNode::Null, _) => Ordering::Greater,
            (_, StaticNode::Null) => Ordering::Less,
            (StaticNode::Bool(v1), StaticNode::Bool(v2)) => v1.cmp(&v2),
            (StaticNode::Bool(_b), _) => Ordering::Greater,
            (_, StaticNode::Bool(_b)) => Ordering::Less,
            (StaticNode::U64(v1), StaticNode::U64(v2)) => v1.cmp(&v2),
            (StaticNode::U64(v1), StaticNode::I64(v2)) => {
                if let Ok(v2) = v2.try_into() {
                    v1.cmp(&v2)
                } else {
                    Ordering::Greater
                }
            }
            (StaticNode::U64(v1), StaticNode::F64(v2)) => {
                if v2 < u64::min_value() as f64 {
                    Ordering::Greater
                } else if v2 > u64::max_value() as f64 {
                    Ordering::Less
                } else {
                    v1.cmp(&(v2 as u64))
                }
            }
            (StaticNode::I64(v1), StaticNode::I64(v2)) => v1.cmp(&v2),
            (StaticNode::I64(v1), StaticNode::U64(v2)) => {
                if let Ok(v1) = v1.try_into() {
                    let v1: u64 = v1;
                    v1.cmp(&v2)
                } else {
                    Ordering::Less
                }
            }
            (StaticNode::I64(v1), StaticNode::F64(v2)) => {
                if v2 < i64::min_value() as f64 {
                    Ordering::Greater
                } else if v2 > i64::max_value() as f64 {
                    Ordering::Less
                } else {
                    v1.cmp(&(v2 as i64))
                }
            }
            // This is not great!
            // While we don't expose it NaN but float doesn't implement Ord since it refuses to
            // compare Nan==Nan which makes sense so we kind of cheat around it by saying if it is
            // decide if one is greater or smaller then the other they're the same
            (StaticNode::F64(v1), StaticNode::F64(v2)) => {
                if v1 > v2 {
                    Ordering::Greater
                } else if v1 < v2 {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            }
            (StaticNode::F64(v1), StaticNode::U64(v2)) => {
                if v1 < u64::min_value() as f64 {
                    Ordering::Less
                } else if v1 > u64::max_value() as f64 {
                    Ordering::Greater
                } else {
                    (v1 as u64).cmp(&v2)
                }
            }
            (StaticNode::F64(v1), StaticNode::I64(v2)) => {
                if v1 < i64::min_value() as f64 {
                    Ordering::Less
                } else if v1 > i64::max_value() as f64 {
                    Ordering::Greater
                } else {
                    (v1 as i64).cmp(&v2)
                }
            }
        }
    }
}

impl<'value> PartialOrd for Value<'value> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<'value> Ord for Value<'value> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Static(v1), Value::Static(v2)) => Static(*v1).cmp(&Static(*v2)),
            (Value::Static(_s), _) => Ordering::Greater,
            (_, Value::Static(_s)) => Ordering::Less,
            (Value::Bytes(v1), Value::Bytes(v2)) => v1.cmp(v2),
            (Value::Bytes(v1), Value::String(v2)) => {
                let v1: &[u8] = &v1;
                v1.cmp(v2.as_bytes())
            }
            (Value::String(v1), Value::Bytes(v2)) => v1.as_bytes().cmp(&v2),
            (Value::Bytes(_b), _) => Ordering::Greater,
            (_, Value::Bytes(_b)) => Ordering::Less,
            (Value::String(v1), Value::String(v2)) => v1.cmp(v2),
            (Value::String(_s), _) => Ordering::Greater,
            (_, Value::String(_s)) => Ordering::Less,
            (Value::Array(v1), Value::Array(v2)) => v1.cmp(v2),
            (Value::Array(_a), _) => Ordering::Greater,
            (_, Value::Array(_a)) => Ordering::Less,
            (Value::Object(v1), Value::Object(v2)) => cmp_map(v1.as_ref(), v2.as_ref()),
        }
    }
}
fn cmp_map(left: &Object, right: &Object) -> Ordering {
    // Compare length first

    match left.len().cmp(&right.len()) {
        Ordering::Equal => (),
        o @ (Ordering::Greater | Ordering::Less) => return o,
    };

    // compare keyspace (sorted keys cmp)
    let mut keys_left: Vec<_> = left.keys().collect();
    let mut keys_right: Vec<_> = right.keys().collect();
    keys_left.sort();
    keys_right.sort();

    match keys_left.cmp(&keys_right) {
        Ordering::Equal => (),
        o @ (Ordering::Greater | Ordering::Less) => return o,
    };
    // Compare values (the first sorted value being non equal determines order)
    for k in keys_left {
        if let Some(left_val) = left.get(k) {
            if let Some(right_val) = right.get(k) {
                let c = left_val.cmp(right_val);
                if c != Ordering::Equal {
                    return c;
                }
            }
        }
    }
    Ordering::Equal
}

// impl<'value> Ord for Value<'value> {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         match (self, other) {
//             (Value::Static(n1), Value::Static(n2)) => n1.cmp(n2),
//             (Value::Static(_), _) => Ordering::Less,
//         }
//     }
// }

impl<'value> Value<'value> {
    /// Enforces static lifetime on a borrowed value, this will
    /// force all strings to become owned COW's, the same applies for
    /// Object keys.
    #[inline]
    #[must_use]
    pub fn into_static(self) -> Value<'static> {
        self.clone_static()
    }

    /// Clones the current value and enforces a static lifetime, it works the same
    /// as `into_static` but includes cloning logic
    #[inline]
    #[must_use]
    pub fn clone_static(&self) -> Value<'static> {
        match self {
            Self::String(s) => Value::String(Cow::owned(s.to_string())),
            Self::Array(arr) => arr.iter().map(Value::clone_static).collect(),
            Self::Object(obj) => obj
                .iter()
                .map(|(k, v)| (Cow::owned(k.to_string()), v.clone_static()))
                .collect(),
            Self::Static(s) => Value::Static(*s),
            Self::Bytes(b) => Value::Bytes(Cow::owned(b.to_vec())),
        }
    }

    /// Tries to get the bytes from a Value
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(bs) => Some(&bs),
            Value::String(bs) => Some(bs.as_bytes()),
            _ => None,
        }
    }

    /// Tries to get an element of an object as bytes
    #[inline]
    #[must_use]
    pub fn get_bytes<Q: ?Sized>(&self, k: &Q) -> Option<&[u8]>
    where
        Cow<'value, str>: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq + Ord,
    {
        self.get(k).and_then(Self::as_bytes)
    }

    /// Tries to get the value as a char
    #[inline]
    #[must_use]
    pub fn as_char(&self) -> Option<char> {
        match self {
            Self::String(s) => s.chars().next(),
            _ => None,
        }
    }

    /// Tests if the value can be represented as a char
    #[inline]
    #[must_use]
    pub fn is_char(&self) -> bool {
        self.as_char().is_some()
    }

    /// Tries to get an element of an object as char, takes the first one if it
    /// is a string
    #[inline]
    #[must_use]
    pub fn get_char<Q: ?Sized>(&self, k: &Q) -> Option<char>
    where
        Cow<'value, str>: Borrow<Q> + Hash + Eq,
        Q: Hash + Eq + Ord,
    {
        self.get(k).and_then(Self::as_char)
    }
}

impl<'value> Builder<'value> for Value<'value> {
    #[inline]
    #[must_use]
    fn null() -> Self {
        Self::Static(StaticNode::Null)
    }
    #[inline]
    #[must_use]
    fn array_with_capacity(capacity: usize) -> Self {
        Self::Array(Vec::with_capacity(capacity))
    }
    #[inline]
    #[must_use]
    fn object_with_capacity(capacity: usize) -> Self {
        Self::Object(Box::new(Object::with_capacity(capacity)))
    }
}

impl<'value> Mutable for Value<'value> {
    #[inline]
    #[must_use]
    fn as_array_mut(&mut self) -> Option<&mut Vec<Value<'value>>> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }
    #[inline]
    #[must_use]
    fn as_object_mut(&mut self) -> Option<&mut HashMap<<Self as ValueAccess>::Key, Self>> {
        match self {
            Self::Object(m) => Some(m),
            _ => None,
        }
    }
}
impl<'value> ValueAccess for Value<'value> {
    type Target = Self;
    type Key = Cow<'value, str>;
    type Array = Vec<Self>;
    type Object = HashMap<Self::Key, Self>;

    #[inline]
    #[must_use]
    fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Static(StaticNode::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Static(s) => s.as_i64(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    fn as_i128(&self) -> Option<i128> {
        match self {
            Self::Static(s) => s.as_i128(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Static(s) => s.as_u64(),
            _ => None,
        }
    }

    #[cfg(feature = "128bit")]
    #[inline]
    #[must_use]
    #[allow(clippy::cast_sign_loss)]
    fn as_u128(&self) -> Option<u128> {
        match self {
            Self::Static(s) => s.as_u128(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Static(s) => s.as_f64(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    fn cast_f64(&self) -> Option<f64> {
        match self {
            Self::Static(s) => s.cast_f64(),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s.borrow()),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    fn as_array(&self) -> Option<&Vec<Value<'value>>> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    #[inline]
    #[must_use]
    fn as_object(&self) -> Option<&HashMap<Self::Key, Self>> {
        match self {
            Self::Object(m) => Some(m),
            _ => None,
        }
    }
}

impl<'value> ValueTrait for Value<'value> {
    #[inline]
    #[must_use]
    fn is_custom(&self) -> bool {
        matches!(self, Value::Bytes(_))
    }

    #[inline]
    #[must_use]
    fn value_type(&self) -> ValueType {
        match self {
            Self::Static(s) => s.value_type(),
            Self::String(_) => ValueType::String,
            Self::Array(_) => ValueType::Array,
            Self::Object(_) => ValueType::Object,
            Self::Bytes(_) => ValueType::Custom("bytes"),
        }
    }

    #[inline]
    #[must_use]
    fn is_null(&self) -> bool {
        matches!(self, Self::Static(StaticNode::Null))
    }
}

#[cfg(not(tarpaulin_include))]
impl<'value> fmt::Display for Value<'value> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Static(s) => write!(f, "{}", s),
            Self::String(s) => write!(f, "{}", s),
            Self::Array(a) => write!(f, "{:?}", a),
            Self::Object(o) => write!(f, "{:?}", o),
            Value::Bytes(b) => write!(f, "<<{:?}>>", b),
        }
    }
}

impl<'value> Index<&str> for Value<'value> {
    type Output = Value<'value>;
    #[inline]
    #[must_use]
    fn index(&self, index: &str) -> &Self::Output {
        // ALLOW: panicking in Index is expected behavior
        self.get(index).expect("index out of bounds")
    }
}

impl<'value> Index<usize> for Value<'value> {
    type Output = Value<'value>;
    #[inline]
    #[must_use]
    fn index(&self, index: usize) -> &Self::Output {
        // ALLOW: panicking in Index is expected behavior
        self.get_idx(index).expect("index out of bounds")
    }
}

impl<'value> IndexMut<&str> for Value<'value> {
    #[inline]
    #[must_use]
    fn index_mut(&mut self, index: &str) -> &mut Self::Output {
        // ALLOW: panicking in Index is expected behavior
        self.get_mut(index).expect("index out of bounds")
    }
}

impl<'value> IndexMut<usize> for Value<'value> {
    #[inline]
    #[must_use]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        // ALLOW: panicking in Index is expected behavior
        self.get_idx_mut(index).expect("index out of bounds")
    }
}

impl<'value> Default for Value<'value> {
    #[inline]
    #[must_use]
    fn default() -> Self {
        Self::Static(StaticNode::Null)
    }
}

struct ValueDeserializer<'de>(Deserializer<'de>);

impl<'de> ValueDeserializer<'de> {
    pub fn from_deserializer(de: Deserializer<'de>) -> Self {
        Self(de)
    }

    #[cfg_attr(not(feature = "no-inline"), inline(always))]
    pub fn parse(&mut self) -> Value<'de> {
        // We know the tape is valid JSON
        match unsafe { self.0.next_() } {
            Node::Static(s) => Value::Static(s),
            Node::String(s) => Value::from(s),
            Node::Array(len, _) => self.parse_array(len),
            Node::Object(len, _) => self.parse_map(len),
        }
    }

    #[cfg_attr(not(feature = "no-inline"), inline(always))]
    fn parse_array(&mut self, len: usize) -> Value<'de> {
        // Rust doesn't optimize the normal loop away here
        // so we write our own avoiding the length
        // checks during push
        let mut res = Vec::with_capacity(len);
        unsafe {
            res.set_len(len);
            for i in 0..len {
                std::ptr::write(res.get_unchecked_mut(i), self.parse());
            }
        }
        Value::Array(res)
    }

    #[cfg_attr(not(feature = "no-inline"), inline(always))]
    fn parse_map(&mut self, len: usize) -> Value<'de> {
        let mut res = Object::with_capacity(len);

        for _ in 0..len {
            // We know the tape is sane
            if let Node::String(key) = unsafe { self.0.next_() } {
                res.insert_nocheck(key.into(), self.parse());
            } else {
                // ALLOW: we guarantee this in the tape
                unreachable!()
            }
        }
        Value::from(res)
    }
}

#[cfg(test)]
mod test {
    #![allow(clippy::cognitive_complexity)]
    use super::*;
    use crate::literal;
    use proptest::proptest;

    #[test]
    fn obj_eq() {
        let o1: Value = literal!({"k": 1, "v":2});
        let o2: Value = literal!({"k": 1, "v":2});
        assert_eq!(o1, o2);
        assert_eq!(o2, o1);
        let o1: Value = literal!({"k": (), "v":()});
        let o2: Value = literal!({"k": (),  "":()});
        assert!(!(o1 == o2));
        assert!(!(o2 == o1));

        let v1: Value = literal!({"¢": (), "¡": (), "": (), "\u{0}": ()});
        let v2: Value = literal!({"¦": (), "¥": (), "£": (), "¤": ()});
        assert!(v1 != v2);
        assert!(v2 != v1);
    }

    #[test]
    fn as_char() {
        assert_eq!(Value::from("badger").as_char(), Some('b'));
        assert_eq!(Value::from(42).as_char(), None);

        assert!(Value::from("badger").is_char());
        assert!(!Value::from(42).is_char());
    }

    #[test]
    fn cmp_byte_string() {
        use std::cmp::Ordering;

        let b = Value::Bytes(b"snot"[..].into());
        assert_eq!(Value::from("snot").cmp(&b), Ordering::Equal);
        assert_eq!(b.cmp(&Value::from("snot")), Ordering::Equal);

        assert_eq!(Value::from("badger").cmp(&b), Ordering::Less);
        assert_eq!(b.cmp(&Value::from("badger")), Ordering::Greater);

        assert_eq!(Value::from("zoot").cmp(&b), Ordering::Greater);
        assert_eq!(b.cmp(&Value::from("zoot")), Ordering::Less);
    }

    #[test]
    fn nuber_ord_u64() {
        use std::cmp::Ordering;
        assert_eq!(Value::from(1u64).cmp(&Value::from(1u64)), Ordering::Equal);
        assert_eq!(Value::from(1u64).cmp(&Value::from(1i64)), Ordering::Equal);
        assert_eq!(Value::from(1u64).cmp(&Value::from(1f64)), Ordering::Equal);

        assert_eq!(Value::from(2u64).cmp(&Value::from(1u64)), Ordering::Greater);
        assert_eq!(Value::from(2u64).cmp(&Value::from(1i64)), Ordering::Greater);
        assert_eq!(Value::from(2u64).cmp(&Value::from(1f64)), Ordering::Greater);
        assert_eq!(
            Value::from(2u64).cmp(&Value::from(-1i64)),
            Ordering::Greater
        );
        assert_eq!(
            Value::from(2u64).cmp(&Value::from(-1f64)),
            Ordering::Greater
        );
        assert_eq!(Value::from(0u64).cmp(&Value::from(1u64)), Ordering::Less);
        assert_eq!(Value::from(0u64).cmp(&Value::from(1i64)), Ordering::Less);
        assert_eq!(Value::from(0u64).cmp(&Value::from(1f64)), Ordering::Less);
        assert_eq!(
            Value::from(1u64).cmp(&Value::from(1e100f64)),
            Ordering::Less
        );
    }

    #[test]
    fn nuber_ord_i64() {
        use std::cmp::Ordering;
        assert_eq!(Value::from(1i64).cmp(&Value::from(1u64)), Ordering::Equal);
        assert_eq!(Value::from(1i64).cmp(&Value::from(1i64)), Ordering::Equal);
        assert_eq!(Value::from(1i64).cmp(&Value::from(1f64)), Ordering::Equal);

        assert_eq!(Value::from(2i64).cmp(&Value::from(1u64)), Ordering::Greater);
        assert_eq!(Value::from(2i64).cmp(&Value::from(1i64)), Ordering::Greater);
        assert_eq!(Value::from(2i64).cmp(&Value::from(1f64)), Ordering::Greater);
        assert_eq!(
            Value::from(2i64).cmp(&Value::from(-1i64)),
            Ordering::Greater
        );
        assert_eq!(
            Value::from(2i64).cmp(&Value::from(-1f64)),
            Ordering::Greater
        );
        assert_eq!(
            Value::from(2i64).cmp(&Value::from(-1e100f64)),
            Ordering::Greater
        );
        assert_eq!(Value::from(0i64).cmp(&Value::from(1u64)), Ordering::Less);
        assert_eq!(Value::from(0i64).cmp(&Value::from(1i64)), Ordering::Less);
        assert_eq!(Value::from(0i64).cmp(&Value::from(1f64)), Ordering::Less);
        assert_eq!(
            Value::from(0i64).cmp(&Value::from(1e100f64)),
            Ordering::Less
        );

        assert_eq!(Value::from(-1i64).cmp(&Value::from(1u64)), Ordering::Less);
        assert_eq!(Value::from(-1i64).cmp(&Value::from(1i64)), Ordering::Less);
        assert_eq!(Value::from(-1i64).cmp(&Value::from(1f64)), Ordering::Less);
    }

    #[test]
    fn nuber_ord_f64() {
        use std::cmp::Ordering;
        assert_eq!(Value::from(1f64).cmp(&Value::from(1u64)), Ordering::Equal);
        assert_eq!(Value::from(1f64).cmp(&Value::from(1i64)), Ordering::Equal);
        assert_eq!(Value::from(1f64).cmp(&Value::from(1f64)), Ordering::Equal);

        assert_eq!(Value::from(2f64).cmp(&Value::from(1u64)), Ordering::Greater);
        assert_eq!(Value::from(2f64).cmp(&Value::from(1i64)), Ordering::Greater);
        assert_eq!(Value::from(2f64).cmp(&Value::from(1f64)), Ordering::Greater);

        assert_eq!(
            Value::from(2e100f64).cmp(&Value::from(1u64)),
            Ordering::Greater
        );
        assert_eq!(
            Value::from(2e100f64).cmp(&Value::from(1i64)),
            Ordering::Greater
        );
        assert_eq!(
            Value::from(2e100f64).cmp(&Value::from(1f64)),
            Ordering::Greater
        );

        assert_eq!(
            Value::from(2f64).cmp(&Value::from(-1i64)),
            Ordering::Greater
        );
        assert_eq!(
            Value::from(2f64).cmp(&Value::from(-1f64)),
            Ordering::Greater
        );
        assert_eq!(Value::from(0f64).cmp(&Value::from(1u64)), Ordering::Less);
        assert_eq!(Value::from(0f64).cmp(&Value::from(1i64)), Ordering::Less);
        assert_eq!(Value::from(0f64).cmp(&Value::from(1f64)), Ordering::Less);

        assert_eq!(Value::from(-1f64).cmp(&Value::from(1u64)), Ordering::Less);
        assert_eq!(Value::from(-1f64).cmp(&Value::from(1i64)), Ordering::Less);
        assert_eq!(Value::from(-1f64).cmp(&Value::from(1f64)), Ordering::Less);

        assert_eq!(
            Value::from(-1e100f64).cmp(&Value::from(1u64)),
            Ordering::Less
        );
        assert_eq!(
            Value::from(-1e100f64).cmp(&Value::from(1i64)),
            Ordering::Less
        );
        assert_eq!(
            Value::from(-1e100f64).cmp(&Value::from(1f64)),
            Ordering::Less
        );
    }

    #[test]
    fn is_custom() {
        assert!(Value::Bytes(vec![42u8].into()).is_custom());
        assert!(!Value::from(1).is_custom());
    }

    #[test]
    fn char_access() {
        let v = literal!({"snot": "🦡"});

        assert_eq!(Some('🦡'), v.get_char("snot"));
    }
    #[test]
    fn bytes_access() {
        let v = literal!({"snot": "🦡"});

        assert_eq!(Some("🦡".as_bytes()), v.get_bytes("snot"));
    }
    #[test]
    fn object_access() {
        let mut v = Value::null();
        assert_eq!(v.insert("key", ()), Err(AccessError::NotAnObject));
        assert_eq!(v.remove("key"), Err(AccessError::NotAnObject));
        let mut v = Value::object();
        assert_eq!(v.insert("key", 1), Ok(None));
        assert_eq!(v["key"], 1);
        assert_eq!(v.insert("key", 2), Ok(Some(Value::from(1))));
        v["key"] = 3.into();
        assert_eq!(v.remove("key"), Ok(Some(Value::from(3))));
    }

    #[test]
    fn array_access() {
        let mut v = Value::null();
        assert_eq!(v.push("key"), Err(AccessError::NotAnArray));
        assert_eq!(v.pop(), Err(AccessError::NotAnArray));
        let mut v = Value::array();
        assert_eq!(v.push(1), Ok(()));
        assert_eq!(v.push(2), Ok(()));
        assert_eq!(v[0], 1);
        v[0] = 0.into();
        v[1] = 1.into();
        assert_eq!(v.pop(), Ok(Some(Value::from(1))));
        assert_eq!(v.pop(), Ok(Some(Value::from(0))));
        assert_eq!(v.pop(), Ok(None));
    }

    #[cfg(feature = "128bit")]
    #[test]
    fn conversions_i128() {
        let v = Value::from(i128::max_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(!v.is_i64());
        assert!(!v.is_u64());
        assert!(!v.is_i32());
        assert!(!v.is_u32());
        assert!(!v.is_i16());
        assert!(!v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
        let v = Value::from(i128::min_value());
        assert!(v.is_i128());
        assert!(!v.is_u128());
        assert!(!v.is_i64());
        assert!(!v.is_u64());
        assert!(!v.is_i32());
        assert!(!v.is_u32());
        assert!(!v.is_i16());
        assert!(!v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_i64() {
        let v = Value::from(i64::max_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(!v.is_i32());
        assert!(!v.is_u32());
        assert!(!v.is_i16());
        assert!(!v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
        let v = Value::from(i64::min_value());
        assert!(v.is_i128());
        assert!(!v.is_u128());
        assert!(v.is_i64());
        assert!(!v.is_u64());
        assert!(!v.is_i32());
        assert!(!v.is_u32());
        assert!(!v.is_i16());
        assert!(!v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_i32() {
        let v = Value::from(i32::max_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(v.is_i32());
        assert!(v.is_u32());
        assert!(!v.is_i16());
        assert!(!v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
        let v = Value::from(i32::min_value());
        assert!(v.is_i128());
        assert!(!v.is_u128());
        assert!(v.is_i64());
        assert!(!v.is_u64());
        assert!(v.is_i32());
        assert!(!v.is_u32());
        assert!(!v.is_i16());
        assert!(!v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_i16() {
        let v = Value::from(i16::max_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(v.is_i32());
        assert!(v.is_u32());
        assert!(v.is_i16());
        assert!(v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
        let v = Value::from(i16::min_value());
        assert!(v.is_i128());
        assert!(!v.is_u128());
        assert!(v.is_i64());
        assert!(!v.is_u64());
        assert!(v.is_i32());
        assert!(!v.is_u32());
        assert!(v.is_i16());
        assert!(!v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_i8() {
        let v = Value::from(i8::max_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(v.is_i32());
        assert!(v.is_u32());
        assert!(v.is_i16());
        assert!(v.is_u16());
        assert!(v.is_i8());
        assert!(v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
        let v = Value::from(i8::min_value());
        assert!(v.is_i128());
        assert!(!v.is_u128());
        assert!(v.is_i64());
        assert!(!v.is_u64());
        assert!(v.is_i32());
        assert!(!v.is_u32());
        assert!(v.is_i16());
        assert!(!v.is_u16());
        assert!(v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_usize() {
        let v = Value::from(usize::min_value() as u64);
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(v.is_usize());
        assert!(v.is_i32());
        assert!(v.is_u32());
        assert!(v.is_i16());
        assert!(v.is_u16());
        assert!(v.is_i8());
        assert!(v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[cfg(feature = "128bit")]
    #[test]
    fn conversions_u128() {
        let v = Value::from(u128::min_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(v.is_i32());
        assert!(v.is_u32());
        assert!(v.is_i16());
        assert!(v.is_u16());
        assert!(v.is_i8());
        assert!(v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_u64() {
        let v = Value::from(u64::min_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(v.is_i32());
        assert!(v.is_u32());
        assert!(v.is_i16());
        assert!(v.is_u16());
        assert!(v.is_i8());
        assert!(v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_u32() {
        let v = Value::from(u32::max_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(!v.is_i32());
        assert!(v.is_u32());
        assert!(!v.is_i16());
        assert!(!v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_u16() {
        let v = Value::from(u16::max_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(v.is_i32());
        assert!(v.is_u32());
        assert!(!v.is_i16());
        assert!(v.is_u16());
        assert!(!v.is_i8());
        assert!(!v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_u8() {
        let v = Value::from(u8::max_value());
        assert!(v.is_i128());
        assert!(v.is_u128());
        assert!(v.is_i64());
        assert!(v.is_u64());
        assert!(v.is_i32());
        assert!(v.is_u32());
        assert!(v.is_i16());
        assert!(v.is_u16());
        assert!(!v.is_i8());
        assert!(v.is_u8());
        assert!(!v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_f64() {
        let v = Value::from(std::f64::MAX);
        assert!(!v.is_i64());
        assert!(!v.is_u64());
        assert!(v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
        let v = Value::from(std::f64::MIN);
        assert!(!v.is_i64());
        assert!(!v.is_u64());
        assert!(v.is_f64());
        assert!(!v.is_f32());
        assert!(v.is_f64_castable());
        let v = Value::from("not a f64");
        assert!(!v.is_f64_castable());
    }

    #[test]
    fn conversions_f32() {
        let v = Value::from(std::f32::MAX);
        assert!(!v.is_i64());
        assert!(!v.is_u64());
        assert!(v.is_f64());
        assert!(v.is_f32());
        assert!(v.is_f64_castable());
        let v = Value::from(std::f32::MIN);
        assert!(!v.is_i64());
        assert!(!v.is_u64());
        assert!(v.is_f64());
        assert!(v.is_f32());
        assert!(v.is_f64_castable());
    }

    #[test]
    fn conversions_array() {
        let v = Value::from(vec![true]);
        assert!(v.is_array());
        assert_eq!(v.value_type(), ValueType::Array);
        let v = Value::from("no array");
        assert!(!v.is_array());
    }

    #[test]
    fn conversions_bool() {
        let v = Value::from(true);
        assert!(v.is_bool());
        assert_eq!(v.value_type(), ValueType::Bool);
        let v = Value::from("no bool");
        assert!(!v.is_bool());
    }

    #[test]
    fn conversions_float() {
        let v = Value::from(42.0);
        assert!(v.is_f64());
        assert_eq!(v.value_type(), ValueType::F64);
        let v = Value::from("no float");
        assert!(!v.is_f64());
    }

    #[test]
    fn conversions_int() {
        let v = Value::from(-42);
        assert!(v.is_i64());
        assert_eq!(v.value_type(), ValueType::I64);
        #[cfg(feature = "128bit")]
        {
            let v = Value::from(-42_i128);
            assert!(v.is_i64());
            assert!(v.is_i128());
            assert_eq!(v.value_type(), ValueType::I128);
        }
        let v = Value::from("no i64");
        assert!(!v.is_i64());
        assert!(!v.is_i128());
    }

    #[test]
    fn conversions_uint() {
        let v = Value::from(42_u64);
        assert!(v.is_u64());
        assert_eq!(v.value_type(), ValueType::U64);
        #[cfg(feature = "128bit")]
        {
            let v = Value::from(42_u128);
            assert!(v.is_u64());
            assert!(v.is_u128());
            assert_eq!(v.value_type(), ValueType::U128);
        }
        let v = Value::from("no u64");
        assert!(!v.is_u64());
        #[cfg(feature = "128bit")]
        assert!(!v.is_u128());
    }

    #[test]
    fn conversions_null() {
        let v = Value::from(());
        assert!(v.is_null());
        assert_eq!(v.value_type(), ValueType::Null);
        let v = Value::from("no null");
        assert!(!v.is_null());
    }

    #[test]
    fn conversions_object() {
        let v = Value::from(Object::new());
        assert!(v.is_object());
        assert_eq!(v.value_type(), ValueType::Object);
        let v = Value::from("no object");
        assert!(!v.is_object());
    }

    #[test]
    fn conversions_str() {
        let v = Value::from("bla");
        assert!(v.is_str());
        assert_eq!(v.value_type(), ValueType::String);
        let v = Value::from(42);
        assert!(!v.is_str());
    }

    #[test]
    fn default() {
        assert_eq!(Value::default(), Value::null())
    }

    #[test]
    fn float_cmp() {
        use std::cmp::Ordering;
        assert_eq!(Value::from(1u64).cmp(&Value::from(1f64)), Ordering::Equal);
        assert_eq!(Value::from(1i64).cmp(&Value::from(1f64)), Ordering::Equal);
        assert_eq!(Value::from(1f64).cmp(&Value::from(1f64)), Ordering::Equal);
        assert_eq!(Value::from(1f64).cmp(&Value::from(1u64)), Ordering::Equal);
        assert_eq!(Value::from(1f64).cmp(&Value::from(1i64)), Ordering::Equal);
        assert_eq!(Value::from(1u64).cmp(&Value::from(2f64)), Ordering::Less);
        assert_eq!(Value::from(1i64).cmp(&Value::from(2f64)), Ordering::Less);
        assert_eq!(Value::from(1f64).cmp(&Value::from(2f64)), Ordering::Less);
        assert_eq!(Value::from(1f64).cmp(&Value::from(2u64)), Ordering::Less);
        assert_eq!(Value::from(1f64).cmp(&Value::from(2i64)), Ordering::Less);
        assert_eq!(Value::from(2u64).cmp(&Value::from(1f64)), Ordering::Greater);
        assert_eq!(Value::from(2i64).cmp(&Value::from(1f64)), Ordering::Greater);
        assert_eq!(Value::from(2f64).cmp(&Value::from(1f64)), Ordering::Greater);
        assert_eq!(Value::from(2f64).cmp(&Value::from(1u64)), Ordering::Greater);
        assert_eq!(Value::from(2f64).cmp(&Value::from(1i64)), Ordering::Greater);
    }

    use proptest::prelude::*;

    fn arb_tremor_value() -> BoxedStrategy<Value<'static>> {
        let leaf = prop_oneof![
            Just(Value::Static(StaticNode::Null)),
            any::<bool>()
                .prop_map(StaticNode::Bool)
                .prop_map(Value::Static),
            any::<i64>()
                .prop_map(StaticNode::I64)
                .prop_map(Value::Static),
            any::<u64>()
                .prop_map(StaticNode::U64)
                .prop_map(Value::Static),
            any::<Vec<u8>>().prop_map(Cow::from).prop_map(Value::Bytes),
            any::<f64>()
                .prop_map(StaticNode::F64)
                .prop_map(Value::Static),
            ".*".prop_map(Value::from),
        ];
        leaf.prop_recursive(
            8,   // 8 levels deep
            256, // Shoot for maximum size of 256 nodes
            10,  // We put up to 10 items per collection
            |inner| {
                prop_oneof![
                    // Take the inner strategy and make the two recursive cases.
                    prop::collection::vec(inner.clone(), 0..10).prop_map(Value::Array),
                    prop::collection::hash_map(".*".prop_map(Cow::from), inner, 0..10)
                        .prop_map(|m| m.into_iter().collect()),
                ]
            },
        )
        .boxed()
    }

    fn arb_value() -> BoxedStrategy<Value<'static>> {
        let leaf = prop_oneof![
            Just(Value::Static(StaticNode::Null)),
            any::<bool>()
                .prop_map(StaticNode::Bool)
                .prop_map(Value::Static),
            any::<i64>()
                .prop_map(StaticNode::I64)
                .prop_map(Value::Static),
            any::<u64>()
                .prop_map(StaticNode::U64)
                .prop_map(Value::Static),
            any::<f64>()
                .prop_map(StaticNode::F64)
                .prop_map(Value::Static),
            ".*".prop_map(Value::from),
        ];
        leaf.prop_recursive(
            8,   // 8 levels deep
            256, // Shoot for maximum size of 256 nodes
            10,  // We put up to 10 items per collection
            |inner| {
                prop_oneof![
                    // Take the inner strategy and make the two recursive cases.
                    prop::collection::vec(inner.clone(), 0..10).prop_map(Value::Array),
                    prop::collection::hash_map(".*".prop_map(Cow::from), inner, 0..10)
                        .prop_map(|m| m.into_iter().collect()),
                ]
            },
        )
        .boxed()
    }

    proptest! {
        #[test]
        fn prop_cmq(v1 in arb_tremor_value(), v2 in arb_tremor_value()) {
            if v1 > v2 {
               prop_assert!(v2 < v1);
            } else if v1 < v2 {
               prop_assert!(v2 > v1);
            } else if v1 == v2 {
                dbg!(v1 == v2, v2 == v1);
                prop_assert!(v2 == v1);
            } else {
                dbg!(v1 == v2, v2 == v1);
                dbg!(v1.cmp(&v2), v2.cmp(&v1));
                prop_assert!(false)
            };
        }

        #[test]
        fn prop_to_owned(borrowed in arb_value()) {
            use simd_json::OwnedValue;
            let owned: OwnedValue = borrowed.clone().into();
            prop_assert_eq!(borrowed, owned);
        }
        #[test]
        fn prop_into_static(borrowed in arb_tremor_value()) {
            let static_borrowed = borrowed.clone().into_static();
            assert_eq!(borrowed, static_borrowed);
        }
        #[test]
        fn prop_clone_static(borrowed in arb_value()) {
            let static_borrowed = borrowed.clone_static();
            assert_eq!(borrowed, static_borrowed);
        }
        #[test]
        fn prop_serialize_deserialize(borrowed in arb_value()) {
            let mut string = borrowed.encode();
            let mut bytes = unsafe{ string.as_bytes_mut()};
            let decoded = parse_to_value(&mut bytes).expect("Failed to decode");
            prop_assert_eq!(borrowed, decoded)
        }
        #[test]
        #[allow(clippy::float_cmp)]
        fn prop_f64_cmp(f in proptest::num::f64::NORMAL) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)

        }

        #[test]
        #[allow(clippy::float_cmp)]
        fn prop_f32_cmp(f in proptest::num::f32::NORMAL) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)

        }
        #[test]
        fn prop_i64_cmp(f in proptest::num::i64::ANY) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)
        }
        #[test]
        fn prop_i32_cmp(f in proptest::num::i32::ANY) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)
        }
        #[test]
        fn prop_i16_cmp(f in proptest::num::i16::ANY) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)
        }
        #[test]
        fn prop_i8_cmp(f in proptest::num::i8::ANY) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)
        }
        #[test]
        fn prop_u64_cmp(f in proptest::num::u64::ANY) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)
        }

        #[test]
        #[allow(clippy::cast_possible_truncation)]
        fn prop_usize_cmp(f in proptest::num::usize::ANY) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)
        }
         #[test]
        fn prop_u32_cmp(f in proptest::num::u32::ANY) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)
        }
        #[test]
        fn prop_u16_cmp(f in proptest::num::u16::ANY) {
            let v: Value = f.into();
            prop_assert_eq!(v, f)
        }
        #[test]
        fn prop_u8_cmp(f in proptest::num::u8::ANY) {
            let v: Value = f.into();
            assert_eq!(v, &f);
            prop_assert_eq!(v, f)
        }
        #[test]
        fn prop_string_cmp(f in ".*") {
            let v: Value = f.clone().into();
            prop_assert_eq!(v.clone(), f.as_str());
            prop_assert_eq!(v, f);
        }

    }
    #[test]
    fn test_union_cmp() {
        let v: Value = ().into();
        assert_eq!(v, ())
    }
    #[test]
    fn test_bool_cmp() {
        let v: Value = true.into();
        assert_eq!(v, true);
        let v: Value = false.into();
        assert_eq!(v, false);
    }
    #[test]
    fn test_slice_cmp() {
        use std::iter::FromIterator;
        let v: Value = Value::from_iter(vec!["a", "b"]);
        assert_eq!(v, &["a", "b"][..]);
    }
    #[test]
    fn test_hashmap_cmp() {
        use std::iter::FromIterator;
        let v: Value = Value::from_iter(vec![("a", 1)]);
        assert_eq!(
            v,
            vec![("a", 1)]
                .iter()
                .cloned()
                .collect::<std::collections::HashMap<&str, i32>>()
        );
    }

    #[test]
    fn test_option_from() {
        let v: Option<u8> = None;
        let v: Value = v.into();
        assert_eq!(v, ());
        let v: Option<u8> = Some(42);
        let v: Value = v.into();
        assert_eq!(v, 42);
    }
}
