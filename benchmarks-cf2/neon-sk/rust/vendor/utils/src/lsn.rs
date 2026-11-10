#![warn(missing_docs)]

use std::fmt;
use std::ops::{Add, AddAssign};
use std::str::FromStr;

use serde::de::Visitor;
use serde::{Deserialize, Serialize};


/// Transaction log block size in bytes
pub const XLOG_BLCKSZ: u32 = 8192;

/// A Postgres LSN (Log Sequence Number), also known as an XLogRecPtr
#[derive(Clone, Copy, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct Lsn(pub u64);

impl Serialize for Lsn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.collect_str(self)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Lsn {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct LsnVisitor {
            is_human_readable_deserializer: bool,
        }

        impl Visitor<'_> for LsnVisitor {
            type Value = Lsn;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                if self.is_human_readable_deserializer {
                    formatter.write_str(
                        "value in form of hex string({upper_u32_hex}/{lower_u32_hex}) representing u64 integer",
                    )
                } else {
                    formatter.write_str("value in form of integer(u64)")
                }
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Lsn(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Lsn::from_str(v).map_err(|e| E::custom(e))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(LsnVisitor {
                is_human_readable_deserializer: true,
            })
        } else {
            deserializer.deserialize_u64(LsnVisitor {
                is_human_readable_deserializer: false,
            })
        }
    }
}

/// Allows (de)serialization of an `Lsn` always as `u64`.
///
/// ### Example
///
/// ```rust
/// # use serde::{Serialize, Deserialize};
/// use utils::lsn::Lsn;
///
/// #[derive(PartialEq, Serialize, Deserialize, Debug)]
/// struct Foo {
///   #[serde(with = "utils::lsn::serde_as_u64")]
///   always_u64: Lsn,
/// }
///
/// let orig = Foo { always_u64: Lsn(1234) };
///
/// let res = serde_json::to_string(&orig).unwrap();
/// assert_eq!(res, r#"{"always_u64":1234}"#);
///
/// let foo = serde_json::from_str::<Foo>(&res).unwrap();
/// assert_eq!(foo, orig);
/// ```
///
pub mod serde_as_u64 {
    use super::Lsn;

    /// Serializes the Lsn as u64 disregarding the human readability of the format.
    ///
    /// Meant to be used via `#[serde(with = "...")]` or `#[serde(serialize_with = "...")]`.
    pub fn serialize<S: serde::Serializer>(lsn: &Lsn, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::Serialize;
        lsn.0.serialize(serializer)
    }

    /// Deserializes the Lsn as u64 disregarding the human readability of the format.
    ///
    /// Meant to be used via `#[serde(with = "...")]` or `#[serde(deserialize_with = "...")]`.
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Lsn, D::Error> {
        use serde::Deserialize;
        u64::deserialize(deserializer).map(Lsn)
    }
}

/// We tried to parse an LSN from a string, but failed
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("LsnParseError")]
pub struct LsnParseError;

impl Lsn {
    /// Maximum possible value for an LSN
    pub const MAX: Lsn = Lsn(u64::MAX);

    /// Invalid value for InvalidXLogRecPtr, as defined in xlogdefs.h
    pub const INVALID: Lsn = Lsn(0);

    /// Subtract a number, returning None on overflow.
    pub fn checked_sub<T: Into<u64>>(self, other: T) -> Option<Lsn> {
        let other: u64 = other.into();
        self.0.checked_sub(other).map(Lsn)
    }

    /// Subtract a number, saturating at numeric bounds instead of overflowing.
    pub fn saturating_sub<T: Into<u64>>(self, other: T) -> Lsn {
        Lsn(self.0.saturating_sub(other.into()))
    }

    /// Subtract a number, returning the difference as i128 to avoid overflow.
    pub fn widening_sub<T: Into<u64>>(self, other: T) -> i128 {
        let other: u64 = other.into();
        i128::from(self.0) - i128::from(other)
    }

    /// Parse an LSN from a string in the form `0000000000000000`
    pub fn from_hex<S>(s: S) -> Result<Self, LsnParseError>
    where
        S: AsRef<str>,
    {
        let s: &str = s.as_ref();
        let n = u64::from_str_radix(s, 16).or(Err(LsnParseError))?;
        Ok(Lsn(n))
    }

    /// Compute the offset into a segment
    #[inline]
    pub fn segment_offset(self, seg_sz: usize) -> usize {
        (self.0 % seg_sz as u64) as usize
    }

    /// Compute LSN of the segment start.
    #[inline]
    pub fn segment_lsn(self, seg_sz: usize) -> Lsn {
        Lsn(self.0 - (self.0 % seg_sz as u64))
    }

    /// Compute the segment number
    #[inline]
    pub fn segment_number(self, seg_sz: usize) -> u64 {
        self.0 / seg_sz as u64
    }

    /// Compute the offset into a block
    #[inline]
    pub fn block_offset(self) -> u64 {
        const BLCKSZ: u64 = XLOG_BLCKSZ as u64;
        self.0 % BLCKSZ
    }

    /// Compute the block offset of the first byte of this Lsn within this
    /// segment
    #[inline]
    pub fn page_lsn(self) -> Lsn {
        Lsn(self.0 - self.block_offset())
    }

    /// Compute the block offset of the first byte of this Lsn within this
    /// segment
    #[inline]
    pub fn page_offset_in_segment(self, seg_sz: usize) -> u64 {
        (self.0 - self.block_offset()) - self.segment_lsn(seg_sz).0
    }

    /// Compute the bytes remaining in this block
    ///
    /// If the LSN is already at the block boundary, it will return `XLOG_BLCKSZ`.
    #[inline]
    pub fn remaining_in_block(self) -> u64 {
        const BLCKSZ: u64 = XLOG_BLCKSZ as u64;
        BLCKSZ - (self.0 % BLCKSZ)
    }

    /// Compute the bytes remaining to fill a chunk of some size
    ///
    /// If the LSN is already at the chunk boundary, it will return 0.
    pub fn calc_padding<T: Into<u64>>(self, sz: T) -> u64 {
        let sz: u64 = sz.into();
        // By using wrapping_sub, we can subtract first and then mod second.
        // If it's done the other way around, then we would return a full
        // chunk size if we're already at the chunk boundary.
        // (Regular subtraction will panic on overflow in debug builds.)
        (sz.wrapping_sub(self.0)) % sz
    }

    /// Align LSN on 8-byte boundary (alignment of WAL records).
    pub fn align(&self) -> Lsn {
        Lsn((self.0 + 7) & !7)
    }

    /// Align LSN on 8-byte boundary (alignment of WAL records).
    pub fn is_aligned(&self) -> bool {
        *self == self.align()
    }

    /// Return if the LSN is valid
    /// mimics postgres XLogRecPtrIsInvalid macro
    pub fn is_valid(self) -> bool {
        self != Lsn::INVALID
    }
}

impl From<u64> for Lsn {
    fn from(n: u64) -> Self {
        Lsn(n)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> u64 {
        lsn.0
    }
}

impl FromStr for Lsn {
    type Err = LsnParseError;

    /// Parse an LSN from a string in the form `00000000/00000000`
    ///
    /// If the input string is missing the '/' character, then use `Lsn::from_hex`
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splitter = s.trim().split('/');
        if let (Some(left), Some(right), None) = (splitter.next(), splitter.next(), splitter.next())
        {
            let left_num = u32::from_str_radix(left, 16).map_err(|_| LsnParseError)?;
            let right_num = u32::from_str_radix(right, 16).map_err(|_| LsnParseError)?;
            Ok(Lsn(((left_num as u64) << 32) | right_num as u64))
        } else {
            Err(LsnParseError)
        }
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffffffff)
    }
}

impl Add<u64> for Lsn {
    type Output = Lsn;

    fn add(self, other: u64) -> Self::Output {
        // panic if the addition overflows.
        Lsn(self.0.checked_add(other).unwrap())
    }
}

impl AddAssign<u64> for Lsn {
    fn add_assign(&mut self, other: u64) {
        // panic if the addition overflows.
        self.0 = self.0.checked_add(other).unwrap();
    }
}

