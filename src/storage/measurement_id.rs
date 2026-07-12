//! Opaque, time-sortable measurement identifiers.
//!
//! Measurements use `UUIDv7` (a 48-bit millisecond timestamp prefix plus a tail) so that
//! `(case_id, measurement_id)` yields ordered history and the tail can shard JSON files. New
//! measurements use a random tail. Migrated v1 measurements use their recorded whole-second
//! timestamp and a stable tail derived from `(benchmark, canonical config)`.
//!
//! Backed by [`uuid::Uuid`] for correct version/variant handling. Stored in SQLite as a 16-byte
//! BLOB (via [`ToSql`]/[`FromSql`]) and serialized to JSON as the canonical hyphenated string.

use crate::config::BenchmarkId;
use crate::workload::Sha256;
use jiff::Timestamp;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use uuid::{Builder, Uuid};

/// A 128-bit measurement identifier.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MeasurementId(Uuid);

impl MeasurementId {
    /// Generate a fresh `UUIDv7` from the current time and a random tail.
    #[must_use]
    pub fn new_v7() -> Self {
        Self(Uuid::now_v7())
    }

    /// Derive a deterministic `UUIDv7` id for a migrated v1 run series.
    ///
    /// `canonical_config` is the normalized hostless `key=value,...` representation. The v7
    /// timestamp prefix carries `timestamp`; the stable hash tail distinguishes cases recorded in
    /// the same second.
    #[must_use]
    pub fn for_v1(benchmark: &BenchmarkId, canonical_config: &str, timestamp: Timestamp) -> Self {
        Self::from_stable_v7(benchmark, canonical_config, timestamp)
    }

    fn from_stable_v7(
        benchmark: &BenchmarkId,
        config_identity: &str,
        timestamp: Timestamp,
    ) -> Self {
        // The NUL frames benchmark and config unambiguously under their validated grammars.
        let mut preimage = Vec::with_capacity(benchmark.as_str().len() + config_identity.len() + 1);
        preimage.extend_from_slice(benchmark.as_str().as_bytes());
        preimage.push(0);
        preimage.extend_from_slice(config_identity.as_bytes());
        let digest = Sha256::hash_bytes(&preimage);

        let seconds = u64::try_from(timestamp.as_second())
            .expect("measurement timestamps must not precede the Unix epoch");
        let millis = seconds
            .checked_mul(1_000)
            .expect("measurement timestamp milliseconds overflow u64");
        assert!(
            millis < (1_u64 << 48),
            "measurement timestamp exceeds the UUIDv7 range"
        );
        let tail: &[u8; 10] = digest.as_bytes()[..10]
            .try_into()
            .expect("SHA-256 has at least ten bytes");
        Self(Builder::from_unix_timestamp_millis(millis, tail).into_uuid())
    }

    /// The two 2-hex shard components derived from the low two bytes.
    ///
    /// The high timestamp bytes are deliberately not used, since they cluster by time.
    #[must_use]
    pub fn shard(&self) -> (String, String) {
        let bytes = self.0.as_bytes();
        (format!("{:02x}", bytes[15]), format!("{:02x}", bytes[14]))
    }

    /// The raw 16 bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }

    #[cfg(test)]
    fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }
}

impl Display for MeasurementId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0.hyphenated(), f)
    }
}

impl fmt::Debug for MeasurementId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "MeasurementId({self})")
    }
}

/// Error parsing a [`MeasurementId`] from text.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid measurement id '{0}'")]
pub struct MeasurementIdParseError(String);

impl FromStr for MeasurementId {
    type Err = MeasurementIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Uuid::try_parse(s)
            .map(Self)
            .map_err(|_| MeasurementIdParseError(s.to_string()))
    }
}

impl Serialize for MeasurementId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for MeasurementId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

impl ToSql for MeasurementId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Blob(self.0.as_bytes())))
    }
}

impl FromSql for MeasurementId {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let blob = value.as_blob()?;
        Uuid::from_slice(blob)
            .map(Self)
            .map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigFile;
    use tempfile::TempDir;

    #[test]
    fn v7_is_version_7_and_variant_rfc() {
        let id = MeasurementId::new_v7();
        assert_eq!(id.0.get_version_num(), 7);
        assert_eq!(id.0.get_variant(), uuid::Variant::RFC4122);
    }

    #[test]
    fn v7_string_round_trips() {
        let id = MeasurementId::new_v7();
        let s = id.to_string();
        assert_eq!(s.len(), 36); // 32 hex + 4 hyphens
        assert_eq!(MeasurementId::from_str(&s).unwrap(), id);
    }

    #[test]
    fn v7_is_time_sortable() {
        // v7 orders lexicographically on the 48-bit timestamp prefix, regardless of the tail.
        let mut earlier = [0xffu8; 16];
        earlier[0..6].copy_from_slice(&1000u64.to_be_bytes()[2..8]);
        let mut later = [0x00u8; 16];
        later[0..6].copy_from_slice(&2000u64.to_be_bytes()[2..8]);
        assert!(MeasurementId::from_bytes(earlier) < MeasurementId::from_bytes(later));
    }

    #[test]
    fn v1_is_deterministic_and_v7() {
        let dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": { "build": { "values": ["opt"] } },
            "benchmarks": [
                { "benchmark": "b", "command": ["cmd", "{build}"], "config": { "build": ["opt"] } }
            ]
        }"#;
        let cf = ConfigFile::from_str(dir.path(), None, json).unwrap();
        let config = cf.config_from_string("build=opt").unwrap();
        let canonical_config = config.to_string();
        let bench: BenchmarkId = "b".try_into().unwrap();
        let ts = Timestamp::from_second(1_700_000_000).unwrap();

        let a = MeasurementId::for_v1(&bench, &canonical_config, ts);
        let b = MeasurementId::for_v1(&bench, &canonical_config, ts);
        assert_eq!(a, b);
        assert_eq!(a.0.get_version_num(), 7);
        assert_eq!(a.0.get_timestamp().unwrap().to_unix(), (1_700_000_000, 0));

        let other_config = MeasurementId::for_v1(&bench, "build=other", ts);
        assert_ne!(a, other_config);

        // A different timestamp changes only the timestamp-bearing portion of the id.
        let c = MeasurementId::for_v1(
            &bench,
            &canonical_config,
            Timestamp::from_second(1).unwrap(),
        );
        assert_ne!(a, c);
        assert_eq!(&a.as_bytes()[6..], &c.as_bytes()[6..]);
    }

    #[test]
    fn shard_uses_low_bytes() {
        let id = MeasurementId::new_v7();
        let (h1, h2) = id.shard();
        assert_eq!(h1, format!("{:02x}", id.as_bytes()[15]));
        assert_eq!(h2, format!("{:02x}", id.as_bytes()[14]));
    }

    #[test]
    fn serde_is_hyphenated_string() {
        let id = MeasurementId::new_v7();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{id}\""));
        let back: MeasurementId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }
}
