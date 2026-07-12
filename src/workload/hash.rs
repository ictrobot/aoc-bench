//! Content-addressing primitive: a fixed 32-byte SHA-256 digest with a lowercase-hex text form.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256 as Sha256Hasher};
use std::fmt::{self, Debug, Display, Formatter};
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::str::FromStr;

/// A SHA-256 digest.
///
/// The in-memory form is the raw 32 bytes; the text form (via [`Display`]/[`FromStr`] and serde)
/// is the 64-character lowercase hex encoding, matching the on-disk `builds/by-hash/<sha256>` and
/// `inputs/<sha256>` content-addressed layout.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Sha256([u8; 32]);

impl Sha256 {
    /// Length of the lowercase-hex text form.
    pub const HEX_LEN: usize = 64;

    /// Hash an in-memory byte slice.
    #[must_use]
    pub fn hash_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Sha256Hasher::new();
        hasher.update(bytes);
        Self(hasher.finalize().into())
    }

    /// Hash the full contents of a file.
    ///
    /// The file is streamed so large executables are not held in memory.
    pub fn hash_file(path: &Path) -> io::Result<Self> {
        Self::hash_reader(File::open(path)?)
    }

    /// Hash all bytes read from a stream.
    pub fn hash_reader(mut reader: impl Read) -> io::Result<Self> {
        let mut hasher = Sha256Hasher::new();
        let mut buf = [0u8; 8 * 1024];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(Self(hasher.finalize().into()))
    }

    /// The raw digest bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Display for Sha256 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Debug for Sha256 {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Sha256({self})")
    }
}

/// Error parsing a [`Sha256`] from hex text.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("invalid sha256 hex '{0}': expected 64 lowercase hex characters")]
pub struct Sha256ParseError(String);

impl FromStr for Sha256 {
    type Err = Sha256ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != Self::HEX_LEN {
            return Err(Sha256ParseError(s.to_string()));
        }
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            let hi = hex_val(s.as_bytes()[i * 2]).ok_or_else(|| Sha256ParseError(s.to_string()))?;
            let lo =
                hex_val(s.as_bytes()[i * 2 + 1]).ok_or_else(|| Sha256ParseError(s.to_string()))?;
            *byte = (hi << 4) | lo;
        }
        Ok(Self(bytes))
    }
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

impl Serialize for Sha256 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Sha256 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_digest_matches_known_value() {
        // Well-known SHA-256 of the empty input.
        assert_eq!(
            Sha256::hash_bytes(&[]).to_string(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hex_round_trips() {
        let digest = Sha256::hash_bytes(b"hello world");
        let hex = digest.to_string();
        assert_eq!(hex.len(), Sha256::HEX_LEN);
        assert_eq!(Sha256::from_str(&hex).unwrap(), digest);
    }

    #[test]
    fn rejects_bad_hex() {
        assert!(Sha256::from_str("nope").is_err());
        assert!(Sha256::from_str(&"z".repeat(64)).is_err());
        assert!(Sha256::from_str(&"a".repeat(63)).is_err());
        // Uppercase is not the canonical form and is rejected.
        assert!(Sha256::from_str(&"A".repeat(64)).is_err());
    }

    #[test]
    fn hash_file_matches_bytes() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("blob");
        let data = b"some benchmark executable bytes";
        std::fs::write(&path, data).unwrap();

        assert_eq!(Sha256::hash_file(&path).unwrap(), Sha256::hash_bytes(data));
    }

    #[test]
    fn serde_is_hex_string() {
        let digest = Sha256::hash_bytes(b"x");
        let json = serde_json::to_string(&digest).unwrap();
        assert_eq!(json, format!("\"{digest}\""));
        let back: Sha256 = serde_json::from_str(&json).unwrap();
        assert_eq!(back, digest);
    }
}
