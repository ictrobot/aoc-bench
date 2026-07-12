//! Durable benchmark workload identity.
//!
//! A workload identifies measured work independently of logical configs and transient inode
//! groups. Shared workloads combine executable/stdin content digests with a [`GroupSpec`]; isolated
//! workloads use their canonical case config.

mod hash;
mod identity;

pub use hash::{Sha256, Sha256ParseError};
pub use identity::{GroupSpec, SEMANTICS_VERSION, WorkloadIdentity};
