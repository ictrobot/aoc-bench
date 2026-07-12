//! Durable identity of a reusable benchmark invocation (a "workload").
//!
//! A workload identity separates the *content artifacts* (executable and stdin, addressed by
//! SHA-256) from the *non-artifact invocation fields* (the [`GroupSpec`]: argv, checksum, resolved
//! statistics options, and semantics version). Combining them yields the `workload_sha256`, a
//! domain-separated, length-framed SHA-256 over the whole identity so that a later config generation
//! can find an older measurement by hashing, and any single differing field splits the workload.

use super::hash::Sha256;
use crate::config::{BenchmarkId, Config};
use crate::stats::StatsOptions;
use serde::{Deserialize, Serialize};

/// Combined runner-protocol + statistics-pipeline semantics version.
///
/// This is part of every workload identity: bumping it splits all durable workloads, which is the
/// intended escape hatch for "the meaning of a measurement changed". It is deliberately distinct
/// from the on-the-wire `version=1` protocol field (which only guards line parsing).
pub const SEMANTICS_VERSION: u32 = 1;

/// Domain separator for the shared (content-backed) workload hash.
const DOMAIN_SHARED: &[u8] = b"aoc-bench/workload/shared/v1";
/// Domain separator for the isolated (case-specific) workload hash.
const DOMAIN_ISOLATED: &[u8] = b"aoc-bench/workload/isolated/v1";

/// The non-artifact invocation fields of a workload.
///
/// Holds everything that affects the measured work *except* the executable and stdin content, which
/// are identified separately by their artifact SHA-256s. Deliberately excludes commit/build labels,
/// the full logical config, host name, dry-run/force flags, and ambient host execution policy.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupSpec {
    /// Fully expanded `argv[1..]`, preserving argument boundaries.
    pub argv: Vec<String>,
    /// Expected checksum, with explicit absence (`None`) distinct from present.
    pub checksum: Option<String>,
    /// Every resolved statistics option.
    pub stats: StatsOptions,
    /// Combined runner-protocol + statistics semantics version.
    pub semantics_version: u32,
}

impl GroupSpec {
    /// Construct a group spec with the current semantics version.
    #[must_use]
    pub fn new(argv: Vec<String>, checksum: Option<String>, stats: StatsOptions) -> Self {
        Self {
            argv,
            checksum,
            stats,
            semantics_version: SEMANTICS_VERSION,
        }
    }

    /// A stable digest over this group spec's canonical encoding, for use as a process-local
    /// grouping key. Domain-separated so it never collides with a full workload hash.
    #[must_use]
    pub fn digest(&self) -> Sha256 {
        let mut framer = Framer::with_domain(b"aoc-bench/group-spec/v1");
        self.encode(&mut framer);
        framer.finish()
    }

    /// Append this group spec's canonical, length-framed binary encoding to `out`.
    fn encode(&self, out: &mut Framer) {
        // Destructure every field so additions fail to compile until the identity encoding handles
        // them.
        let Self {
            argv,
            checksum,
            stats,
            semantics_version,
        } = self;

        out.u64(argv.len() as u64);
        for arg in argv {
            out.bytes(arg.as_bytes());
        }
        out.opt_bytes(checksum.as_deref().map(str::as_bytes));

        // Destructure every field so additions fail to compile until the identity encoding handles
        // them.
        let StatsOptions {
            min_samples,
            min_total_time_ns,
            target_rel_ci,
            skip_warmup,
            min_warmup_samples,
            min_warmup_time_ns,
            runs_per_series,
            run_timeout_ns,
        } = *stats;
        out.u64(min_samples.get() as u64);
        out.u64(min_total_time_ns.get());
        out.u64(target_rel_ci.to_bits());
        out.u8(u8::from(skip_warmup));
        out.u64(min_warmup_samples.get() as u64);
        out.u64(min_warmup_time_ns.get());
        out.u64(runs_per_series.get() as u64);
        out.u64(run_timeout_ns.get());

        out.u32(*semantics_version);
    }
}

/// The durable identity of a workload, ready to store or look up.
///
/// Construct via [`WorkloadIdentity::shared`] for an content-backed (shared) workload or
/// [`WorkloadIdentity::isolated`] for a case-specific (isolated) workload. The identity
/// kind is derived from `executable_sha256` nullability, never stored separately.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadIdentity {
    pub benchmark: BenchmarkId,
    pub workload_sha256: Sha256,
    pub executable_sha256: Option<Sha256>,
    pub stdin_sha256: Option<Sha256>,
    /// Canonical JSON stored alongside so a hash conflict is resolved by comparing full identity.
    pub group_spec_json: String,
}

impl WorkloadIdentity {
    /// Identity of a shared, content-backed workload.
    ///
    /// `stdin` is `None` when the benchmark has no stdin; that is distinct from an empty stdin file,
    /// which hashes to the empty digest.
    #[must_use]
    pub fn shared(
        benchmark: BenchmarkId,
        executable: Sha256,
        stdin: Option<Sha256>,
        group_spec: &GroupSpec,
    ) -> Self {
        let mut framer = Framer::with_domain(DOMAIN_SHARED);
        framer.bytes(benchmark.as_str().as_bytes());
        framer.opt_sha(Some(executable));
        framer.opt_sha(stdin);
        let mut group_encoded = Framer::new();
        group_spec.encode(&mut group_encoded);
        framer.bytes(&group_encoded.0);

        Self {
            benchmark,
            workload_sha256: framer.finish(),
            executable_sha256: Some(executable),
            stdin_sha256: stdin,
            group_spec_json: serde_json::to_string(group_spec)
                .expect("GroupSpec serializes to JSON"),
        }
    }

    /// Identity of an isolated, case-specific workload derived from benchmark and canonical config.
    ///
    /// Used for isolated benchmarks and v1 migration. Both content digests are `None`.
    #[must_use]
    pub fn isolated(benchmark: BenchmarkId, config: &Config) -> Self {
        let config_json =
            serde_json::to_string(config).expect("hostless Config serializes to JSON");
        Self::isolated_from_json(benchmark, &config_json)
    }

    /// Identity of an isolated workload from a raw canonical config JSON string.
    ///
    /// Used by v1 migration, where the stored config JSON is taken verbatim rather than
    /// reconstructed through today's `ConfigFile`.
    #[must_use]
    pub fn isolated_from_json(benchmark: BenchmarkId, config_json: &str) -> Self {
        let mut framer = Framer::with_domain(DOMAIN_ISOLATED);
        framer.bytes(benchmark.as_str().as_bytes());
        framer.bytes(config_json.as_bytes());

        Self {
            benchmark,
            workload_sha256: framer.finish(),
            executable_sha256: None,
            stdin_sha256: None,
            group_spec_json: config_json.to_string(),
        }
    }

    /// Whether this is a shared (content-backed) workload rather than an isolated one.
    #[must_use]
    pub fn is_shared(&self) -> bool {
        self.executable_sha256.is_some()
    }
}

/// A length-framing binary encoder used to build domain-separated hash preimages.
///
/// Every variable-length field is prefixed with its `u64` little-endian length so that no two
/// distinct field sequences can produce the same byte stream.
struct Framer(Vec<u8>);

impl Framer {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn with_domain(domain: &[u8]) -> Self {
        let mut framer = Self::new();
        framer.bytes(domain);
        framer
    }

    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.u64(value.len() as u64);
        self.0.extend_from_slice(value);
    }

    fn opt_bytes(&mut self, value: Option<&[u8]>) {
        match value {
            Some(bytes) => {
                self.u8(1);
                self.bytes(bytes);
            }
            None => self.u8(0),
        }
    }

    fn opt_sha(&mut self, value: Option<Sha256>) {
        match value {
            Some(sha) => {
                self.u8(1);
                self.0.extend_from_slice(sha.as_bytes());
            }
            None => self.u8(0),
        }
    }

    fn finish(&self) -> Sha256 {
        Sha256::hash_bytes(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigFile;
    use std::num::{NonZeroU64, NonZeroUsize};
    use tempfile::TempDir;

    fn digest(byte: u8) -> Sha256 {
        Sha256::hash_bytes(&[byte])
    }

    fn group_spec() -> GroupSpec {
        GroupSpec::new(
            vec!["bench".into(), "1".into()],
            Some("abc123".into()),
            StatsOptions::default(),
        )
    }

    fn bench(name: &str) -> BenchmarkId {
        name.try_into().unwrap()
    }

    #[test]
    fn shared_identity_is_deterministic() {
        let a = WorkloadIdentity::shared(bench("b"), digest(1), Some(digest(2)), &group_spec());
        let b = WorkloadIdentity::shared(bench("b"), digest(1), Some(digest(2)), &group_spec());
        assert_eq!(a.workload_sha256, b.workload_sha256);
        assert!(a.is_shared());
        assert_eq!(a.executable_sha256, Some(digest(1)));
        assert_eq!(a.stdin_sha256, Some(digest(2)));
    }

    #[test]
    fn changing_any_component_splits_workload() {
        let base = WorkloadIdentity::shared(bench("b"), digest(1), Some(digest(2)), &group_spec());

        // Different benchmark id.
        let diff_bench =
            WorkloadIdentity::shared(bench("c"), digest(1), Some(digest(2)), &group_spec());
        assert_ne!(base.workload_sha256, diff_bench.workload_sha256);

        // Different executable content.
        let diff_exe =
            WorkloadIdentity::shared(bench("b"), digest(9), Some(digest(2)), &group_spec());
        assert_ne!(base.workload_sha256, diff_exe.workload_sha256);

        // Different stdin content.
        let diff_stdin =
            WorkloadIdentity::shared(bench("b"), digest(1), Some(digest(9)), &group_spec());
        assert_ne!(base.workload_sha256, diff_stdin.workload_sha256);

        // Different argv.
        let mut gs = group_spec();
        gs.argv.push("extra".into());
        let diff_argv = WorkloadIdentity::shared(bench("b"), digest(1), Some(digest(2)), &gs);
        assert_ne!(base.workload_sha256, diff_argv.workload_sha256);

        // Different checksum.
        let mut gs = group_spec();
        gs.checksum = Some("different".into());
        let diff_ck = WorkloadIdentity::shared(bench("b"), digest(1), Some(digest(2)), &gs);
        assert_ne!(base.workload_sha256, diff_ck.workload_sha256);

        // Different stats.
        let mut gs = group_spec();
        gs.stats.min_samples = NonZeroUsize::new(999).unwrap();
        let diff_stats = WorkloadIdentity::shared(bench("b"), digest(1), Some(digest(2)), &gs);
        assert_ne!(base.workload_sha256, diff_stats.workload_sha256);

        // Different semantics version.
        let mut gs = group_spec();
        gs.semantics_version = SEMANTICS_VERSION + 1;
        let diff_sem = WorkloadIdentity::shared(bench("b"), digest(1), Some(digest(2)), &gs);
        assert_ne!(base.workload_sha256, diff_sem.workload_sha256);
    }

    #[test]
    fn absent_stdin_differs_from_empty_stdin() {
        let absent = WorkloadIdentity::shared(bench("b"), digest(1), None, &group_spec());
        let empty_digest = Sha256::hash_bytes(&[]);
        let empty =
            WorkloadIdentity::shared(bench("b"), digest(1), Some(empty_digest), &group_spec());
        assert_ne!(absent.workload_sha256, empty.workload_sha256);
        assert_eq!(absent.stdin_sha256, None);
        assert_eq!(empty.stdin_sha256, Some(empty_digest));
    }

    #[test]
    fn field_boundaries_do_not_collide() {
        // argv ["a","bc"] must not hash the same as ["ab","c"] thanks to length framing.
        let gs1 = GroupSpec::new(vec!["a".into(), "bc".into()], None, StatsOptions::default());
        let gs2 = GroupSpec::new(vec!["ab".into(), "c".into()], None, StatsOptions::default());
        let w1 = WorkloadIdentity::shared(bench("b"), digest(1), None, &gs1);
        let w2 = WorkloadIdentity::shared(bench("b"), digest(1), None, &gs2);
        assert_ne!(w1.workload_sha256, w2.workload_sha256);
    }

    #[test]
    fn shared_and_isolated_never_collide() {
        let dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": { "build": { "values": ["opt"] } },
            "benchmarks": [
                { "benchmark": "b", "command": ["cmd", "{build}"], "config": { "build": ["opt"] } }
            ]
        }"#;
        let cf = ConfigFile::from_str(dir.path(), None, json).unwrap();
        let config = cf.config_from_string("build=opt").unwrap();

        let isolated = WorkloadIdentity::isolated(bench("b"), &config);
        assert!(!isolated.is_shared());
        assert_eq!(isolated.executable_sha256, None);
        assert_eq!(isolated.stdin_sha256, None);

        let shared = WorkloadIdentity::shared(bench("b"), digest(1), None, &group_spec());
        assert_ne!(isolated.workload_sha256, shared.workload_sha256);

        // Isolated identity is deterministic and per-config.
        let isolated2 = WorkloadIdentity::isolated(bench("b"), &config);
        assert_eq!(isolated.workload_sha256, isolated2.workload_sha256);

        let config_native = cf.config_from_string("build=opt").unwrap();
        let same = WorkloadIdentity::isolated(bench("b"), &config_native);
        assert_eq!(isolated.workload_sha256, same.workload_sha256);
    }

    #[test]
    fn group_spec_json_round_trips() {
        let gs = group_spec();
        let json = serde_json::to_string(&gs).unwrap();
        let back: GroupSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(gs, back);
        let _ = NonZeroU64::new(1); // keep import used across cfgs
    }
}
