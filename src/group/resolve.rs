//! Runtime resolution and cheap `(device, inode)` grouping of a shared benchmark's cases.
//!
//! Resolution turns a `(benchmark variant, config)` case into a lightweight [`ResolvedCase`]: it
//! expands the command template, resolves the executable and stdin paths, and snapshots their
//! transient file identities. Grouping then buckets resolved cases that currently name the same
//! executable and stdin files under an identical [`GroupSpec`], so one member can be executed to
//! cover the whole group. No executable or stdin *bytes* are read during resolution or grouping;
//! selected groups prepare those bytes and their content hashes immediately before processing.

use crate::config::{BenchmarkId, BenchmarkVariant, Config, ConfigError};
use crate::workload::{GroupSpec, Sha256, WorkloadIdentity};
use ahash::{HashMap, HashMapExt as _};
use std::fs::{File, Metadata};
use std::io::Read as _;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A transient file identity: `(device, inode)`.
///
/// Process-local only — used to group hardlinked files cheaply and never persisted to storage,
/// JSON, or exports.
#[cfg(unix)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl FileIdentity {
    fn from_metadata(_path: &Path, metadata: &Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[cfg(not(unix))]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileIdentity(PathBuf);

#[cfg(not(unix))]
impl FileIdentity {
    fn from_metadata(path: &Path, _metadata: &Metadata) -> Self {
        // Without portable device/inode metadata, resolved paths safely remain separate groups.
        Self(path.to_path_buf())
    }
}

/// A process-local observation used to detect ordinary replacement and in-place modification.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FileSnapshot {
    identity: FileIdentity,
    len: u64,
    #[cfg(unix)]
    mtime: (i64, i64),
    #[cfg(unix)]
    ctime: (i64, i64),
    #[cfg(not(unix))]
    modified: Option<std::time::SystemTime>,
}

impl FileSnapshot {
    fn identity(&self) -> FileIdentity {
        self.identity.clone()
    }
}

#[cfg(unix)]
impl FileSnapshot {
    fn from_metadata(path: &Path, metadata: &Metadata) -> Self {
        Self {
            identity: FileIdentity::from_metadata(path, metadata),
            len: metadata.len(),
            mtime: (metadata.mtime(), metadata.mtime_nsec()),
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

#[cfg(not(unix))]
impl FileSnapshot {
    fn from_metadata(path: &Path, metadata: &Metadata) -> Self {
        Self {
            identity: FileIdentity::from_metadata(path, metadata),
            len: metadata.len(),
            modified: metadata.modified().ok(),
        }
    }
}

/// A selected case's content hashes and the exact stdin bytes prepared for execution.
#[derive(Debug)]
pub struct HashedArtifacts {
    executable: Sha256,
    stdin: Option<Sha256>,
    stdin_input: Option<Vec<u8>>,
}

impl HashedArtifacts {
    /// Executable content digest.
    #[must_use]
    pub fn executable(&self) -> Sha256 {
        self.executable
    }

    /// Optional stdin content digest.
    #[must_use]
    pub fn stdin(&self) -> Option<Sha256> {
        self.stdin
    }

    /// Consume the prepared artifacts and return the exact stdin bytes that were hashed.
    #[must_use]
    pub fn into_stdin_input(self) -> Option<Vec<u8>> {
        self.stdin_input
    }
}

/// A lightweight, filesystem-resolved case ready for grouping.
///
/// This records paths and transient snapshots, but deliberately does not read artifact contents.
/// Once its group is selected, the same object feeds artifact preparation and `Runner`
/// construction so argv and paths are never independently re-resolved.
#[derive(Debug, Clone)]
pub struct ResolvedCase {
    /// Owning benchmark id.
    pub benchmark: BenchmarkId,
    /// The source case config. Grouping callers provide the canonical hostless config.
    pub config: Config,
    /// Absolute executable path, anchored to `data_dir` when configured as relative.
    pub executable: PathBuf,
    /// Resolved stdin file path, if the benchmark has stdin.
    pub stdin_path: Option<PathBuf>,
    /// Non-artifact invocation fields (argv, checksum, stats, semantics, workdir policy).
    pub group_spec: GroupSpec,
    /// Transient executable identity and change-detection snapshot.
    executable_snapshot: FileSnapshot,
    /// Transient stdin identity and change-detection snapshot, if any.
    stdin_snapshot: Option<FileSnapshot>,
}

impl ResolvedCase {
    /// The cheap grouping key: same executable and stdin inode under an identical group spec.
    #[must_use]
    pub fn group_key(&self) -> GroupKey {
        GroupKey {
            benchmark: self.benchmark.clone(),
            group_spec_digest: self.group_spec.digest(),
            executable_identity: self.executable_snapshot.identity(),
            stdin_identity: self.stdin_snapshot.as_ref().map(FileSnapshot::identity),
        }
    }

    /// Prepare a selected case's artifact hashes and exact stdin bytes for execution.
    ///
    /// Only called once a group is selected for processing.
    pub fn hash_artifacts(&self) -> Result<HashedArtifacts, GroupError> {
        let executable = hash_content(&self.executable, &self.executable_snapshot)?;
        let (stdin, stdin_input) = match (&self.stdin_path, &self.stdin_snapshot) {
            (Some(path), Some(snapshot)) => {
                let bytes = read_content(path, snapshot)?;
                (Some(Sha256::hash_bytes(&bytes)), Some(bytes))
            }
            (None, None) => (None, None),
            _ => unreachable!("stdin path and snapshot are created together"),
        };
        Ok(HashedArtifacts {
            executable,
            stdin,
            stdin_input,
        })
    }

    /// Prepare an isolated case's stdin, asserting that its resolved file did not change.
    pub fn read_stdin(&self) -> Result<Option<Vec<u8>>, GroupError> {
        match (&self.stdin_path, &self.stdin_snapshot) {
            (Some(path), Some(snapshot)) => read_content(path, snapshot).map(Some),
            (None, None) => Ok(None),
            _ => unreachable!("stdin path and snapshot are created together"),
        }
    }

    /// Build this case's shared workload identity from freshly hashed content digests.
    #[must_use]
    pub fn workload_identity(&self, executable: Sha256, stdin: Option<Sha256>) -> WorkloadIdentity {
        WorkloadIdentity::shared(self.benchmark.clone(), executable, stdin, &self.group_spec)
    }
}

/// The cheap, hashable key used to find candidate groups.
///
/// Equality means two cases currently name the same executable/stdin files and have the same group
/// spec digest. The complete [`GroupSpec`] is compared separately before cases are merged, so this
/// key never makes a digest collision authoritative.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GroupKey {
    pub benchmark: BenchmarkId,
    pub group_spec_digest: Sha256,
    executable_identity: FileIdentity,
    stdin_identity: Option<FileIdentity>,
}

/// A group of resolved cases proven to currently name the same executable and stdin files.
#[derive(Debug, Clone)]
pub struct CaseGroup {
    pub key: GroupKey,
    /// Compact members, sorted by stable variant/product coordinates.
    cases: Vec<CaseRef>,
    /// The deterministic member reconstructed for hashing and execution only after selection.
    representative: CaseRef,
    /// Distinct executable projections covered by this group. Paths are reconstructed on demand.
    projections: Vec<Arc<ExecutableProjection>>,
}

impl CaseGroup {
    /// Number of concrete configs covered by this group.
    #[must_use]
    pub fn case_count(&self) -> usize {
        self.cases.len()
    }

    /// Reconstruct member configs lazily from their variant product ordinals.
    pub fn configs(&self) -> impl ExactSizeIterator<Item = Config> + '_ {
        self.cases.iter().map(CaseRef::config)
    }

    /// Reconstruct the representative case selected to cover this group.
    pub fn resolve_representative(&self) -> Result<ResolvedCase, GroupError> {
        let resolved = self.representative.resolve()?;
        if resolved.group_key() != self.key {
            return Err(GroupError::GroupChanged {
                path: resolved.executable,
            });
        }
        Ok(resolved)
    }

    /// Reconstruct and re-stat each distinct executable projection.
    ///
    /// Called immediately before processing a selected group, per the plan's before/after stat
    /// guards. Fixed stdin paths are also checked against their variant-level snapshot.
    pub fn restat(&self) -> Result<(), GroupError> {
        for projection in &self.projections {
            projection.restat()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct CaseRef {
    projection: Arc<ExecutableProjection>,
    ordinal: usize,
}

impl CaseRef {
    fn config(&self) -> Config {
        self.projection.plan.config(self.ordinal)
    }

    fn resolve(&self) -> Result<ResolvedCase, GroupError> {
        self.projection.resolve(self.ordinal)
    }

    fn order_key(&self) -> (usize, usize) {
        (self.projection.plan.id, self.ordinal)
    }
}

#[derive(Debug)]
struct ResolutionContext {
    data_dir: PathBuf,
}

impl ResolutionContext {
    fn new(data_dir: &Path) -> Result<Self, GroupError> {
        Ok(Self {
            data_dir: absolute_data_dir(data_dir)?,
        })
    }
}

#[derive(Debug)]
struct VariantPlan {
    id: usize,
    context: Arc<ResolutionContext>,
    variant: BenchmarkVariant,
    stdin_snapshot: Option<FileSnapshot>,
}

impl VariantPlan {
    fn config(&self, ordinal: usize) -> Config {
        self.variant
            .config()
            .config_at(ordinal)
            .expect("case ordinal comes from this variant's config product")
    }

    fn executable_path(&self, ordinal: usize) -> Result<PathBuf, GroupError> {
        let config = self.config(ordinal);
        let executable = config.expand_template(&self.variant.command_template()[0])?;
        Ok(resolve_executable(&executable, &self.context.data_dir))
    }
}

#[derive(Debug)]
struct ExecutableProjection {
    plan: Arc<VariantPlan>,
    representative_ordinal: usize,
    executable_snapshot: FileSnapshot,
}

#[derive(Clone, Default)]
enum ProjectionResolution {
    #[default]
    Unresolved,
    Resolved(Arc<ExecutableProjection>),
    Failed(Arc<GroupError>),
}

impl ExecutableProjection {
    fn resolve(&self, ordinal: usize) -> Result<ResolvedCase, GroupError> {
        let config = self.plan.config(ordinal);
        let mut argv = config.expand_templates(self.plan.variant.command_template())?;
        if argv.is_empty() {
            return Err(GroupError::EmptyCommand);
        }
        let executable_arg = argv.remove(0);
        let executable = resolve_executable(&executable_arg, &self.plan.context.data_dir);
        assert_path_snapshot(&executable, &self.executable_snapshot)?;

        let stdin_path = self.plan.variant.input().map(Path::to_path_buf);
        match (&stdin_path, &self.plan.stdin_snapshot) {
            (Some(path), Some(snapshot)) => assert_path_snapshot(path, snapshot)?,
            (None, None) => {}
            _ => unreachable!("stdin path and snapshot are created together"),
        }

        Ok(ResolvedCase {
            benchmark: self.plan.variant.benchmark_id().clone(),
            config,
            executable,
            stdin_path,
            group_spec: GroupSpec::new(
                argv,
                self.plan.variant.checksum().map(str::to_string),
                self.plan.variant.stats_options(),
            ),
            executable_snapshot: self.executable_snapshot.clone(),
            stdin_snapshot: self.plan.stdin_snapshot.clone(),
        })
    }

    fn restat(&self) -> Result<(), GroupError> {
        let executable = self.plan.executable_path(self.representative_ordinal)?;
        assert_path_snapshot(&executable, &self.executable_snapshot)?;
        match (self.plan.variant.input(), &self.plan.stdin_snapshot) {
            (Some(path), Some(snapshot)) => assert_path_snapshot(path, snapshot),
            (None, None) => Ok(()),
            _ => unreachable!("stdin path and snapshot are created together"),
        }
    }
}

struct GroupBuilder {
    /// Complete spec retained while building so the digest only selects a candidate bucket.
    group_spec: GroupSpec,
    representative: CaseRef,
    cases: Vec<CaseRef>,
    projections: Vec<Arc<ExecutableProjection>>,
}

impl GroupBuilder {
    fn new(group_spec: GroupSpec, case: CaseRef) -> Self {
        Self {
            group_spec,
            cases: vec![case.clone()],
            projections: vec![case.projection.clone()],
            representative: case,
        }
    }

    fn push(&mut self, case: CaseRef) {
        if !self
            .projections
            .iter()
            .any(|projection| Arc::ptr_eq(projection, &case.projection))
        {
            self.projections.push(case.projection.clone());
        }
        if case.order_key() < self.representative.order_key() {
            self.representative = case.clone();
        }
        self.cases.push(case);
    }

    fn finish(mut self, key: GroupKey) -> CaseGroup {
        self.cases.sort_unstable_by_key(CaseRef::order_key);
        self.projections.sort_unstable_by_key(|projection| {
            (projection.plan.id, projection.representative_ordinal)
        });
        CaseGroup {
            key,
            cases: self.cases,
            representative: self.representative,
            projections: self.projections,
        }
    }
}

/// Resolve a single `(variant, config)` case into a [`ResolvedCase`].
///
/// Relative executable paths are anchored to `data_dir`; absolute paths are retained. Executable
/// names are never searched through `PATH`. This is the single resolution path used by both
/// grouping and [`crate::runner::Runner`], so hashing and execution agree.
pub fn resolve_case(
    data_dir: &Path,
    variant: &BenchmarkVariant,
    config: &Config,
) -> Result<ResolvedCase, GroupError> {
    let data_dir = absolute_data_dir(data_dir)?;
    let mut argv = config.expand_templates(variant.command_template())?;
    if argv.is_empty() {
        return Err(GroupError::EmptyCommand);
    }
    let executable_arg = argv.remove(0);

    let executable = resolve_executable(&executable_arg, &data_dir);

    let stdin_path = variant.input().map(Path::to_path_buf);

    let executable_snapshot = stat_regular_file(&executable)?;
    let stdin_snapshot = stdin_path.as_deref().map(stat_regular_file).transpose()?;

    let group_spec = GroupSpec::new(
        argv,
        variant.checksum().map(str::to_string),
        variant.stats_options(),
    );

    Ok(ResolvedCase {
        benchmark: variant.benchmark_id().clone(),
        config: config.clone(),
        executable,
        stdin_path,
        group_spec,
        executable_snapshot,
        stdin_snapshot,
    })
}

/// Resolve and group every concrete case from a set of shared benchmark variants.
///
/// Returns the deterministic groups plus per-case resolution failures. A failed case is never
/// placed in a group; the caller decides whether a failure that matches the command filter is a
/// hard error (`run-all`) or merely skipped (`run`). Executables are resolved once per unique set
/// of values used by the executable template; groups retain only variant/product coordinates and
/// reconstruct paths and configs if selected.
pub fn resolve_and_group<'a>(
    data_dir: &Path,
    variants: impl IntoIterator<Item = &'a BenchmarkVariant>,
) -> (Vec<CaseGroup>, Vec<ResolveFailure>) {
    resolve_and_group_with_digest(data_dir, variants, GroupSpec::digest)
}

/// Implementation with an injectable digest function so collision handling can be tested without
/// finding an actual SHA-256 collision.
fn resolve_and_group_with_digest<'a>(
    data_dir: &Path,
    variants: impl IntoIterator<Item = &'a BenchmarkVariant>,
    group_spec_digest: impl Fn(&GroupSpec) -> Sha256,
) -> (Vec<CaseGroup>, Vec<ResolveFailure>) {
    let variants: Vec<_> = variants.into_iter().collect();
    let context = match ResolutionContext::new(data_dir) {
        Ok(context) => Arc::new(context),
        Err(error) => return failed_variants(variants, error),
    };
    let mut groups: HashMap<GroupKey, GroupBuilder> = HashMap::new();
    let mut failures = Vec::new();

    for (variant_id, variant) in variants.into_iter().enumerate() {
        let stdin_snapshot = match variant.input().map(stat_regular_file).transpose() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                record_variant_failures(variant, &Arc::new(error), &mut failures);
                continue;
            }
        };
        let plan = Arc::new(VariantPlan {
            id: variant_id,
            context: context.clone(),
            variant: variant.clone(),
            stdin_snapshot,
        });
        let product = plan.variant.config();
        let path_projection = product.projection_for_template(&plan.variant.command_template()[0]);
        let mut projections = vec![ProjectionResolution::Unresolved; path_projection.len()];

        for ordinal in 0..product.len() {
            let config = product
                .config_at(ordinal)
                .expect("ordinal is within the product length");
            let projected = path_projection.project(ordinal);
            let projection = match projections[projected].clone() {
                ProjectionResolution::Resolved(projection) => projection,
                ProjectionResolution::Failed(error) => {
                    failures.push(ResolveFailure {
                        benchmark: variant.benchmark_id().clone(),
                        config,
                        error,
                    });
                    continue;
                }
                ProjectionResolution::Unresolved => {
                    match resolve_projection(&plan, ordinal, &config) {
                        Ok(projection) => {
                            projections[projected] =
                                ProjectionResolution::Resolved(projection.clone());
                            projection
                        }
                        Err(error) => {
                            let error = Arc::new(error);
                            projections[projected] = ProjectionResolution::Failed(error.clone());
                            failures.push(ResolveFailure {
                                benchmark: variant.benchmark_id().clone(),
                                config,
                                error,
                            });
                            continue;
                        }
                    }
                }
            };

            let argv = match config.expand_templates(&plan.variant.command_template()[1..]) {
                Ok(argv) => argv,
                Err(error) => {
                    failures.push(ResolveFailure {
                        benchmark: variant.benchmark_id().clone(),
                        config,
                        error: Arc::new(error.into()),
                    });
                    continue;
                }
            };
            let group_spec = GroupSpec::new(
                argv,
                plan.variant.checksum().map(str::to_string),
                plan.variant.stats_options(),
            );
            let key = GroupKey {
                benchmark: plan.variant.benchmark_id().clone(),
                group_spec_digest: group_spec_digest(&group_spec),
                executable_identity: projection.executable_snapshot.identity(),
                stdin_identity: plan.stdin_snapshot.as_ref().map(FileSnapshot::identity),
            };
            push_group(
                &mut groups,
                key,
                group_spec,
                CaseRef {
                    projection,
                    ordinal,
                },
            );
        }
    }

    (finish_groups(groups), failures)
}

fn failed_variants(
    variants: Vec<&BenchmarkVariant>,
    error: GroupError,
) -> (Vec<CaseGroup>, Vec<ResolveFailure>) {
    let error = Arc::new(error);
    let mut failures = Vec::new();
    for variant in variants {
        record_variant_failures(variant, &error, &mut failures);
    }
    (Vec::new(), failures)
}

fn record_variant_failures(
    variant: &BenchmarkVariant,
    error: &Arc<GroupError>,
    failures: &mut Vec<ResolveFailure>,
) {
    for config in variant.config() {
        failures.push(ResolveFailure {
            benchmark: variant.benchmark_id().clone(),
            config,
            error: error.clone(),
        });
    }
}

fn resolve_projection(
    plan: &Arc<VariantPlan>,
    ordinal: usize,
    config: &Config,
) -> Result<Arc<ExecutableProjection>, GroupError> {
    let executable_arg = config.expand_template(&plan.variant.command_template()[0])?;
    let executable = resolve_executable(&executable_arg, &plan.context.data_dir);
    let executable_snapshot = stat_regular_file(&executable)?;
    Ok(Arc::new(ExecutableProjection {
        plan: plan.clone(),
        representative_ordinal: ordinal,
        executable_snapshot,
    }))
}

fn resolve_executable(argument: &str, data_dir: &Path) -> PathBuf {
    let argument_path = Path::new(argument);
    if argument_path.is_absolute() {
        argument_path.to_path_buf()
    } else {
        data_dir.join(argument_path)
    }
}

fn absolute_data_dir(data_dir: &Path) -> Result<PathBuf, GroupError> {
    if data_dir.is_absolute() {
        return Ok(data_dir.to_path_buf());
    }
    std::env::current_dir()
        .map(|current_dir| current_dir.join(data_dir))
        .map_err(|error| GroupError::Io {
            path: data_dir.to_path_buf(),
            error,
        })
}

fn push_group(
    groups: &mut HashMap<GroupKey, GroupBuilder>,
    key: GroupKey,
    group_spec: GroupSpec,
    case: CaseRef,
) {
    use std::collections::hash_map::Entry;

    match groups.entry(key) {
        Entry::Occupied(mut entry) => {
            // The digest only narrows the search. Structural equality is what authorizes sharing
            // one execution and measurement across the cases. A mismatch is an internal identity
            // invariant violation: continuing would silently fan one measurement out to different
            // invocations, and the durable workload hash cannot represent both specs either.
            assert!(
                entry.get().group_spec == group_spec,
                "different complete group specs produced the same digest {}",
                entry.key().group_spec_digest,
            );
            entry.get_mut().push(case);
        }
        Entry::Vacant(entry) => {
            entry.insert(GroupBuilder::new(group_spec, case));
        }
    }
}

fn finish_groups(groups: HashMap<GroupKey, GroupBuilder>) -> Vec<CaseGroup> {
    let mut result: Vec<CaseGroup> = groups
        .into_iter()
        .map(|(key, builder)| builder.finish(key))
        .collect();

    // Deterministic group order: by benchmark, stable case coordinate, then group-spec digest.
    result.sort_by(|a, b| {
        a.key
            .benchmark
            .cmp(&b.key.benchmark)
            .then_with(|| {
                a.representative
                    .order_key()
                    .cmp(&b.representative.order_key())
            })
            .then_with(|| a.key.group_spec_digest.cmp(&b.key.group_spec_digest))
    });
    result
}

/// A case that failed to resolve and is therefore excluded from grouping/fan-out.
#[derive(Debug)]
pub struct ResolveFailure {
    pub benchmark: BenchmarkId,
    pub config: Config,
    pub error: Arc<GroupError>,
}

/// Open a resolved artifact, assert the opened file is the expected snapshot, and hash that handle.
fn hash_content(path: &Path, expected: &FileSnapshot) -> Result<Sha256, GroupError> {
    let file = open_asserted(path, expected)?;
    let digest = Sha256::hash_reader(&file).map_err(|error| GroupError::Io {
        path: path.to_path_buf(),
        error,
    })?;
    assert_file_snapshot(path, &file, expected)?;
    Ok(digest)
}

/// Read a resolved artifact through one asserted handle and verify it again after the read.
fn read_content(path: &Path, expected: &FileSnapshot) -> Result<Vec<u8>, GroupError> {
    let mut file = open_asserted(path, expected)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| GroupError::Io {
            path: path.to_path_buf(),
            error,
        })?;
    assert_file_snapshot(path, &file, expected)?;
    Ok(bytes)
}

fn open_asserted(path: &Path, expected: &FileSnapshot) -> Result<File, GroupError> {
    let file = File::open(path).map_err(|error| GroupError::Io {
        path: path.to_path_buf(),
        error,
    })?;
    assert_file_snapshot(path, &file, expected)?;
    Ok(file)
}

fn assert_file_snapshot(
    path: &Path,
    file: &File,
    expected: &FileSnapshot,
) -> Result<(), GroupError> {
    let metadata = file.metadata().map_err(|error| GroupError::Io {
        path: path.to_path_buf(),
        error,
    })?;
    let current = snapshot(path, &metadata)?;
    if &current != expected {
        return Err(GroupError::GroupChanged {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn assert_path_snapshot(path: &Path, expected: &FileSnapshot) -> Result<(), GroupError> {
    let current = stat_regular_file(path)?;
    if &current != expected {
        return Err(GroupError::GroupChanged {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

/// Stat a path following symlinks and require it to be a regular file, returning its `(device,
/// inode)` identity.
///
/// A non-regular file — FIFO, directory, device, or socket — is rejected as a validation error,
/// because such a path has no stable content to hash and could block or read unbounded.
fn stat_regular_file(path: &Path) -> Result<FileSnapshot, GroupError> {
    // `metadata` follows symlinks (unlike `symlink_metadata`).
    let metadata = std::fs::metadata(path).map_err(|error| GroupError::Io {
        path: path.to_path_buf(),
        error,
    })?;
    snapshot(path, &metadata)
}

fn snapshot(path: &Path, metadata: &Metadata) -> Result<FileSnapshot, GroupError> {
    if !metadata.file_type().is_file() {
        return Err(GroupError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    Ok(FileSnapshot::from_metadata(path, metadata))
}

/// Errors resolving, stating, or hashing a case's files.
#[derive(Debug, thiserror::Error)]
pub enum GroupError {
    #[error("failed to expand command template: {0}")]
    Config(#[from] ConfigError),
    #[error("benchmark command is empty")]
    EmptyCommand,
    #[error("'{path}' is not a regular file")]
    NotRegularFile { path: PathBuf },
    #[error("artifact '{path}' changed during grouping, hashing, or measurement")]
    GroupChanged { path: PathBuf },
    #[error("I/O error at '{path}': {error}")]
    Io {
        path: PathBuf,
        #[source]
        error: std::io::Error,
    },
}

#[cfg(unix)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Benchmark, ConfigProduct};
    use crate::stats::StatsOptions;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;
    use tempfile::TempDir;

    /// Create an executable file at `data_dir/rel` with the given bytes.
    fn write_exe(data_dir: &Path, rel: &str, bytes: &[u8]) -> PathBuf {
        let path = data_dir.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&path, perms).unwrap();
        path
    }

    fn variant(command: &[&str]) -> Benchmark {
        Benchmark::new(
            "b".try_into().unwrap(),
            ConfigProduct::default(),
            command.iter().map(|s| (*s).to_string()).collect(),
            None,
            None,
            StatsOptions::default(),
        )
        .unwrap()
    }

    #[test]
    fn hardlinked_cases_group_together() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        let a = write_exe(data, "builds/a/bin", b"identical executable bytes");
        let b_path = data.join("builds/b/bin");
        fs::create_dir_all(b_path.parent().unwrap()).unwrap();
        fs::hard_link(&a, &b_path).unwrap();

        let bench_a = variant(&["builds/a/bin", "arg"]);
        let bench_b = variant(&["builds/b/bin", "arg"]);
        let groups = resolve_and_group(data, [&bench_a.variants()[0], &bench_b.variants()[0]]).0;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].case_count(), 2);
        // Distinct hardlink path projections are both retained without retaining their paths.
        assert_eq!(groups[0].projections.len(), 2);
    }

    #[test]
    fn copied_equal_files_are_separate_groups() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        // Byte-identical but separate inodes (copies, not hardlinks).
        write_exe(data, "builds/a/bin", b"identical executable bytes");
        write_exe(data, "builds/b/bin", b"identical executable bytes");

        let bench_a = variant(&["builds/a/bin", "arg"]);
        let bench_b = variant(&["builds/b/bin", "arg"]);
        let cfg = Config::new();

        let case_a = resolve_case(data, &bench_a.variants()[0], &cfg).unwrap();
        let case_b = resolve_case(data, &bench_b.variants()[0], &cfg).unwrap();
        assert_ne!(case_a.group_key(), case_b.group_key());

        let groups = resolve_and_group(data, [&bench_a.variants()[0], &bench_b.variants()[0]]).0;
        assert_eq!(groups.len(), 2);

        // But their content hashes agree, so a selected copy can inherit the same shared workload.
        let artifacts = case_a.hash_artifacts().unwrap();
        assert_eq!(
            artifacts.executable,
            Sha256::hash_bytes(b"identical executable bytes")
        );
    }

    #[test]
    fn differing_argv_does_not_group() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        let a = write_exe(data, "builds/a/bin", b"exe");
        let b_path = data.join("builds/b/bin");
        fs::create_dir_all(b_path.parent().unwrap()).unwrap();
        fs::hard_link(&a, &b_path).unwrap();

        let bench_a = variant(&["builds/a/bin", "one"]);
        let bench_b = variant(&["builds/b/bin", "two"]);

        let groups = resolve_and_group(data, [&bench_a.variants()[0], &bench_b.variants()[0]]).0;
        assert_eq!(groups.len(), 2);
    }

    #[test]
    #[should_panic(expected = "different complete group specs produced the same digest")]
    fn differing_argv_digest_collision_is_fatal() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        let a = write_exe(data, "builds/a/bin", b"exe");
        let b_path = data.join("builds/b/bin");
        fs::create_dir_all(b_path.parent().unwrap()).unwrap();
        fs::hard_link(&a, &b_path).unwrap();

        let bench_a = variant(&["builds/a/bin", "one"]);
        let bench_b = variant(&["builds/b/bin", "two"]);
        let forced_collision = Sha256::hash_bytes(b"forced group-spec digest collision");
        let _ = resolve_and_group_with_digest(
            data,
            [&bench_a.variants()[0], &bench_b.variants()[0]],
            |_| forced_collision,
        );
    }

    #[test]
    fn non_regular_executable_rejected() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        // A directory where the executable should be.
        fs::create_dir_all(data.join("builds/a/bin")).unwrap();
        let bench_a = variant(&["builds/a/bin"]);
        let err = resolve_case(data, &bench_a.variants()[0], &Config::new()).unwrap_err();
        assert!(matches!(err, GroupError::NotRegularFile { .. }));
    }

    #[test]
    fn non_executable_file_is_left_for_the_runner_to_reject() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        let path = write_exe(data, "builds/a/bin", b"not runnable");
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o644);
        fs::set_permissions(&path, permissions).unwrap();
        let bench = variant(&["builds/a/bin"]);

        let (groups, failures) = resolve_and_group(data, [&bench.variants()[0]]);
        assert_eq!(groups.len(), 1);
        assert!(failures.is_empty());
    }

    #[test]
    fn bare_executable_name_is_relative_to_data_dir() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        let expected = write_exe(data, "local-benchmark", b"executable");
        let bench = variant(&["local-benchmark"]);

        let case = resolve_case(data, &bench.variants()[0], &Config::new()).unwrap();
        assert_eq!(case.executable, expected);
    }

    #[test]
    fn non_regular_stdin_rejected() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        write_exe(data, "builds/a/bin", b"exe");
        // A FIFO as the stdin path.
        let fifo = data.join("fifo");
        let status = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(status.success());

        let bench = Benchmark::new(
            "b".try_into().unwrap(),
            ConfigProduct::default(),
            vec!["builds/a/bin".into()],
            Some(fifo),
            None,
            StatsOptions::default(),
        )
        .unwrap();

        let err = resolve_case(data, &bench.variants()[0], &Config::new()).unwrap_err();
        assert!(matches!(err, GroupError::NotRegularFile { .. }));
    }

    #[test]
    fn restat_detects_changed_executable() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        write_exe(data, "builds/a/bin", b"original");
        let bench = variant(&["builds/a/bin"]);
        let group = resolve_and_group(data, [&bench.variants()[0]]).0;
        assert_eq!(group.len(), 1);
        group[0].restat().unwrap();

        // Replace the executable with a new inode.
        fs::remove_file(data.join("builds/a/bin")).unwrap();
        write_exe(data, "builds/a/bin", b"replacement with different inode");
        assert!(matches!(
            group[0].restat(),
            Err(GroupError::GroupChanged { .. })
        ));
    }

    #[test]
    fn restat_detects_executable_modified_in_place() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        write_exe(data, "builds/a/bin", b"original");
        let bench = variant(&["builds/a/bin"]);
        let group = resolve_and_group(data, [&bench.variants()[0]]).0;

        // Same-length `fs::write` changes the bytes without replacing the inode or changing size.
        fs::write(data.join("builds/a/bin"), b"changed!").unwrap();
        assert!(matches!(
            group[0].restat(),
            Err(GroupError::GroupChanged { .. })
        ));
    }

    #[test]
    fn stdin_digest_and_prepared_bytes_come_from_one_read() {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        write_exe(data, "builds/a/bin", b"exe");
        let input = data.join("input");
        fs::write(&input, b"input bytes").unwrap();
        let bench = Benchmark::new(
            "b".try_into().unwrap(),
            ConfigProduct::default(),
            vec!["builds/a/bin".into()],
            Some(input),
            None,
            StatsOptions::default(),
        )
        .unwrap();
        let case = resolve_case(data, &bench.variants()[0], &Config::new()).unwrap();

        let artifacts = case.hash_artifacts().unwrap();
        assert_eq!(artifacts.stdin(), Some(Sha256::hash_bytes(b"input bytes")));
        assert_eq!(
            artifacts.into_stdin_input().as_deref(),
            Some(b"input bytes".as_slice())
        );
    }
}
