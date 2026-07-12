//! Shared fixtures for run workflow unit tests.

use crate::config::ConfigFile;
use crate::host_config::HostConfig;
use crate::storage::HybridDiskStorage;
use std::fs;
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use tempfile::TempDir;

/// Write an executable script that streams valid sample lines forever.
pub fn write_sampler(path: &Path) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        path,
        "#!/bin/sh\nwhile true; do printf 'SAMPLE\\t1000\\t20000000\\n'; done\n",
    )
    .unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// Storage and host policy rooted in a temporary test data directory.
pub struct Fixture {
    _dir: TempDir,
    pub storage: HybridDiskStorage,
    pub host_config: HostConfig,
}

impl Fixture {
    /// Create a fixture, allowing the caller to lay out executables before config is loaded.
    pub fn new(config_json: &str, setup_files: impl FnOnce(&Path)) -> Self {
        let dir = TempDir::new().unwrap();
        let data = dir.path();
        fs::create_dir_all(data.join("results/testhost")).unwrap();
        setup_files(data);

        let config_file = ConfigFile::from_str(data, Some("testhost"), config_json).unwrap();
        let storage = HybridDiskStorage::new(config_file, "testhost").unwrap();
        Self {
            _dir: dir,
            storage,
            host_config: HostConfig::default(),
        }
    }
}
