use crate::host_config::HostConfigError;
use crate::storage::HybridDiskError;
use std::io;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum WebExportError {
    #[error("storage error: {0}")]
    Storage(#[from] HybridDiskError),
}

#[derive(Debug, thiserror::Error)]
pub enum WebSnapshotExportError {
    #[error(transparent)]
    WebExport(#[from] WebExportError),
    #[error("host config error: {0}")]
    HostConfig(#[from] HostConfigError),
    #[error("I/O error at '{path:?}': {error}")]
    Io {
        path: PathBuf,
        #[source]
        error: io::Error,
    },
    #[error("JSON error: {0}")]
    Json(#[source] serde_json::Error),
}
