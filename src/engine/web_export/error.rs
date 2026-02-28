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
    #[error("I/O error at '{path:?}': {error}")]
    Io {
        path: PathBuf,
        #[source]
        error: io::Error,
    },
    #[error("JSON error: {0}")]
    Json(#[source] serde_json::Error),
}
