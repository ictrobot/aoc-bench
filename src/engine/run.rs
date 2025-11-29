use crate::config::{BenchmarkVariant, Config, ConfigFile, KeyValue};
use crate::host_config::{HostConfig, HostConfigError};
use crate::runner::{RunError, Runner};
use crate::stable::{RecordOptions, RecordOutcome, preview_run_series, record_run_series};
use crate::storage::{FileLock, HybridDiskError, HybridDiskStorage, Storage};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub struct RunEngineConfig {
    pub config_file: ConfigFile,
    pub host_kv: KeyValue,
    pub dry_run: bool,
    pub force_update_stable: bool,
}

#[derive(Debug)]
pub struct RunEngine {
    pub config_file: ConfigFile,
    pub host_config: HostConfig,
    pub host_kv: KeyValue,
    pub storage: HybridDiskStorage,
    pub dry_run: bool,
    pub force_update_stable: bool,
    _lock: FileLock,
}

impl RunEngine {
    pub fn new(config: RunEngineConfig) -> Result<Self, RunEngineError> {
        let RunEngineConfig {
            config_file,
            host_kv,
            dry_run,
            force_update_stable,
        } = config;

        let host = host_kv.value_name();

        let host_config = HostConfig::load(config_file.data_dir(), host)?;

        let storage = HybridDiskStorage::new(config_file.clone(), host)
            .map_err(RunEngineError::StorageError)?;

        let lock = storage.acquire_lock().map_err(RunEngineError::LockError)?;

        Ok(Self {
            config_file,
            host_config,
            host_kv,
            storage,
            dry_run,
            force_update_stable,
            _lock: lock,
        })
    }

    pub fn run(&self, variant: &BenchmarkVariant, config: &Config) -> Result<(), RunEngineError> {
        let config = config.with(self.host_kv.clone());

        let runner = Runner::new(
            self.config_file.data_dir(),
            variant,
            config.clone(),
            self.host_config.clone(),
        )?;

        info!("running series");

        let series = runner.run_series()?;

        let record_options = RecordOptions {
            force_update_stable: self.force_update_stable,
        };

        let outcome = if self.dry_run {
            preview_run_series(&self.storage, &series, record_options)
                .map_err(RunEngineError::PreviewError)?
        } else {
            let (outcome, json_path) = record_run_series(&self.storage, &series, record_options)
                .map_err(RunEngineError::PersistError)?;
            info!(
                path = %json_path.display(),
                "stored run series"
            );
            outcome
        };

        self.log_outcome(&outcome);

        Ok(())
    }

    fn log_outcome(&self, outcome: &RecordOutcome) {
        match outcome {
            RecordOutcome::Initial if self.dry_run => {
                info!("would have recorded new run series");
            }
            RecordOutcome::Initial => {
                info!("recorded new run series");
            }
            RecordOutcome::Matched => {
                info!("matched existing stable result");
            }
            RecordOutcome::Suspicious {
                current_stable,
                suspicious_count,
            } => {
                warn!(
                    suspicious_count,
                    stable_ns = current_stable.median_run_mean_ns,
                    "didn't match stable result, suspicious"
                );
            }
            RecordOutcome::Replaced { old_stable } if self.dry_run => {
                warn!(
                    old_stable_ns = old_stable.median_run_mean_ns,
                    "didn't match stable result, would have replaced"
                );
            }
            RecordOutcome::Replaced { old_stable } => {
                warn!(
                    old_stable_ns = old_stable.median_run_mean_ns,
                    "didn't match stable result, replaced"
                );
            }
            RecordOutcome::Forced { old_stable } if self.dry_run => {
                warn!(
                    old_stable_ns = old_stable.median_run_mean_ns,
                    "would have forced replacement of stable result"
                );
            }
            RecordOutcome::Forced { old_stable } => {
                warn!(
                    old_stable_ns = old_stable.median_run_mean_ns,
                    "forced replacement of stable result"
                );
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunEngineError {
    #[error("failed to load host config: {0}")]
    HostConfigError(#[from] HostConfigError),
    #[error("failed to open storage: {0}")]
    StorageError(#[source] HybridDiskError),
    #[error("failed to acquire exclusive benchmark lock: {0}")]
    LockError(#[source] HybridDiskError),
    #[error("failed to run benchmark: {0}")]
    RunnerError(#[from] RunError),
    #[error("failed to compute dry-run outcome: {0}")]
    PreviewError(#[source] HybridDiskError),
    #[error("failed to persist results: {0}")]
    PersistError(#[source] HybridDiskError),
}
