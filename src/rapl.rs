//! Package power from the Linux powercap (RAPL) energy counters, which cover Intel and modern AMD.
//! The counters are root-only by default, so hosts grant read access (see `tools/bench-host`).

use std::fmt::{Display, Formatter};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

const POWERCAP_DIR: &str = "/sys/class/powercap";

#[derive(Debug, thiserror::Error)]
pub enum RaplError {
    #[error("package power readings are only supported on Linux")]
    Unsupported,
    #[error("no RAPL package zone found under {POWERCAP_DIR}")]
    NoPackageZone,
    #[error("failed to read {path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("invalid value in {path}: {value:?}")]
    InvalidValue { path: PathBuf, value: String },
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct EnergyCounter {
    max_energy_uj: u64,
    last_energy_uj: u64,
}

impl EnergyCounter {
    fn new(max_energy_uj: u64, initial_energy_uj: u64) -> Self {
        Self {
            max_energy_uj,
            last_energy_uj: initial_energy_uj,
        }
    }

    fn advance(&mut self, energy_uj: u64) -> u64 {
        let delta = if energy_uj >= self.last_energy_uj {
            energy_uj - self.last_energy_uj
        } else {
            // The counter wrapped past max_energy_range_uj
            self.max_energy_uj
                .saturating_sub(self.last_energy_uj)
                .saturating_add(energy_uj)
        };
        self.last_energy_uj = energy_uj;
        delta
    }
}

#[derive(Debug)]
pub struct PackagePower {
    energy_path: PathBuf,
    counter: EnergyCounter,
    last_instant: Instant,
}

impl PackagePower {
    pub fn open() -> Result<Self, RaplError> {
        if !cfg!(target_os = "linux") {
            return Err(RaplError::Unsupported);
        }
        let zone = find_package_zone(Path::new(POWERCAP_DIR))?;
        Self::open_zone(&zone)
    }

    pub(crate) fn open_zone(zone: &Path) -> Result<Self, RaplError> {
        let energy_path = zone.join("energy_uj");
        let max_energy_uj = read_u64(&zone.join("max_energy_range_uj"))?;
        let initial_energy_uj = read_u64(&energy_path)?;
        Ok(Self {
            energy_path,
            counter: EnergyCounter::new(max_energy_uj, initial_energy_uj),
            last_instant: Instant::now(),
        })
    }

    #[must_use]
    pub fn since_last(&self) -> std::time::Duration {
        self.last_instant.elapsed()
    }

    /// Average watts since the previous reading.
    pub fn sample_watts(&mut self) -> Result<f64, RaplError> {
        let energy_uj = read_u64(&self.energy_path)?;
        let now = Instant::now();
        let delta_uj = self.counter.advance(energy_uj);
        let seconds = now.duration_since(self.last_instant).as_secs_f64();
        self.last_instant = now;
        Ok(watts(delta_uj, seconds))
    }
}

#[allow(clippy::cast_precision_loss)]
fn watts(delta_uj: u64, seconds: f64) -> f64 {
    if seconds <= 0.0 {
        return 0.0;
    }
    delta_uj as f64 / 1_000_000.0 / seconds
}

/// The powercap layout is `<control type>:<zone>[:<subzone>]` with a `name` attribute per zone.
/// `intel_rapl_common.c` names package zones `package-<n>` (`package-<n>-die-<m>` on multi-die
/// parts) and registers them under `intel-rapl` (MSR, also AMD), `intel-rapl-mmio` or
/// `intel-rapl-tpmi`. Only an exact `package-0` is accepted, preferring the MSR control type when
/// several expose it; anything else means package power is unavailable.
fn find_package_zone(powercap_dir: &Path) -> Result<PathBuf, RaplError> {
    let entries = fs::read_dir(powercap_dir).map_err(|source| RaplError::Io {
        path: powercap_dir.to_path_buf(),
        source,
    })?;

    let mut candidates: Vec<(bool, PathBuf)> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter_map(|path| {
            let preferred = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(rapl_zone_control_type)
                .map(|control_type| control_type == "intel-rapl")?;
            let name = fs::read_to_string(path.join("name")).ok()?;
            (name.trim() == "package-0").then_some((!preferred, path))
        })
        .collect();
    candidates.sort();
    candidates
        .into_iter()
        .next()
        .map(|(_, path)| path)
        .ok_or(RaplError::NoPackageZone)
}

/// `intel-rapl:0` gives `intel-rapl`; sub-zones like `intel-rapl:0:1` and other drivers give none.
fn rapl_zone_control_type(dir_name: &str) -> Option<&str> {
    let (control_type, id) = dir_name.split_once(':')?;
    let top_level = !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit());
    (top_level && control_type.starts_with("intel-rapl")).then_some(control_type)
}

fn read_u64(path: &Path) -> Result<u64, RaplError> {
    let value = fs::read_to_string(path).map_err(|source| RaplError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    value.trim().parse().map_err(|_| RaplError::InvalidValue {
        path: path.to_path_buf(),
        value: value.trim().to_string(),
    })
}

impl Display for PackagePower {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.energy_path.display())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn counter_delta_and_wraparound() {
        let mut counter = EnergyCounter::new(1000, 900);
        assert_eq!(counter.advance(950), 50);
        // 950 -> 1000 wraps to 0 -> 30
        assert_eq!(counter.advance(30), 80);
        assert_eq!(counter.advance(30), 0);
    }

    #[test]
    fn watts_from_energy_delta() {
        assert!((watts(65_000_000, 1.0) - 65.0).abs() < 1e-9);
        assert!((watts(16_250_000, 0.25) - 65.0).abs() < 1e-9);
        assert_eq!(watts(1, 0.0), 0.0);
    }

    #[test]
    fn zone_control_types() {
        assert_eq!(rapl_zone_control_type("intel-rapl:0"), Some("intel-rapl"));
        assert_eq!(rapl_zone_control_type("intel-rapl:12"), Some("intel-rapl"));
        assert_eq!(
            rapl_zone_control_type("intel-rapl-mmio:0"),
            Some("intel-rapl-mmio")
        );
        assert_eq!(
            rapl_zone_control_type("intel-rapl-tpmi:1"),
            Some("intel-rapl-tpmi")
        );
        assert_eq!(rapl_zone_control_type("intel-rapl:0:1"), None);
        assert_eq!(rapl_zone_control_type("intel-rapl:"), None);
        assert_eq!(rapl_zone_control_type("intel-rapl"), None);
        assert_eq!(rapl_zone_control_type("dtpm:0"), None);
    }

    fn write_zone(dir: &Path, id: &str, name: &str, max: u64, energy: u64) -> PathBuf {
        write_typed_zone(dir, "intel-rapl", id, name, max, energy)
    }

    fn write_typed_zone(
        dir: &Path,
        control_type: &str,
        id: &str,
        name: &str,
        max: u64,
        energy: u64,
    ) -> PathBuf {
        let zone = dir.join(format!("{control_type}:{id}"));
        fs::create_dir_all(&zone).unwrap();
        fs::write(zone.join("name"), format!("{name}\n")).unwrap();
        fs::write(zone.join("max_energy_range_uj"), format!("{max}\n")).unwrap();
        fs::write(zone.join("energy_uj"), format!("{energy}\n")).unwrap();
        zone
    }

    #[test]
    fn finds_package_zone_by_name() {
        let dir = TempDir::new().unwrap();
        write_zone(dir.path(), "0:0", "core", 10, 1);
        write_zone(dir.path(), "1", "psys", 10, 1);
        let package = write_zone(dir.path(), "0", "package-0", 10, 1);
        assert_eq!(find_package_zone(dir.path()).unwrap(), package);
    }

    #[test]
    fn package_zone_can_be_under_another_rapl_control_type() {
        let dir = TempDir::new().unwrap();
        let tpmi = write_typed_zone(dir.path(), "intel-rapl-tpmi", "0", "package-0", 10, 1);
        assert_eq!(find_package_zone(dir.path()).unwrap(), tpmi);

        // The MSR control type is preferred when both expose the package
        let msr = write_zone(dir.path(), "0", "package-0", 10, 1);
        write_typed_zone(dir.path(), "intel-rapl-mmio", "0", "package-0", 10, 1);
        assert_eq!(find_package_zone(dir.path()).unwrap(), msr);
    }

    #[test]
    fn no_fallback_to_other_zones() {
        let dir = TempDir::new().unwrap();
        write_zone(dir.path(), "0", "psys", 10, 1);
        write_zone(dir.path(), "0:0", "core", 10, 1);
        write_zone(dir.path(), "1", "package-0-die-0", 10, 1);
        write_zone(dir.path(), "2", "package-1", 10, 1);
        write_typed_zone(dir.path(), "dtpm", "0", "package-0", 10, 1);
        assert!(matches!(
            find_package_zone(dir.path()),
            Err(RaplError::NoPackageZone)
        ));
    }

    #[test]
    fn errors_without_zones() {
        let dir = TempDir::new().unwrap();
        assert!(matches!(
            find_package_zone(dir.path()),
            Err(RaplError::NoPackageZone)
        ));
    }

    #[test]
    fn samples_power_from_sysfs_files() {
        let dir = TempDir::new().unwrap();
        let zone = write_zone(dir.path(), "0", "package-0", 1_000_000_000, 500);
        let mut power = PackagePower::open_zone(&zone).unwrap();
        fs::write(zone.join("energy_uj"), "1000500\n").unwrap();
        // Elapsed time is tiny, so just check the delta is accounted for once
        let first = power.sample_watts().unwrap();
        assert!(first > 0.0);
        let second = power.sample_watts().unwrap();
        assert_eq!(second, 0.0);
    }

    #[test]
    fn invalid_values_are_reported() {
        let dir = TempDir::new().unwrap();
        let zone = dir.path().join("intel-rapl:0");
        fs::create_dir_all(&zone).unwrap();
        fs::write(zone.join("max_energy_range_uj"), "lots\n").unwrap();
        fs::write(zone.join("energy_uj"), "1\n").unwrap();
        assert!(matches!(
            PackagePower::open_zone(&zone),
            Err(RaplError::InvalidValue { .. })
        ));
    }
}
