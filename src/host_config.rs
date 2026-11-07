use crate::config::{Config, ConfigFile, Key};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

const HOSTS_DIR: &str = "hosts";

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct HostConfig {
    #[serde(default)]
    pub cpu_affinity: CpuAffinity,
    #[serde(default = "default_disable_aslr")]
    pub disable_aslr: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Sustained package power limit in watts (PL1 on Intel). Set it above the real limit for more
    /// headroom.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_limit_watts: Option<f64>,
    /// Settings for particular cases, in order. The first override whose config matches a case
    /// replaces the settings it sets for that case.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config_overrides: Vec<ConfigOverride>,
}

/// Runner settings for the cases that have every config value in `config`.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigOverride {
    /// Config key names mapped to the value name a case must have.
    pub config: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_affinity: Option<CpuAffinity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disable_aslr: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_limit_watts: Option<f64>,
}

impl ConfigOverride {
    fn matches(&self, config: &Config) -> bool {
        self.config.iter().all(|(name, value)| {
            config
                .get_by_name(name)
                .is_some_and(|kv| kv.value_name() == value)
        })
    }
}

impl HostConfig {
    pub fn load(data_dir: &Path, host: &str) -> Result<Self, HostConfigError> {
        let path = data_dir.join(HOSTS_DIR).join(format!("{host}.json"));
        let contents = match fs::read_to_string(&path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(HostConfigError::Io { path, source: e }),
        };

        let parsed: HostConfig =
            serde_json::from_str(&contents).map_err(|source| HostConfigError::Json {
                path: path.clone(),
                source,
            })?;
        if let Some(watts) = parsed.power_limit_watts {
            validate_power_limit(watts).map_err(HostConfigError::Invalid)?;
        }
        for (index, config_override) in parsed.config_overrides.iter().enumerate() {
            if config_override.config.is_empty() {
                return Err(HostConfigError::Override {
                    index,
                    reason: "config must name at least one key".to_string(),
                });
            }
            if let Some(watts) = config_override.power_limit_watts {
                validate_power_limit(watts).map_err(|reason| HostConfigError::Override {
                    index,
                    reason: reason.to_string(),
                })?;
            }
        }

        Ok(parsed)
    }

    /// The settings for one case, with the first matching override applied.
    #[must_use]
    pub fn for_config(&self, config: &Config) -> HostConfig {
        let mut settings = HostConfig {
            cpu_affinity: self.cpu_affinity.clone(),
            disable_aslr: self.disable_aslr,
            description: self.description.clone(),
            power_limit_watts: self.power_limit_watts,
            config_overrides: Vec::new(),
        };
        if let Some(config_override) = self.config_overrides.iter().find(|o| o.matches(config)) {
            if let Some(cpu_affinity) = &config_override.cpu_affinity {
                settings.cpu_affinity = cpu_affinity.clone();
            }
            if let Some(disable_aslr) = config_override.disable_aslr {
                settings.disable_aslr = disable_aslr;
            }
            if let Some(watts) = config_override.power_limit_watts {
                settings.power_limit_watts = Some(watts);
            }
        }
        settings
    }

    /// Check the overrides against the benchmark config.
    ///
    /// Each benchmark that uses a key with overrides must appear in the arguments of every
    /// benchmark command. Cases with different arguments never share a workload, so every case a
    /// measurement covers gets the same settings. A key used only in the executable path, could
    /// otherwise give shared workloads different settings.
    pub fn check_config_overrides(&self, config_file: &ConfigFile) -> Result<(), HostConfigError> {
        // Each key with the first override that uses it, so the benchmarks are checked once per key
        let mut keys: BTreeMap<&str, (usize, &Key)> = BTreeMap::new();
        for (index, config_override) in self.config_overrides.iter().enumerate() {
            let invalid = |reason: String| HostConfigError::Override { index, reason };
            for (name, value) in &config_override.config {
                let key = config_file
                    .key_from_name(name)
                    .ok_or_else(|| invalid(format!("unknown config key {name:?}")))?;
                if key == config_file.host_key() {
                    return Err(invalid(format!("{name:?} is the host key")));
                }
                if key.value_from_name(value).is_none() {
                    return Err(invalid(format!("{value:?} is not a value of {name:?}")));
                }
                keys.entry(name).or_insert((index, key));
            }
        }
        for (name, (index, key)) in keys {
            let placeholder = format!("{{{name}}}");
            for benchmark in config_file.benchmarks() {
                for variant in benchmark.variants() {
                    let uses_key = variant.config().keys().any(|k| k == key);
                    let in_arguments = variant.command_template()[1..]
                        .iter()
                        .any(|argument| argument.contains(&placeholder));
                    if uses_key && !in_arguments {
                        return Err(HostConfigError::Override {
                            index,
                            reason: format!(
                                "{name:?} is not in the command arguments of {}",
                                benchmark.id()
                            ),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn validate_power_limit(watts: f64) -> Result<(), &'static str> {
    if watts.is_finite() && watts > 0.0 {
        Ok(())
    } else {
        Err("power_limit_watts must be positive")
    }
}

/// Parses a `power_limit_watts` command line value with the same rule as the host config file.
pub fn parse_power_limit(value: &str) -> Result<f64, String> {
    let watts: f64 = value
        .parse()
        .map_err(|_| format!("{value:?} is not a number"))?;
    validate_power_limit(watts).map_err(str::to_string)?;
    Ok(watts)
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            cpu_affinity: CpuAffinity::All,
            disable_aslr: default_disable_aslr(),
            description: None,
            power_limit_watts: None,
            config_overrides: Vec::new(),
        }
    }
}

fn default_disable_aslr() -> bool {
    // A warning is logged if disable_aslr is enabled on unsupported platforms
    cfg!(target_os = "linux")
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum CpuAffinity {
    #[default]
    All,
    Cpus(Vec<usize>),
}

impl CpuAffinity {
    fn parse_cpuset(s: &str) -> Option<Vec<usize>> {
        let mut cpus = Vec::new();
        for part in s.split(',') {
            if part.is_empty() {
                return None;
            }

            if let Some((a, b)) = part.split_once('-') {
                let start: usize = a.parse().ok()?;
                let end: usize = b.parse().ok()?;
                if start > end {
                    return None;
                }
                for cpu in start..=end {
                    cpus.push(cpu);
                }
            } else {
                let cpu: usize = part.parse().ok()?;
                cpus.push(cpu);
            }
        }
        cpus.sort_unstable();
        cpus.dedup();
        Some(cpus)
    }
}

impl FromStr for CpuAffinity {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "all" {
            Ok(CpuAffinity::All)
        } else {
            match CpuAffinity::parse_cpuset(s) {
                Some(cpus) if cpus.is_empty() => Ok(CpuAffinity::All),
                Some(cpus) => Ok(CpuAffinity::Cpus(cpus)),
                None => Err("invalid cpuset string"),
            }
        }
    }
}

impl Display for CpuAffinity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let CpuAffinity::Cpus(cpus) = self
            && !cpus.is_empty()
        {
            let mut iter = cpus.iter().peekable();
            let mut first = true;

            while let Some(&start) = iter.next() {
                if !first {
                    write!(f, ",")?;
                }
                first = false;

                let mut end = start;
                while let Some(&&next) = iter.peek()
                    && next == end + 1
                {
                    end = next;
                    iter.next();
                }

                if end == start {
                    write!(f, "{start}")?;
                } else if end == start + 1 {
                    write!(f, "{start},{end}")?;
                } else {
                    write!(f, "{start}-{end}")?;
                }
            }

            Ok(())
        } else {
            write!(f, "all")
        }
    }
}

// Used by serde
impl TryFrom<String> for CpuAffinity {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
impl From<CpuAffinity> for String {
    fn from(s: CpuAffinity) -> Self {
        s.to_string()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HostConfigError {
    #[error("failed to read host config at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse host config at {path}: {source}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid host config: {0}")]
    Invalid(&'static str),
    #[error("invalid host config config_overrides[{index}]: {reason}")]
    Override { index: usize, reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_defaults_when_missing() {
        let dir = TempDir::new().unwrap();
        let cfg = HostConfig::load(dir.path(), "missing").unwrap();
        assert_eq!(cfg.cpu_affinity, CpuAffinity::All);
        assert!(cfg.disable_aslr);
    }

    #[test]
    fn test_parses_empty_config() {
        let dir = TempDir::new().unwrap();
        let hosts = dir.path().join("hosts");
        fs::create_dir_all(&hosts).unwrap();
        fs::write(hosts.join("testhost.json"), "{}").unwrap();

        let cfg = HostConfig::load(dir.path(), "testhost").unwrap();
        assert_eq!(cfg, HostConfig::default());
    }

    #[test]
    fn test_parses_power_limit() {
        let dir = TempDir::new().unwrap();
        let hosts = dir.path().join("hosts");
        fs::create_dir_all(&hosts).unwrap();
        fs::write(hosts.join("ex44.json"), r#"{ "power_limit_watts": 65 }"#).unwrap();

        let cfg = HostConfig::load(dir.path(), "ex44").unwrap();
        assert_eq!(cfg.power_limit_watts, Some(65.0));
    }

    #[test]
    fn test_parse_power_limit_applies_the_same_rule() {
        assert_eq!(parse_power_limit("65").unwrap(), 65.0);
        assert_eq!(parse_power_limit("65.5").unwrap(), 65.5);
        for bad in ["0", "-1", "NaN", "inf", "-inf", "sixty"] {
            assert!(parse_power_limit(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn test_rejects_invalid_power_limit() {
        let dir = TempDir::new().unwrap();
        let hosts = dir.path().join("hosts");
        fs::create_dir_all(&hosts).unwrap();
        for json in [
            r#"{ "power_limit_watts": 0 }"#,
            r#"{ "power_limit_watts": -1 }"#,
            r#"{ "power_limit_watts": "65" }"#,
        ] {
            fs::write(hosts.join("bad.json"), json).unwrap();
            assert!(HostConfig::load(dir.path(), "bad").is_err(), "{json}");
        }
    }

    #[test]
    fn test_parses_affinity_and_aslr() {
        let dir = TempDir::new().unwrap();
        let hosts = dir.path().join("hosts");
        fs::create_dir_all(&hosts).unwrap();
        fs::write(
            hosts.join("pi5.json"),
            r#"{ "cpu_affinity": "0-2,4", "disable_aslr": false }"#,
        )
        .unwrap();

        let cfg = HostConfig::load(dir.path(), "pi5").unwrap();
        assert_eq!(cfg.cpu_affinity, CpuAffinity::Cpus(vec![0, 1, 2, 4]));
        assert!(!cfg.disable_aslr);
    }

    #[test]
    fn test_serde_cpu_affinity_round_trip() {
        let cfg = HostConfig {
            cpu_affinity: CpuAffinity::Cpus(vec![0, 1, 2, 4]),
            disable_aslr: false,
            ..HostConfig::default()
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("\"cpu_affinity\":\"0-2,4\""));
        assert!(json.contains("\"disable_aslr\":false"));

        let round_trip: HostConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(round_trip.cpu_affinity, cfg.cpu_affinity);
        assert!(!round_trip.disable_aslr);
    }

    #[test]
    fn test_cpu_affinity_parsing_matrix() {
        let cases = [
            ("\"3\"", Some(CpuAffinity::Cpus(vec![3])), None),
            (
                "\"4,2,0-1,2\"",
                Some(CpuAffinity::Cpus(vec![0, 1, 2, 4])),
                Some("\"0-2,4\""),
            ),
            (
                "\"0-2,4-5\"",
                Some(CpuAffinity::Cpus(vec![0, 1, 2, 4, 5])),
                Some("\"0-2,4,5\""),
            ),
            ("\"all\"", Some(CpuAffinity::All), None),
            ("null", None, None),
            ("", None, None),
            ("a", None, None),
            ("1", None, None),
            ("\"2-\"", None, None),
        ];

        for (json, expected, serialized_as) in cases {
            assert_eq!(
                serde_json::from_str::<CpuAffinity>(json).ok(),
                expected,
                "case={json}"
            );

            if let Some(v) = expected {
                let serialized_as = serialized_as.unwrap_or(json);
                assert_eq!(
                    serde_json::to_string(&v).unwrap(),
                    serialized_as,
                    "case={json}"
                );
            }
        }
    }

    fn write_host(dir: &TempDir, json: &str) {
        let hosts = dir.path().join("hosts");
        fs::create_dir_all(&hosts).unwrap();
        fs::write(hosts.join("h.json"), json).unwrap();
    }

    fn overrides_config_file(dir: &TempDir) -> ConfigFile {
        let json = r#"{
            "config_keys": {
                "commit": { "values": ["abc1234", "def5678"] },
                "threads": { "values": ["1", "n"] },
                "multiversion": { "values": ["default", "Scalar"] }
            },
            "benchmarks": [
                {
                    "benchmark": "b",
                    "command": ["builds/{commit}/b", "bench", "{threads}", "{multiversion}"],
                    "config": {
                        "commit": ["abc1234", "def5678"],
                        "threads": ["1", "n"],
                        "multiversion": ["default", "Scalar"]
                    }
                },
                { "benchmark": "plain", "command": ["true"], "config": {} }
            ]
        }"#;
        ConfigFile::from_str(dir.path(), Some("h"), json).unwrap()
    }

    #[test]
    fn test_config_overrides_first_match_applies() {
        let dir = TempDir::new().unwrap();
        write_host(
            &dir,
            r#"{
                "cpu_affinity": "0-1",
                "power_limit_watts": 65,
                "config_overrides": [
                    { "config": { "multiversion": "Scalar" } },
                    { "config": { "threads": "1" }, "cpu_affinity": "1" },
                    { "config": { "threads": "1", "multiversion": "default" }, "cpu_affinity": "0" },
                    { "config": { "threads": "n" }, "disable_aslr": false, "power_limit_watts": 50 }
                ]
            }"#,
        );
        let cfg = HostConfig::load(dir.path(), "h").unwrap();
        assert_eq!(cfg.config_overrides.len(), 4);
        let config_file = overrides_config_file(&dir);
        cfg.check_config_overrides(&config_file).unwrap();

        let settings = |s: &str| cfg.for_config(&config_file.config_from_string(s).unwrap());

        // The earlier threads=1 override wins over the more specific one after it
        let single = settings("commit=abc1234,threads=1,multiversion=default");
        assert_eq!(single.cpu_affinity, CpuAffinity::Cpus(vec![1]));
        assert!(single.disable_aslr);
        assert_eq!(single.power_limit_watts, Some(65.0));
        assert_eq!(single.config_overrides, Vec::new());

        // An override that sets nothing still stops the search
        let scalar = settings("commit=abc1234,threads=1,multiversion=Scalar");
        assert_eq!(scalar.cpu_affinity, CpuAffinity::Cpus(vec![0, 1]));

        let multi = settings("commit=def5678,threads=n,multiversion=default");
        assert_eq!(multi.cpu_affinity, CpuAffinity::Cpus(vec![0, 1]));
        assert!(!multi.disable_aslr);
        assert_eq!(multi.power_limit_watts, Some(50.0));

        // A case without the key never matches
        let plain = settings("");
        assert_eq!(plain.cpu_affinity, CpuAffinity::Cpus(vec![0, 1]));
        assert_eq!(plain.power_limit_watts, Some(65.0));
    }

    #[test]
    fn test_config_overrides_round_trip_keeps_order() {
        let json = r#"{"cpu_affinity":"0,1","disable_aslr":true,"config_overrides":[{"config":{"threads":"1"},"cpu_affinity":"1"},{"config":{"threads":"n"},"disable_aslr":false}]}"#;
        let cfg: HostConfig = serde_json::from_str(json).unwrap();
        assert_eq!(serde_json::to_string(&cfg).unwrap(), json);
        assert!(
            !serde_json::to_string(&HostConfig::default())
                .unwrap()
                .contains("config_overrides")
        );
    }

    #[test]
    fn test_rejects_invalid_config_overrides_on_load() {
        let dir = TempDir::new().unwrap();
        for json in [
            r#"{ "config_overrides": [{ "config": {}, "cpu_affinity": "1" }] }"#,
            r#"{ "config_overrides": [{ "cpu_affinity": "1" }] }"#,
            r#"{ "config_overrides": [{ "config": { "threads": "1" }, "cpus": "1" }] }"#,
            r#"{ "config_overrides": [{ "config": { "threads": "1" }, "description": "x" }] }"#,
            r#"{ "config_overrides": [{ "config": { "threads": "1" }, "cpu_affinity": "2-" }] }"#,
            r#"{ "config_overrides": [{ "config": { "threads": "1" }, "power_limit_watts": 0 }] }"#,
        ] {
            write_host(&dir, json);
            assert!(HostConfig::load(dir.path(), "h").is_err(), "{json}");
        }
    }

    #[test]
    fn test_check_config_overrides_rejects_unsafe_keys() {
        let dir = TempDir::new().unwrap();
        let config_file = overrides_config_file(&dir);
        for (config, reason) in [
            (r#"{ "thread": "1" }"#, "unknown config key \"thread\""),
            (
                r#"{ "threads": "2" }"#,
                "\"2\" is not a value of \"threads\"",
            ),
            (r#"{ "host": "h" }"#, "\"host\" is the host key"),
            (
                r#"{ "commit": "abc1234" }"#,
                "\"commit\" is not in the command arguments of b",
            ),
        ] {
            let json = format!(r#"{{ "config_overrides": [{{ "config": {config} }}] }}"#);
            let cfg: HostConfig = serde_json::from_str(&json).unwrap();
            match cfg.check_config_overrides(&config_file) {
                Err(HostConfigError::Override {
                    index: 0,
                    reason: r,
                }) => {
                    assert_eq!(r, reason, "{config}");
                }
                other => panic!("{config}: {other:?}"),
            }
        }
    }

    #[test]
    fn test_check_config_overrides_reports_first_override_using_key() {
        let dir = TempDir::new().unwrap();
        let config_file = overrides_config_file(&dir);
        let cfg: HostConfig = serde_json::from_str(
            r#"{ "config_overrides": [
                { "config": { "threads": "1" } },
                { "config": { "commit": "abc1234" } },
                { "config": { "commit": "def5678", "threads": "n" } }
            ] }"#,
        )
        .unwrap();
        match cfg.check_config_overrides(&config_file) {
            Err(HostConfigError::Override { index: 1, reason }) => {
                assert_eq!(reason, "\"commit\" is not in the command arguments of b");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn test_parses_description() {
        let dir = TempDir::new().unwrap();
        let hosts = dir.path().join("hosts");
        fs::create_dir_all(&hosts).unwrap();
        fs::write(
            hosts.join("h1.json"),
            r#"{ "description": "[Example](https://example.com) instance." }"#,
        )
        .unwrap();

        let cfg = HostConfig::load(dir.path(), "h1").unwrap();
        assert_eq!(
            cfg.description.as_deref(),
            Some("[Example](https://example.com) instance.")
        );

        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("\"description\":"));

        // Without description, field is omitted
        let no_desc = HostConfig::default();
        let json = serde_json::to_string(&no_desc).unwrap();
        assert!(!json.contains("description"));
    }
}
