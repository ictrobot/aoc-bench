use serde::{Deserialize, Serialize};
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

        Ok(parsed)
    }
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            cpu_affinity: CpuAffinity::All,
            disable_aslr: default_disable_aslr(),
            description: None,
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
            description: None,
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
