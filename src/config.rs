// Config management: parse config.json, expand Cartesian products, validate constraints

use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::{fs, io};

/// Parsed and validated configuration file
///
/// Cheap to clone (uses Arc internally).
#[derive(Debug)]
pub struct ConfigFile {
    /// Data directory
    data_dir: Arc<Path>,
    /// Config key definitions (sorted)
    config_keys: Arc<[Key]>,
    /// Benchmark definitions
    benchmarks: Arc<[Benchmark]>,
    /// Host key
    host_key: Key,
}

impl ConfigFile {
    const CONFIG_JSON_PATH: &'static str = "config.json";
    const INPUTS_DIR: &'static str = "inputs";
    const RESULTS_DIR: &'static str = "results";

    /// Load the config in the provided data directory.
    pub fn new(data_dir: &Path, current_host: Option<&str>) -> Result<Self, ConfigError> {
        let path = data_dir.join(Self::CONFIG_JSON_PATH);
        Self::from_str(
            data_dir,
            current_host,
            &fs::read_to_string(&path).map_err(|error| ConfigError::Io { path, error })?,
        )
    }

    /// Create a [`ConfigFile`] from a JSON string
    pub fn from_str(
        data_dir: &Path,
        current_host: Option<&str>,
        json_str: &str,
    ) -> Result<Self, ConfigError> {
        #[derive(Deserialize)]
        struct ConfigFileJson {
            config_keys: HashMap<String, ConfigKeyDef>,
            benchmarks: Vec<BenchmarkDef>,
        }

        #[derive(Deserialize)]
        struct ConfigKeyDef {
            values: Vec<String>,
            #[serde(default)]
            presets: HashMap<String, Vec<String>>,
        }

        #[derive(Deserialize)]
        struct BenchmarkDef {
            benchmark: String,
            command: Vec<String>,
            #[serde(default)]
            input: Option<String>,
            #[serde(default)]
            checksum: Option<String>,
            config: HashMap<String, ConfigSpec>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ConfigSpec {
            Preset(String),
            Literal(Vec<String>),
        }

        let json: ConfigFileJson = serde_json::from_str(json_str)?;

        let mut config_keys = Vec::new();
        let mut key_map = HashMap::new();

        // Parse and validate config keys
        for (key_name, key_def) in json.config_keys {
            let key = Key::new(key_name.clone(), key_def.values)?;

            // Parse presets
            let mut presets = HashMap::new();
            for (preset_name, preset_values) in key_def.presets {
                presets.insert(
                    preset_name,
                    key.subset_from_names(preset_values.iter().map(String::as_str))?,
                );
            }

            config_keys.push(key.clone());
            key_map.insert(key_name, (key, presets));
        }

        // Parse benchmarks
        let mut benchmarks = Vec::new();
        for bench_def in json.benchmarks {
            // Parse config expansion
            let mut subsets = Vec::new();
            for (key_name, spec) in bench_def.config {
                let (key, presets) = key_map
                    .get(&key_name)
                    .ok_or_else(|| ConfigError::UnknownKey(key_name.clone()))?;

                subsets.push(match spec {
                    ConfigSpec::Preset(preset_name) => presets
                        .get(&preset_name)
                        .ok_or_else(|| ConfigError::UnknownPreset {
                            key: key_name.clone(),
                            preset: preset_name,
                        })?
                        .clone(),
                    ConfigSpec::Literal(values) => {
                        key.subset_from_names(values.iter().map(String::as_str))?
                    }
                });
            }

            if bench_def.command.is_empty() {
                return Err(ConfigError::EmptyBenchmarkCommand(bench_def.benchmark));
            }

            let input =
                if let Some(name) = bench_def.input {
                    let path = data_dir.join(Self::INPUTS_DIR).join(&name);
                    // std::fs::canonicalize is documented to return an error if the path does not exist
                    let path = fs::canonicalize(&path)
                        .map_err(|error| ConfigError::MissingInput { name, path, error })?;
                    Some(path)
                } else {
                    None
                };

            benchmarks.push(Benchmark::new(
                bench_def.benchmark.try_into()?,
                ConfigProduct::new(subsets),
                bench_def.command,
                input,
                bench_def.checksum,
            )?);
        }

        let host_key = Key::new_host_key(&data_dir.join(Self::RESULTS_DIR), current_host)?;
        config_keys.push(host_key.clone());
        config_keys.sort_unstable();

        Ok(ConfigFile {
            data_dir: data_dir.into(),
            config_keys: config_keys.into(),
            benchmarks: benchmarks.into(),
            host_key,
        })
    }

    /// Get the path to the data directory
    #[must_use]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Get all config keys
    #[must_use]
    pub fn config_keys(&self) -> &[Key] {
        &self.config_keys
    }

    /// Get all benchmarks
    #[must_use]
    pub fn benchmarks(&self) -> &[Benchmark] {
        &self.benchmarks
    }

    /// Get the host key
    #[must_use]
    pub fn host_key(&self) -> &Key {
        &self.host_key
    }

    /// Look up a [`Key`] by its string name, returning None if not found
    #[must_use]
    pub fn key_from_name(&self, name: &str) -> Option<&Key> {
        let index = self
            .config_keys
            .binary_search_by_key(&name, |k| k.name())
            .ok()?;
        Some(&self.config_keys[index])
    }

    /// Parse a config string into a [`Config`] in the context of this `ConfigFile`.
    ///
    /// Returns an error if the format is invalid, or the keys or values keys are unknown.
    pub fn config_from_string(&self, s: &str) -> Result<Config, ConfigError> {
        if s.is_empty() {
            return Ok(Config::new());
        }

        self.config_from_impl(s.split(',').map(|pair| {
            pair.split_once('=')
                .ok_or_else(|| ConfigError::InvalidConfigString(s.to_string()))
        }))
    }

    /// Convert a [`BTreeMap<String, String>`] into a [`Config`] in the context of this `ConfigFile`.
    ///
    /// Returns an error if any key or value is unknown.
    pub fn config_from_map(&self, map: &BTreeMap<String, String>) -> Result<Config, ConfigError> {
        self.config_from_impl(
            map.iter()
                .map(|(key, value)| Ok((key.as_str(), value.as_str()))),
        )
    }

    fn config_from_impl<'a>(
        &self,
        pairs: impl Iterator<Item = Result<(&'a str, &'a str), ConfigError>>,
    ) -> Result<Config, ConfigError> {
        let mut config = Config::new();
        for pair in pairs {
            let (key, value) = pair?;

            let key = self
                .key_from_name(key)
                .ok_or_else(|| ConfigError::UnknownKey(key.to_string()))?;

            config.kv.push(key.value_from_name(value).ok_or_else(|| {
                ConfigError::UnknownValueForKey {
                    key: key.to_string(),
                    value: value.to_string(),
                }
            })?);
        }
        config.kv.sort_unstable();
        Ok(config)
    }
}

/// A configuration key with its set of valid values.
///
/// Cheap to clone (uses Arc internally).
#[derive(Clone)]
pub struct Key(Arc<KeyInner>);
struct KeyInner {
    name: Arc<str>,
    values: Vec<Arc<str>>,
    name_to_idx: HashMap<Arc<str>, usize>,
}

impl Key {
    /// The name of the special config key that specifies the host the benchmark was ran on.
    pub const HOST_KEY_NAME: &'static str = "host";
    /// The set of config keys that cannot be configured by the user.
    pub const DISALLOWED_CONFIG_KEY_NAMES: &'static [&'static str] = &[
        "bench", // Could be confused with the benchmarks name
        "benchmark",
        "host",      // Cannot be configured by the user
        "timestamp", // Could be confused with the run series timestamp
    ];

    /// Create a new configuration key with the given name and valid values.
    ///
    /// Validates that the key name matches `[a-z][a-z0-9_]+` and values match `[a-zA-Z0-9_-]+`.
    /// Returns an error if values are empty, contain duplicates, or fail validation.
    pub fn new(name: String, values: Vec<String>) -> Result<Self, ConfigError> {
        Self::validate_key_name(&name)?;

        let values = values
            .into_iter()
            .map(|s| {
                Self::validate_value(&s)?;
                Ok(Arc::from(s))
            })
            .collect::<Result<Vec<Arc<str>>, ConfigError>>()?;

        let mut name_to_idx = HashMap::with_capacity(values.len());
        for (idx, value) in values.iter().enumerate() {
            if name_to_idx.insert(value.clone(), idx).is_some() {
                return Err(ConfigError::DuplicateValue {
                    key: name,
                    value: value.to_string(),
                });
            }
        }

        Ok(Self(Arc::new(KeyInner {
            name: Arc::from(name),
            values,
            name_to_idx,
        })))
    }

    fn new_host_key(results_dir: &Path, current_host: Option<&str>) -> Result<Self, ConfigError> {
        let mut values: Vec<Arc<str>> = Vec::new();
        if let Some(current_host) = current_host {
            Self::validate_value(current_host)
                .map_err(|_| ConfigError::InvalidHost(current_host.into()))?;
            values.push(Arc::from(current_host));
        }

        match fs::read_dir(results_dir) {
            Ok(dir) => {
                for entry in dir {
                    let entry = entry.map_err(|error| ConfigError::Io {
                        path: results_dir.to_path_buf(),
                        error,
                    })?;

                    let path = entry.path();
                    if path.is_dir()
                        && let Some(name) = path.file_name()
                        && name != OsStr::new(current_host.unwrap_or_default())
                    {
                        let name = name.to_string_lossy().into_owned();
                        Self::validate_value(&name).map_err(|_| {
                            ConfigError::InvalidHostAtPath {
                                host: name.clone(),
                                path,
                            }
                        })?;
                        values.push(Arc::from(name));
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(ConfigError::Io {
                    path: results_dir.to_path_buf(),
                    error,
                });
            }
        }

        values.sort_unstable();

        let mut name_to_idx = HashMap::with_capacity(values.len());
        for (idx, value) in values.iter().enumerate() {
            // Filesystem names must be unique, and the loop checks for current host matches
            assert_eq!(name_to_idx.insert(value.clone(), idx), None);
        }

        Ok(Self(Arc::new(KeyInner {
            name: Arc::from(Self::HOST_KEY_NAME),
            values,
            name_to_idx,
        })))
    }

    /// Get the key's name
    #[must_use]
    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// Get the number of valid values for this key
    #[must_use]
    pub fn values_len(&self) -> usize {
        self.0.values.len()
    }

    /// Iterate over all valid values for this key as [`KeyValue`] instances
    pub fn values(&self) -> impl Iterator<Item = KeyValue> + use<> {
        let key = self.clone();
        (0..self.0.values.len()).map(move |index| KeyValue {
            index,
            key: key.clone(),
        })
    }

    /// Look up a [`KeyValue`] by its string name, returning None if not found
    #[must_use]
    pub fn value_from_name(&self, name: &str) -> Option<KeyValue> {
        Some(KeyValue {
            key: self.clone(),
            index: *self.0.name_to_idx.get(name)?,
        })
    }

    /// Create a subset containing only the specified values by name.
    ///
    /// Returns an error if any name is not a valid value for this key, if the list is empty, or
    /// contains duplicates.
    pub fn subset_from_names<'a>(
        &self,
        names: impl Iterator<Item = &'a str>,
    ) -> Result<KeyValuesSubset, ConfigError> {
        let indexes = names
            .map(|s| {
                self.0
                    .name_to_idx
                    .get(s)
                    .ok_or_else(|| ConfigError::UnknownValueForKey {
                        key: self.0.name.to_string(),
                        value: s.to_string(),
                    })
                    .copied()
            })
            .collect::<Result<Vec<_>, _>>()?;

        KeyValuesSubset::new(self.clone(), indexes)
    }

    /// Validates a config key name
    fn validate_key_name(key: &str) -> Result<(), ConfigError> {
        if key.is_empty() {
            return Err(ConfigError::InvalidKeyName(key.to_string()));
        }

        let mut chars = key.chars();
        let first = chars.next().unwrap();

        if !first.is_ascii_lowercase() {
            return Err(ConfigError::InvalidKeyName(key.to_string()));
        }

        for c in chars {
            if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '_' {
                return Err(ConfigError::InvalidKeyName(key.to_string()));
            }
        }

        if Self::DISALLOWED_CONFIG_KEY_NAMES.contains(&key) {
            return Err(ConfigError::InvalidKeyName(key.to_string()));
        }

        Ok(())
    }

    /// Validates a config value
    pub(crate) fn validate_value(value: &str) -> Result<(), ConfigError> {
        if value.is_empty() {
            return Err(ConfigError::InvalidValue(value.to_string()));
        }

        for c in value.chars() {
            if !c.is_ascii_alphanumeric() && c != '_' && c != '-' {
                return Err(ConfigError::InvalidValue(value.to_string()));
            }
        }

        Ok(())
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        // Order by name first, then by Arc pointer. Ordering::Equal will only be returned if both
        // match, which is consistent with the eq implementation as if the pointers are equal, the
        // names must be equal within
        self.name()
            .cmp(other.name())
            .then_with(|| Arc::as_ptr(&self.0).cmp(&Arc::as_ptr(&other.0)))
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).hash(state);
    }
}

impl Debug for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // New type wrapper to show indexes of values
        struct Values<'a>(&'a [Arc<str>]);
        impl Debug for Values<'_> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_map().entries(self.0.iter().enumerate()).finish()
            }
        }

        f.debug_struct("ConfigKey")
            .field("name", &self.0.name)
            .field("values", &Values(&self.0.values))
            .finish()
    }
}

impl Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0.name, f)
    }
}

/// A specific value for a configuration key.
///
/// Cheap to clone (Key uses Arc internally).
/// Ordered first by key, then by value index.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct KeyValue {
    key: Key, // `key` must be first for sort order
    index: usize,
}

impl KeyValue {
    /// Get the configuration key
    #[must_use]
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Get the index of this value within its key's value list
    #[must_use]
    pub fn value_index(&self) -> usize {
        self.index
    }

    /// Get the string name of this value
    #[must_use]
    pub fn value_name(&self) -> &str {
        &self.key.0.values[self.index]
    }
}

impl Debug for KeyValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigValue")
            .field("key", &self.key.name())
            .field("value", &self.value_name())
            .field("index", &self.index)
            .finish()
    }
}

impl Display for KeyValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.key.name(), self.value_name())
    }
}

/// A subset of valid values for a key, used in [`ConfigProduct`] for Cartesian product iteration.
///
/// Cheap to clone (uses Arc internally).
#[derive(Clone, Debug)]
pub struct KeyValuesSubset {
    key: Key,
    indexes: Arc<[usize]>,
}

impl KeyValuesSubset {
    /// Create a new subset with the given value indexes.
    ///
    /// Indexes are sorted and validated for uniqueness and bounds.
    /// Returns an error if the list is empty or contains duplicates.
    fn new(key: Key, mut indexes: Vec<usize>) -> Result<Self, ConfigError> {
        if indexes.is_empty() {
            return Err(ConfigError::EmptyValues(key.name().to_string()));
        }

        indexes.sort_unstable();

        // Check that indexes are in bounds. This is an assert, not a ConfigError, as this method is
        // private and should only be called with valid indexes.
        assert!(indexes[indexes.len() - 1] < key.0.values.len());

        if let Some(pair) = indexes.windows(2).find(|w| w[0] == w[1]) {
            return Err(ConfigError::DuplicateValue {
                key: key.name().to_string(),
                value: key.0.values[pair[0]].to_string(),
            });
        }

        Ok(Self {
            key,
            indexes: indexes.into(),
        })
    }
}

/// A configuration key-value map with canonical ordering.
///
/// Keys are stored sorted by [`Key`] for efficient binary search.
///
/// Use [`ConfigFile::config_from_string`] to parse a Config from a string like `"key1=a,key2=b"`.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Config {
    kv: Vec<KeyValue>,
}

impl Config {
    /// Create a new empty config
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Get a value by key
    #[must_use]
    pub fn get(&self, key: &Key) -> Option<&KeyValue> {
        match self.kv.binary_search_by_key(&key, |kv| &kv.key) {
            Ok(idx) => Some(&self.kv[idx]),
            Err(_) => None,
        }
    }

    /// Get a [`KeyValue`] by its string key name
    #[must_use]
    pub fn get_by_name(&self, name: &str) -> Option<&KeyValue> {
        self.kv
            .binary_search_by(|kv| kv.key.name().cmp(name))
            .ok()
            .map(|idx| &self.kv[idx])
    }

    /// Get number of entries
    #[must_use]
    pub fn len(&self) -> usize {
        self.kv.len()
    }

    /// Check if config is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.kv.is_empty()
    }

    /// Iterate over key-value pairs
    pub fn iter(&self) -> impl Iterator<Item = &KeyValue> {
        self.kv.iter()
    }

    /// Clone this config with the specified key value pair.
    #[must_use]
    pub fn with(&self, kv: KeyValue) -> Self {
        let mut clone = self.kv.clone();
        match clone.binary_search_by_key(&&kv.key, |kv| &kv.key) {
            Ok(i) => clone[i] = kv,
            Err(i) => clone.insert(i, kv),
        }
        Self { kv: clone }
    }

    /// Clone this config without the specified key.
    #[must_use]
    pub fn without_key(&self, key: &Key) -> Self {
        let kv = self
            .kv
            .iter()
            .filter(|kv| &kv.key != key)
            .cloned()
            .collect();
        Self { kv }
    }

    /// Expand a template by replacing `{key}` placeholders with config values.
    ///
    /// Returns an error if a placeholder references an unknown key.
    pub fn expand_template(&self, template: &str) -> Result<String, ConfigError> {
        let mut result = String::with_capacity(template.len());
        let mut last_end = 0;

        // Find all {key} placeholders
        for (start, _) in template.match_indices('{') {
            if let Some(end_offset) = template[start..].find('}') {
                let end = start + end_offset;
                let key = &template[start + 1..end];

                let Ok(index) = self.kv.binary_search_by_key(&key, |kv| kv.key.name()) else {
                    return Err(ConfigError::UnknownKeyInPlaceholder(key.to_string()));
                };
                let value = self.kv[index].value_name();

                // Append text before placeholder
                result.push_str(&template[last_end..start]);
                // Append value
                result.push_str(value);
                last_end = end + 1;
            }
        }

        // Append remaining text
        result.push_str(&template[last_end..]);

        Ok(result)
    }

    /// Expand a list of templates.
    ///
    /// See [`expand_template`] for details on placeholder syntax.
    pub fn expand_templates(&self, templates: &[String]) -> Result<Vec<String>, ConfigError> {
        templates
            .iter()
            .map(|template| self.expand_template(template))
            .collect()
    }
}

impl Debug for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.kv, f)
    }
}

impl Display for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (i, kv) in self.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            Display::fmt(kv, f)?;
        }
        Ok(())
    }
}

impl Serialize for Config {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for kv in self.iter() {
            map.serialize_entry(&kv.key.name(), &kv.value_name())?;
        }
        map.end()
    }
}

impl From<Config> for BTreeMap<String, String> {
    fn from(value: Config) -> Self {
        let mut map = BTreeMap::new();
        for kv in value.iter() {
            map.insert(kv.key.name().to_string(), kv.value_name().to_string());
        }
        map
    }
}

/// Specification for a Cartesian product of key-value subsets.
///
/// Subsets are stored sorted by [`Key`] for efficient binary search.
#[derive(Clone, Debug, Default)]
pub struct ConfigProduct {
    subsets: Vec<KeyValuesSubset>,
}

impl ConfigProduct {
    /// Create a new config product from key-value subsets.
    #[must_use]
    pub fn new(mut subsets: Vec<KeyValuesSubset>) -> Self {
        subsets.sort_unstable_by(|a, b| a.key.cmp(&b.key));
        Self { subsets }
    }

    /// Filter this product to only include combinations matching the given config.
    ///
    /// Returns None if any key-value in config is not present in this product.
    #[must_use]
    pub fn filter(&self, config: &Config) -> Option<ConfigProduct> {
        let mut subsets = self.subsets.clone();
        for kv in config.iter() {
            // Check if key exists in subsets
            let index = subsets
                .binary_search_by_key(&&kv.key, |subset| &subset.key)
                .ok()?;
            // Check if value is in subset
            subsets[index]
                .indexes
                .binary_search(&kv.value_index())
                .ok()?;
            // Set that subset to only contain that value
            subsets[index].indexes = Arc::new([kv.value_index()]);
        }
        // Subset keys haven't changed, vec must still be sorted
        Some(Self { subsets })
    }

    /// Get the total number of configs in the Cartesian product
    #[must_use]
    #[allow(clippy::len_without_is_empty, reason = "len is always > 0")]
    pub fn len(&self) -> usize {
        // Always check for overflow as the number of combinations can grow arbitrarily large
        self.subsets
            .iter()
            .fold(1usize, |len, subset| len.strict_mul(subset.indexes.len()))
    }

    /// Iterate over all configs in the Cartesian product lazily
    #[must_use]
    pub fn iter(&self) -> ConfigProductIter<'_> {
        ConfigProductIter {
            subsets: &self.subsets,
            indexes: vec![0; self.subsets.len()],
            len: self.len(),
        }
    }
}

impl<'a> IntoIterator for &'a ConfigProduct {
    type Item = Config;
    type IntoIter = ConfigProductIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator over the Cartesian product of configuration values
pub struct ConfigProductIter<'a> {
    subsets: &'a [KeyValuesSubset],
    indexes: Vec<usize>,
    len: usize,
}

impl Iterator for ConfigProductIter<'_> {
    type Item = Config;

    fn next(&mut self) -> Option<Self::Item> {
        if self.len == 0 {
            return None;
        }

        let mut config = Config::new();
        config.kv.reserve(self.subsets.len());
        let mut carry = true;
        for (subset, i) in self.subsets.iter().zip(self.indexes.iter_mut()) {
            config.kv.push(KeyValue {
                key: subset.key.clone(),
                index: subset.indexes[*i],
            });

            if carry {
                *i += 1;
                if *i >= subset.indexes.len() {
                    *i = 0;
                } else {
                    carry = false;
                }
            }
        }

        self.len -= 1;

        // If carry is still true, we've exhausted all combinations
        if carry {
            assert_eq!(self.len, 0);
        }

        Some(config)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl ExactSizeIterator for ConfigProductIter<'_> {
    fn len(&self) -> usize {
        self.len
    }
}

/// A benchmark with its command template and config expansion
#[derive(Debug)]
pub struct Benchmark {
    id: BenchmarkId,
    config: ConfigProduct,
    command_template: Vec<String>,
    input: Option<PathBuf>,
    checksum: Option<String>,
}

impl Benchmark {
    /// Create a new benchmark.
    ///
    /// Validates that the command template can be expanded with the first config
    /// in the product (all placeholders must have corresponding keys).
    pub fn new(
        id: BenchmarkId,
        config: ConfigProduct,
        command_template: Vec<String>,
        input: Option<PathBuf>,
        checksum: Option<String>,
    ) -> Result<Self, ConfigError> {
        // Check that the command template can be expanded
        let first_config = config.iter().next().unwrap();
        for placeholder in &command_template {
            first_config.expand_template(placeholder)?;
        }

        Ok(Self {
            id,
            config,
            command_template,
            input,
            checksum,
        })
    }

    /// Get benchmark identifier
    #[must_use]
    pub fn id(&self) -> &BenchmarkId {
        &self.id
    }

    /// Get the config product specification for this benchmark
    #[must_use]
    pub fn config(&self) -> &ConfigProduct {
        &self.config
    }

    /// Get command template with {key} placeholders
    #[must_use]
    pub fn command_template(&self) -> &[String] {
        &self.command_template
    }

    /// Get input file path if any
    #[must_use]
    pub fn input(&self) -> Option<&Path> {
        self.input.as_deref()
    }

    /// Get expected checksum if any
    #[must_use]
    pub fn checksum(&self) -> Option<&str> {
        self.checksum.as_deref()
    }
}

/// New type wrapper for valid benchmark identifiers.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BenchmarkId(Box<str>);

impl BenchmarkId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(value: &str) -> Result<(), ConfigError> {
        Key::validate_value(value).map_err(|_| ConfigError::InvalidBenchmarkId(value.to_string()))
    }
}

impl From<BenchmarkId> for String {
    fn from(value: BenchmarkId) -> Self {
        value.0.into_string()
    }
}

impl TryFrom<String> for BenchmarkId {
    type Error = ConfigError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::validate(&value)?;
        Ok(Self(value.into_boxed_str()))
    }
}

impl TryFrom<&str> for BenchmarkId {
    type Error = ConfigError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::validate(value)?;
        Ok(Self(value.into()))
    }
}

impl FromStr for BenchmarkId {
    type Err = ConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.try_into()
    }
}

impl Deref for BenchmarkId {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Debug for BenchmarkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl Display for BenchmarkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Error type for configuration operations
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("I/O error at '{path:?}': {error}")]
    Io {
        path: PathBuf,
        #[source]
        error: io::Error,
    },
    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Invalid key name '{0}': must match [a-z][a-z0-9_]+")]
    InvalidKeyName(String),
    #[error("Invalid value '{0}': must match [a-zA-Z0-9_-]+")]
    InvalidValue(String),
    #[error("Invalid benchmark id '{0}': must match [a-zA-Z0-9_-]+")]
    InvalidBenchmarkId(String),
    #[error("Duplicate value '{value}' in values array for '{key}': values must be unique")]
    DuplicateValue { key: String, value: String },
    #[error("Repeated value '{value}' in subset for '{key}'")]
    RepeatedValue { key: String, value: String },
    #[error("Empty values for key '{0}': present keys must have at least one value")]
    EmptyValues(String),
    #[error("Unknown preset '{preset}' for key '{key}'")]
    UnknownPreset { key: String, preset: String },
    #[error("Unknown config key '{0}'")]
    UnknownKey(String),
    #[error("Unknown config key '{0}' in command placeholder")]
    UnknownKeyInPlaceholder(String),
    #[error("Value '{value}' not in valid values for key '{key}'")]
    UnknownValueForKey { key: String, value: String },
    #[error("Invalid config string: '{0}'")]
    InvalidConfigString(String),
    #[error("Empty benchmark command for '{0}'")]
    EmptyBenchmarkCommand(String),
    #[error("Input file '{name}' not found at '{path:?}': {error}")]
    MissingInput {
        name: String,
        path: PathBuf,
        #[source]
        error: io::Error,
    },
    #[error("Invalid host '{0}': must match the format for config values [a-zA-Z0-9_-]+")]
    InvalidHost(String),
    #[error("Invalid host '{host}' in data directory at '{path:?}'")]
    InvalidHostAtPath { host: String, path: PathBuf },
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_key_validation() {
        // Valid key names
        assert!(Key::new("commit".to_string(), vec!["a".to_string()]).is_ok());
        assert!(Key::new("build_type".to_string(), vec!["a".to_string()]).is_ok());
        assert!(Key::new("t123".to_string(), vec!["a".to_string()]).is_ok());

        // Invalid key names
        assert!(Key::new(String::new(), vec!["a".to_string()]).is_err());
        assert!(Key::new("Commit".to_string(), vec!["a".to_string()]).is_err());
        assert!(Key::new("123".to_string(), vec!["a".to_string()]).is_err());
        assert!(Key::new("build-type".to_string(), vec!["a".to_string()]).is_err());
        assert!(Key::new("build type".to_string(), vec!["a".to_string()]).is_err());
        assert!(Key::new("host".to_string(), vec!["a".to_string()]).is_err());
        assert!(Key::new("bench".to_string(), vec!["a".to_string()]).is_err());
        assert!(Key::new("benchmark".to_string(), vec!["a".to_string()]).is_err());
        assert!(Key::new("timestamp".to_string(), vec!["a".to_string()]).is_err());
    }

    #[test]
    fn test_value_validation() {
        // Valid values
        assert!(Key::new("test".to_string(), vec!["abc123".to_string()]).is_ok());
        assert!(Key::new("test".to_string(), vec!["build_type".to_string()]).is_ok());
        assert!(Key::new("test".to_string(), vec!["build-type".to_string()]).is_ok());
        assert!(Key::new("test".to_string(), vec!["ABC123".to_string()]).is_ok());

        // Invalid values
        assert!(Key::new("test".to_string(), vec![String::new()]).is_err());
        assert!(Key::new("test".to_string(), vec!["build type".to_string()]).is_err());
        assert!(Key::new("test".to_string(), vec!["build/type".to_string()]).is_err());
    }

    #[test]
    fn test_key_duplicate_values() {
        let err = Key::new(
            "test".to_string(),
            vec!["val1".to_string(), "val2".to_string(), "val1".to_string()],
        )
        .unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateValue { .. }));
    }

    #[test]
    fn test_key_interning() {
        let key1 = Key::new("test".to_string(), vec!["a".to_string()]).unwrap();
        let key2 = key1.clone();

        // Keys should share the same Arc
        assert!(Arc::ptr_eq(&key1.0, &key2.0));
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_config_file_loading() {
        let json = r#"{
            "config_keys": {
                "build": {
                    "values": ["debug", "release"],
                    "presets": {
                        "all": ["debug", "release"]
                    }
                },
                "threads": {
                    "values": ["1", "2", "4"]
                }
            },
            "benchmarks": [
                {
                    "benchmark": "test-bench",
                    "command": ["bin/{build}", "run", "{threads}"],
                    "input": "test.txt",
                    "checksum": "abc123",
                    "config": {
                        "build": "all",
                        "threads": ["1", "4"]
                    }
                }
            ]
        }"#;

        let temp_dir = TempDir::new().unwrap();

        // Should error as input is missing
        let result = ConfigFile::from_str(temp_dir.path(), None, json);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConfigError::MissingInput { .. }
        ));

        // Create the input file
        let inputs_dir = temp_dir.path().join("inputs");
        fs::create_dir_all(&inputs_dir).unwrap();
        let input_path = inputs_dir.join("test.txt");
        fs::write(&input_path, []).unwrap();
        let input_path = fs::canonicalize(&input_path).unwrap();

        // Should succeed now the input file exists
        let config_file = ConfigFile::from_str(temp_dir.path(), None, json).unwrap();
        assert_eq!(config_file.benchmarks().len(), 1);
        assert_eq!(config_file.data_dir(), temp_dir.path());

        let bench = &config_file.benchmarks()[0];
        assert_eq!(bench.id().as_str(), "test-bench");
        assert_eq!(
            bench.command_template(),
            vec!["bin/{build}", "run", "{threads}"]
        );
        assert_eq!(bench.input(), Some(input_path.as_path()));
        assert_eq!(bench.checksum(), Some("abc123"));
    }

    #[test]
    fn test_config_expansion() {
        let json = r#"{
            "config_keys": {
                "build": {
                    "values": ["debug", "release"]
                },
                "threads": {
                    "values": ["1", "2"]
                }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["test"],
                    "config": {
                        "build": ["debug", "release"],
                        "threads": ["1", "2"]
                    }
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let bench = &config_file.benchmarks()[0];

        // Should expand to 2 x 2 = 4 configs
        assert_eq!(bench.config().len(), 4);

        let configs: Vec<_> = bench.config().iter().collect();
        assert_eq!(configs.len(), 4);

        // Verify all combinations exist (sorted by key)
        let config_strings: Vec<String> = configs.iter().map(Config::to_string).collect();

        assert!(config_strings.contains(&"build=debug,threads=1".to_string()));
        assert!(config_strings.contains(&"build=debug,threads=2".to_string()));
        assert!(config_strings.contains(&"build=release,threads=1".to_string()));
        assert!(config_strings.contains(&"build=release,threads=2".to_string()));
    }

    #[test]
    fn test_preset_expansion() {
        let json = r#"{
            "config_keys": {
                "build": {
                    "values": ["debug", "release", "profile"],
                    "presets": {
                        "optimized": ["release", "profile"]
                    }
                }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["test"],
                    "config": {
                        "build": "optimized"
                    }
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let bench = &config_file.benchmarks()[0];

        assert_eq!(bench.config().len(), 2);

        let configs: Vec<_> = bench.config().iter().collect();
        assert_eq!(configs.len(), 2);

        let config_strings: Vec<String> = configs.iter().map(Config::to_string).collect();

        assert!(config_strings.contains(&"build=release".to_string()));
        assert!(config_strings.contains(&"build=profile".to_string()));
    }

    #[test]
    fn test_command_templating() {
        let json = r#"{
            "config_keys": {
                "build": { "values": ["debug"] },
                "threads": { "values": ["4"] }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["bin/{build}/bench", "{threads}"],
                    "config": {
                        "build": ["debug"],
                        "threads": ["4"]
                    }
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let bench = &config_file.benchmarks()[0];

        let mut configs = bench.config().iter();
        let config = configs.next().unwrap();

        let expanded = config.expand_templates(bench.command_template()).unwrap();
        assert_eq!(expanded, vec!["bin/debug/bench", "4"]);
    }

    #[test]
    fn test_config_string_parsing() {
        let json = r#"{
            "config_keys": {
                "build": { "values": ["debug", "release"] },
                "threads": { "values": ["1", "n"] },
                "other": { "values": ["a", "b"] }
            },
            "benchmarks": []
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();

        // Test parsing valid config string
        let config = config_file
            .config_from_string("build=debug,threads=n")
            .unwrap();
        assert_eq!(config.len(), 2);

        // Find the build key and check value
        let build_key = config_file.key_from_name("build").unwrap();
        assert_eq!(config.get(build_key).unwrap().value_name(), "debug");

        // Find the threads key and check value
        let threads_key = config_file.key_from_name("threads").unwrap();
        assert_eq!(config.get(threads_key).unwrap().value_name(), "n");

        // Find the other key and check value
        let other_key = config_file.key_from_name("other").unwrap();
        assert!(config.get(other_key).is_none());

        // Test round-trip
        let config_str = config.to_string();
        assert_eq!(config_str, "build=debug,threads=n");

        // Test empty string
        let empty = config_file.config_from_string("").unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.to_string(), "");
    }

    #[test]
    fn test_invalid_configs() {
        let tmp_dir = TempDir::new().unwrap();

        // Invalid key name
        let json = r#"{
            "config_keys": {
                "Build": { "values": ["debug"] }
            },
            "benchmarks": []
        }"#;
        let err = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidKeyName(_)));
        assert_eq!(
            err.to_string(),
            "Invalid key name 'Build': must match [a-z][a-z0-9_]+"
        );

        // Invalid value
        let json = r#"{
            "config_keys": {
                "build": { "values": ["debug release"] }
            },
            "benchmarks": []
        }"#;
        let err = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidValue(_)));
        assert_eq!(
            err.to_string(),
            "Invalid value 'debug release': must match [a-zA-Z0-9_-]+"
        );

        // Unknown preset
        let json = r#"{
            "config_keys": {
                "build": { "values": ["debug"] }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["test"],
                    "config": {
                        "build": "nonexistent"
                    }
                }
            ]
        }"#;
        let err = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap_err();
        assert!(matches!(err, ConfigError::UnknownPreset { .. }));
        assert_eq!(
            err.to_string(),
            "Unknown preset 'nonexistent' for key 'build'"
        );
    }

    #[test]
    fn test_canonical_ordering() {
        let json = r#"{
            "config_keys": {
                "zkey": { "values": ["z1"] },
                "akey": { "values": ["a1"] },
                "mkey": { "values": ["m1"] }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["test"],
                    "config": {
                        "zkey": ["z1"],
                        "akey": ["a1"],
                        "mkey": ["m1"]
                    }
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let bench = &config_file.benchmarks()[0];

        let mut configs = bench.config().iter();
        let config = configs.next().unwrap();

        // Keys are sorted alphabetically
        let config_str = config.to_string();
        assert_eq!(config_str, "akey=a1,mkey=m1,zkey=z1");
    }

    #[test]
    fn test_lazy_iteration() {
        let json = r#"{
            "config_keys": {
                "a": { "values": ["1", "2", "3"] },
                "b": { "values": ["x", "y", "z"] },
                "c": { "values": ["p", "q"] }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["test"],
                    "config": {
                        "a": ["1", "2", "3"],
                        "b": ["x", "y", "z"],
                        "c": ["p", "q"]
                    }
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let bench = &config_file.benchmarks()[0];

        // 3 x 3 x 2 = 18 configs
        assert_eq!(bench.config().len(), 18);

        let mut iter = bench.config().iter();
        assert_eq!(iter.len(), 18);

        // Take only first 5 (demonstrating lazy evaluation)
        let first_five: Vec<_> = iter.by_ref().take(5).collect();
        assert_eq!(first_five.len(), 5);

        // Iterator should have 13 remaining
        assert_eq!(iter.len(), 13);
    }

    #[test]
    fn test_expand_command_edge_cases() {
        let key1 = Key::new("key".to_string(), vec!["value".to_string()]).unwrap();
        let key2 = Key::new("another".to_string(), vec!["test".to_string()]).unwrap();

        let kv1 = key1.value_from_name("value").unwrap();
        let kv2 = key2.value_from_name("test").unwrap();

        let mut config = Config::new();
        config.kv.push(kv2); // another comes first alphabetically
        config.kv.push(kv1);

        // Test multiple placeholders
        assert_eq!(
            config.expand_template("{key} and {another}").unwrap(),
            "value and test"
        );

        // Test adjacent placeholders
        assert_eq!(
            config.expand_template("{key}{another}").unwrap(),
            "valuetest"
        );

        // Test placeholder at start
        assert_eq!(
            config.expand_template("{key} start").unwrap(),
            "value start"
        );

        // Test placeholder at end
        assert_eq!(config.expand_template("end {key}").unwrap(), "end value");

        // Test no placeholders
        assert_eq!(
            config.expand_template("no placeholders").unwrap(),
            "no placeholders"
        );

        // Test empty template
        assert_eq!(config.expand_template("").unwrap(), "");

        // Test missing placeholder
        let result = config.expand_template("{missing}");
        assert!(matches!(
            result,
            Err(ConfigError::UnknownKeyInPlaceholder(_))
        ));

        // Test malformed placeholder (no closing brace)
        assert_eq!(
            config.expand_template("start {key end").unwrap(),
            "start {key end"
        );

        // Test nested braces - the first } closes the placeholder
        let err = config.expand_template("{{key}}").unwrap_err();
        assert!(matches!(err, ConfigError::UnknownKeyInPlaceholder(v) if v == "{key"));
    }

    #[test]
    fn test_empty_config_expansion() {
        let json = r#"{
            "config_keys": {},
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["test"],
                    "config": {}
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let bench = &config_file.benchmarks()[0];

        // Empty config should produce exactly one empty config
        assert_eq!(bench.config().len(), 1);

        let configs: Vec<_> = bench.config().iter().collect();
        assert_eq!(configs.len(), 1);
        assert!(configs[0].is_empty());
    }

    #[test]
    fn test_preset_with_single_value() {
        let json = r#"{
            "config_keys": {
                "build": {
                    "values": ["debug", "release"],
                    "presets": {
                        "debug_only": ["debug"]
                    }
                }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["test"],
                    "config": {
                        "build": "debug_only"
                    }
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let bench = &config_file.benchmarks()[0];

        assert_eq!(bench.config().len(), 1);

        let configs: Vec<_> = bench.config().iter().collect();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].to_string(), "build=debug");
    }

    #[test]
    fn test_missing_placeholder_detected_at_parse_time() {
        let json = r#"{
            "config_keys": {
                "build": { "values": ["debug", "release"] }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["bin/{build}/bench", "{threads}"],
                    "config": {
                        "build": ["debug"]
                    }
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let err = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap_err();
        assert!(matches!(err, ConfigError::UnknownKeyInPlaceholder(_)));
    }

    #[test]
    fn test_config_product_filter() {
        let key1 = Key::new(
            "a".to_string(),
            vec!["1".to_string(), "2".to_string(), "3".to_string()],
        )
        .unwrap();
        let key2 = Key::new("b".to_string(), vec!["x".to_string(), "y".to_string()]).unwrap();

        let subset1 = key1.subset_from_names(["1", "2"].iter().copied()).unwrap();
        let subset2 = key2.subset_from_names(["x", "y"].iter().copied()).unwrap();

        let product = ConfigProduct::new(vec![subset1, subset2]);
        assert_eq!(product.len(), 4); // 2 x 2

        // Filter to only configs with a=1
        let mut filter_config = Config::new();
        filter_config.kv.push(key1.value_from_name("1").unwrap());

        let filtered = product.filter(&filter_config).unwrap();
        assert_eq!(filtered.len(), 2); // 1 x 2

        let configs: Vec<_> = filtered.iter().collect();
        assert_eq!(configs.len(), 2);
        assert!(configs.iter().all(|c| c.to_string().starts_with("a=1")));
    }

    #[test]
    fn test_key_value_ordering() {
        let key1 = Key::new("aaa".to_string(), vec!["1".to_string()]).unwrap();
        let key2 = Key::new("zzz".to_string(), vec!["2".to_string()]).unwrap();

        let kv1 = key1.value_from_name("1").unwrap();
        let kv2 = key2.value_from_name("2").unwrap();

        // KeyValue should order by key name first
        assert!(kv1 < kv2);
    }

    #[test]
    fn test_cartesian_product_iteration_order() {
        let json = r#"{
            "config_keys": {
                "a": { "values": ["1", "2"] },
                "b": { "values": ["x", "y"] },
                "c": { "values": ["p", "q"] }
            },
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": ["test"],
                    "config": {
                        "a": ["1", "2"],
                        "b": ["x", "y"],
                        "c": ["p", "q"]
                    }
                }
            ]
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let bench = &config_file.benchmarks()[0];

        // Collect all configs in iteration order
        let configs: Vec<_> = bench.config().iter().map(|c| c.to_string()).collect();

        // The first dimension should vary fastest (like odometer)
        // Keys are sorted alphabetically: a, b, c
        assert_eq!(
            configs,
            vec![
                "a=1,b=x,c=p", // a varies first
                "a=2,b=x,c=p",
                "a=1,b=y,c=p", // then b
                "a=2,b=y,c=p",
                "a=1,b=x,c=q", // then c
                "a=2,b=x,c=q",
                "a=1,b=y,c=q",
                "a=2,b=y,c=q",
            ]
        );
    }

    #[test]
    fn test_config_filter_with_missing_key() {
        let key1 = Key::new("a".to_string(), vec!["1".to_string(), "2".to_string()]).unwrap();
        let key2 = Key::new("b".to_string(), vec!["x".to_string(), "y".to_string()]).unwrap();
        let key3 = Key::new("c".to_string(), vec!["p".to_string()]).unwrap();

        let subset1 = key1.subset_from_names(["1", "2"].iter().copied()).unwrap();
        let subset2 = key2.subset_from_names(["x", "y"].iter().copied()).unwrap();

        let product = ConfigProduct::new(vec![subset1, subset2]);

        // Try to filter by a key not in the product
        let mut filter_config = Config::new();
        filter_config.kv.push(key3.value_from_name("p").unwrap());

        assert!(product.filter(&filter_config).is_none());
    }

    #[test]
    fn test_config_filter_with_missing_value() {
        let key1 = Key::new("a".to_string(), vec!["1".to_string(), "2".to_string()]).unwrap();
        let key2 = Key::new("b".to_string(), vec!["x".to_string(), "y".to_string()]).unwrap();

        // Product only contains a=1 (not a=2)
        let subset1 = key1.subset_from_names(["1"].iter().copied()).unwrap();
        let subset2 = key2.subset_from_names(["x", "y"].iter().copied()).unwrap();

        let product = ConfigProduct::new(vec![subset1, subset2]);

        // Try to filter by a=2 which is not in the product
        let mut filter_config = Config::new();
        filter_config.kv.push(key1.value_from_name("2").unwrap());

        assert!(product.filter(&filter_config).is_none());
    }

    #[test]
    fn test_config_filter_empty() {
        let key1 = Key::new("a".to_string(), vec!["1".to_string(), "2".to_string()]).unwrap();
        let subset1 = key1.subset_from_names(["1", "2"].iter().copied()).unwrap();
        let product = ConfigProduct::new(vec![subset1]);

        // Filter with empty config should return the original product
        let filter_config = Config::new();
        let filtered = product.filter(&filter_config).unwrap();

        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_config_with() {
        let key1 = Key::new("a".to_string(), vec!["1".to_string(), "2".to_string()]).unwrap();
        let key2 = Key::new("b".to_string(), vec!["x".to_string(), "y".to_string()]).unwrap();

        let kv1 = key1.value_from_name("1").unwrap();
        let kv2 = key2.value_from_name("x").unwrap();

        // Start with one key-value pair
        let mut config = Config::new();
        config.kv.push(kv1.clone());

        // Add a new key
        let config2 = config.with(kv2.clone());
        assert_eq!(config2.len(), 2);
        assert_eq!(config2.to_string(), "a=1,b=x");

        // Replace existing key with different value
        let kv1_new = key1.value_from_name("2").unwrap();
        let config3 = config2.with(kv1_new);
        assert_eq!(config3.len(), 2);
        assert_eq!(config3.to_string(), "a=2,b=x");
    }

    #[test]
    fn test_config_without_key() {
        let key1 = Key::new("a".to_string(), vec!["1".to_string()]).unwrap();
        let key2 = Key::new("b".to_string(), vec!["x".to_string()]).unwrap();
        let key3 = Key::new("c".to_string(), vec!["p".to_string()]).unwrap();

        let kv1 = key1.value_from_name("1").unwrap();
        let kv2 = key2.value_from_name("x").unwrap();

        let mut config = Config::new();
        config.kv.push(kv1);
        config.kv.push(kv2);

        // Remove existing key
        let config2 = config.without_key(&key1);
        assert_eq!(config2.len(), 1);
        assert_eq!(config2.to_string(), "b=x");
        assert!(config2.get(&key1).is_none());
        assert!(config2.get(&key2).is_some());

        // Remove non-existent key (should be no-op)
        let config3 = config2.without_key(&key3);
        assert_eq!(config3.len(), 1);
        assert_eq!(config3.to_string(), "b=x");

        // Remove all keys
        let config4 = config3.without_key(&key2);
        assert!(config4.is_empty());
    }

    #[test]
    fn test_config_get_by_name() {
        let key1 = Key::new("build".to_string(), vec!["debug".to_string()]).unwrap();
        let key2 = Key::new("threads".to_string(), vec!["4".to_string()]).unwrap();

        let kv1 = key1.value_from_name("debug").unwrap();
        let kv2 = key2.value_from_name("4").unwrap();

        let mut config = Config::new();
        config.kv.push(kv1);
        config.kv.push(kv2);

        // Get existing keys by name
        assert!(config.get_by_name("build").is_some());
        assert_eq!(config.get_by_name("build").unwrap().value_name(), "debug");
        assert!(config.get_by_name("threads").is_some());
        assert_eq!(config.get_by_name("threads").unwrap().value_name(), "4");

        // Get non-existent key
        assert!(config.get_by_name("nonexistent").is_none());
        assert!(config.get_by_name("").is_none());
    }

    #[test]
    fn test_config_from_map() {
        let json = r#"{
            "config_keys": {
                "build": { "values": ["debug", "release"] },
                "threads": { "values": ["1", "4"] }
            },
            "benchmarks": []
        }"#;

        let tmp_dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();

        // Test valid map
        let mut map = BTreeMap::new();
        map.insert("build".to_string(), "release".to_string());
        map.insert("threads".to_string(), "4".to_string());

        let config = config_file.config_from_map(&map).unwrap();
        assert_eq!(config.len(), 2);
        assert_eq!(config.to_string(), "build=release,threads=4");

        // Test empty map
        let empty_map = BTreeMap::new();
        let empty_config = config_file.config_from_map(&empty_map).unwrap();
        assert!(empty_config.is_empty());

        // Test unknown key
        let mut bad_map = BTreeMap::new();
        bad_map.insert("unknown".to_string(), "value".to_string());
        let err = config_file.config_from_map(&bad_map).unwrap_err();
        assert!(matches!(err, ConfigError::UnknownKey(_)));

        // Test unknown value
        let mut bad_map2 = BTreeMap::new();
        bad_map2.insert("build".to_string(), "unknown".to_string());
        let err2 = config_file.config_from_map(&bad_map2).unwrap_err();
        assert!(matches!(err2, ConfigError::UnknownValueForKey { .. }));
    }

    #[test]
    fn test_benchmark_id_validation() {
        // Valid IDs
        let id1 = BenchmarkId::try_from("test-bench".to_string()).unwrap();
        assert_eq!(id1.as_str(), "test-bench");

        let id2 = BenchmarkId::try_from("2015-04".to_string()).unwrap();
        assert_eq!(id2.as_str(), "2015-04");

        assert!(BenchmarkId::try_from("compile_test".to_string()).is_ok());
        assert!(BenchmarkId::try_from("ABC123".to_string()).is_ok());

        // Invalid IDs
        let empty_err = BenchmarkId::try_from(String::new()).unwrap_err();
        assert!(matches!(empty_err, ConfigError::InvalidBenchmarkId(_)));

        let space_err = BenchmarkId::try_from("test bench".to_string()).unwrap_err();
        assert!(matches!(space_err, ConfigError::InvalidBenchmarkId(_)));

        let slash_err = BenchmarkId::try_from("test/bench".to_string()).unwrap_err();
        assert!(matches!(slash_err, ConfigError::InvalidBenchmarkId(_)));

        let special_err = BenchmarkId::try_from("test@bench".to_string()).unwrap_err();
        assert!(matches!(special_err, ConfigError::InvalidBenchmarkId(_)));
    }

    #[test]
    fn test_config_serialization() {
        let key1 = Key::new("build".to_string(), vec!["debug".to_string()]).unwrap();
        let key2 = Key::new("threads".to_string(), vec!["4".to_string()]).unwrap();

        let kv1 = key1.value_from_name("debug").unwrap();
        let kv2 = key2.value_from_name("4").unwrap();

        let mut config = Config::new();
        config.kv.push(kv1);
        config.kv.push(kv2);

        // Serialize to JSON
        let json = serde_json::to_value(&config).unwrap();
        assert!(json.is_object());

        let obj = json.as_object().unwrap();
        assert_eq!(obj.len(), 2);
        assert_eq!(obj.get("build").unwrap().as_str().unwrap(), "debug");
        assert_eq!(obj.get("threads").unwrap().as_str().unwrap(), "4");

        // Empty config
        let empty = Config::new();
        let empty_json = serde_json::to_value(&empty).unwrap();
        assert_eq!(empty_json.as_object().unwrap().len(), 0);
    }

    #[test]
    fn test_config_to_btreemap() {
        let key1 = Key::new("build".to_string(), vec!["release".to_string()]).unwrap();
        let key2 = Key::new("threads".to_string(), vec!["8".to_string()]).unwrap();

        let kv1 = key1.value_from_name("release").unwrap();
        let kv2 = key2.value_from_name("8").unwrap();

        let mut config = Config::new();
        config.kv.push(kv1);
        config.kv.push(kv2);

        // Convert to BTreeMap
        let map: BTreeMap<String, String> = config.into();
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("build").unwrap(), "release");
        assert_eq!(map.get("threads").unwrap(), "8");

        // Keys should be sorted (BTreeMap property)
        let keys: Vec<_> = map.keys().collect();
        assert_eq!(keys, vec!["build", "threads"]);
    }

    #[test]
    fn test_host_key_with_current_host() {
        let tmp_dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": {},
            "benchmarks": []
        }"#;

        // Create config with a current host
        let config_file = ConfigFile::from_str(tmp_dir.path(), Some("my-machine"), json).unwrap();
        let host_key = config_file.host_key();

        assert_eq!(host_key.name(), "host");

        // Verify we can get host value
        let host_value = host_key.value_from_name("my-machine");
        assert!(host_value.is_some());
        assert_eq!(host_value.unwrap().value_name(), "my-machine");
    }

    #[test]
    fn test_host_key_autodiscovery() {
        let tmp_dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": {},
            "benchmarks": []
        }"#;

        // Create some host directories
        let results_dir = tmp_dir.path().join("results");
        fs::create_dir_all(&results_dir).unwrap();
        fs::create_dir_all(results_dir.join("host1")).unwrap();
        fs::create_dir_all(results_dir.join("host2")).unwrap();

        // Load config without current host
        let config_file = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap();
        let host_key = config_file.host_key();

        // Should autodiscover both hosts
        assert!(host_key.value_from_name("host1").is_some());
        assert!(host_key.value_from_name("host2").is_some());
        assert!(host_key.value_from_name("host3").is_none());
        assert_eq!(host_key.values_len(), 2);

        // Load config with current host
        let config_file = ConfigFile::from_str(tmp_dir.path(), Some("host3"), json).unwrap();
        let host_key = config_file.host_key();
        assert!(host_key.value_from_name("host1").is_some());
        assert!(host_key.value_from_name("host2").is_some());
        assert!(host_key.value_from_name("host3").is_some());
        assert_eq!(host_key.values_len(), 3);
    }

    #[test]
    fn test_host_key_invalid_name() {
        let tmp_dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": {},
            "benchmarks": []
        }"#;

        // Invalid host name with space
        let err = ConfigFile::from_str(tmp_dir.path(), Some("my machine"), json).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidHost(_)));
        assert_eq!(
            err.to_string(),
            "Invalid host 'my machine': must match the format for config values [a-zA-Z0-9_-]+"
        );

        // Invalid host name with special char
        let err2 = ConfigFile::from_str(tmp_dir.path(), Some("my@machine"), json).unwrap_err();
        assert!(matches!(err2, ConfigError::InvalidHost(_)));
    }

    #[test]
    fn test_empty_benchmark_command() {
        let tmp_dir = TempDir::new().unwrap();
        let json = r#"{
            "config_keys": {},
            "benchmarks": [
                {
                    "benchmark": "test",
                    "command": [],
                    "config": {}
                }
            ]
        }"#;

        let err = ConfigFile::from_str(tmp_dir.path(), None, json).unwrap_err();
        assert!(matches!(err, ConfigError::EmptyBenchmarkCommand(_)));
        assert_eq!(err.to_string(), "Empty benchmark command for 'test'");
    }
}
