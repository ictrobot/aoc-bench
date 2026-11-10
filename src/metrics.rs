//! Metrics pushed to a Prometheus-compatible import endpoint.
//!
//! Enabled by `AOC_BENCH_METRICS_PUSH_URL`, a plain HTTP URL. Unset, every call here is a no-op.
//!
//! Gauges hold state and are resent on every push. Increments are counts that accumulate until a
//! [`Metrics::checkpoint`] stamps them with the time the work finished and queues them as samples,
//! to be added up with `sum_over_time`. They are not counters. Each also feeds an
//! `aoc_bench_batch_*` gauge totalled over the process without the case labels.
//!
//! A background thread owns every push, once an interval and on [`flush`], and the benchmark
//! thread never waits on it. Queued samples keep their timestamps until acknowledged, so a retry
//! is byte-identical. No `# HELP` or `# TYPE` lines are sent; the constants below carry the
//! descriptions. See the design doc for the full series list and the query idiom.
//!
//! Every series carries a `host` label. Series about a case also carry `bench` and a
//! `config_<key>` label for each config key that opts in with `metric_label`.

use crate::config::{BenchmarkId, Config};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

pub const PUSH_URL_ENV: &str = "AOC_BENCH_METRICS_PUSH_URL";
pub const PUSH_INTERVAL_ENV: &str = "AOC_BENCH_METRICS_PUSH_INTERVAL_SECS";
const DEFAULT_PUSH_INTERVAL: Duration = Duration::from_secs(60);
const MIN_PUSH_INTERVAL: Duration = Duration::from_secs(1);
const HTTP_TIMEOUT_SECS: u64 = 5;
/// Queued samples beyond this are dropped oldest first, bounding memory while the agent is away
const MAX_QUEUED_LINES: usize = 100_000;

const PREFIX: &str = "aoc_bench_";
/// Every increment metric has a batch total gauge under this prefix, case labels dropped
pub const BATCH_PREFIX: &str = "aoc_bench_batch_";

// Increments, stamped at the checkpoint that ends the work they belong to

/// Wall time by phase. Phases sum to the process wall clock, see [`Phase`].
pub const TIME_SECONDS: &str = "aoc_bench_time_seconds";
/// Successful runs.
pub const RUNS: &str = "aoc_bench_runs";
/// Samples in the measured phase of successful runs.
pub const SAMPLES: &str = "aoc_bench_samples";
/// Failed run attempts by `reason`, see [`FailureReason`].
pub const RUN_FAILURES: &str = "aoc_bench_run_failures";
/// Recorded measurements by drift `outcome`: initial, matched, suspicious, replaced or forced.
pub const SERIES_OUTCOMES: &str = "aoc_bench_series_outcomes";
/// Processed groups by `class` (new, rerun or all) and `outcome` (executed, reused or failed).
pub const GROUPS: &str = "aoc_bench_groups";

// Batch gauges

/// 1 while a run command is live, 0 on its final push.
pub const ACTIVE: &str = "aoc_bench_active";
pub const BATCH_START_TIMESTAMP: &str = "aoc_bench_batch_start_timestamp_seconds";
pub const BATCH_GROUPS_SELECTED: &str = "aoc_bench_batch_groups_selected";
pub const BATCH_GROUPS_REMAINING: &str = "aoc_bench_batch_groups_remaining";
/// Labelled by `bench`, cleared between groups.
pub const GROUP_START_TIMESTAMP: &str = "aoc_bench_group_start_timestamp_seconds";
pub const GROUP_CASES: &str = "aoc_bench_group_cases";
/// Failed pushes in this process.
pub const PUSH_FAILURES: &str = "aoc_bench_metrics_push_failures";
/// Queued samples dropped in this process because the queue was full.
pub const DROPPED_LINES: &str = "aoc_bench_metrics_dropped_lines";

// Dataset gauges, the in-scope work under the current config plus whole-history counts

/// Eligible groups by `class` before limits, when the scheduler scanned the whole pool.
pub const DATASET_GROUPS: &str = "aoc_bench_dataset_groups";
/// Cases those groups cover, by `class`.
pub const DATASET_CASES: &str = "aoc_bench_dataset_cases";
/// Rerun workloads whose latest measurement disagreed with their stable result.
pub const DATASET_WORKLOADS_SUSPICIOUS: &str = "aoc_bench_dataset_workloads_suspicious";
/// The oldest last measurement by `class`: over rerun workloads (the age of the sweep), or over the
/// workloads new groups' cases still point at. 0 when the class has none.
pub const DATASET_OLDEST_MEASUREMENT_TIMESTAMP: &str =
    "aoc_bench_dataset_oldest_measurement_timestamp_seconds";
/// Every measurement ever recorded for the host.
pub const DATASET_MEASUREMENTS: &str = "aoc_bench_dataset_measurements";
/// Runs over every measurement.
pub const DATASET_MEASUREMENT_RUNS: &str = "aoc_bench_dataset_measurement_runs";
/// The database file plus its write-ahead log.
pub const DATASET_DB_BYTES: &str = "aoc_bench_dataset_db_bytes";

/// Where a run's wall clock went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Discarding samples until package power settled below the host limit
    Wait,
    /// Discarding samples before that and after it, until the warmup minimums were met
    Warmup,
    /// The sampled time the measurement is computed from
    Measurement,
    /// Everything else: hashing, spawning, storage, scheduling, failed attempts
    Other,
}

impl Phase {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Phase::Wait => "wait",
            Phase::Warmup => "warmup",
            Phase::Measurement => "measurement",
            Phase::Other => "other",
        }
    }
}

/// Why a run attempt failed, bounded so `reason` stays a small label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureReason {
    Timeout,
    Warmup,
    Crash,
    Protocol,
    Other,
}

impl FailureReason {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            FailureReason::Timeout => "timeout",
            FailureReason::Warmup => "warmup",
            FailureReason::Crash => "crash",
            FailureReason::Protocol => "protocol",
            FailureReason::Other => "other",
        }
    }
}

/// A count as a metric value. Exact below 2^53, which no count here approaches.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn count(count: usize) -> f64 {
    count as f64
}

/// Label pairs in insertion order, rendered with Prometheus escaping.
///
/// The first `case` pairs identify a case (`bench` and the opted-in `config_<key>` labels). The
/// rest belong to the metric, such as `phase` or `outcome`, and survive into the batch totals.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Labels {
    pairs: Vec<(String, String)>,
    case: usize,
}

impl Labels {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with(mut self, name: &str, value: &str) -> Self {
        self.pairs.push((name.to_string(), value.to_string()));
        self
    }

    /// `bench` plus a `config_<key>` label for every key in `config` that opted in.
    #[must_use]
    pub fn case(bench: &BenchmarkId, config: &Config) -> Self {
        let mut labels = Self::new().with("bench", bench.as_str());
        for kv in config.iter().filter(|kv| kv.key().metric_label()) {
            labels = labels.with(&format!("config_{}", kv.key().name()), kv.value_name());
        }
        labels.case = labels.pairs.len();
        labels
    }

    /// The metric's own labels, without the case.
    #[must_use]
    pub fn without_case(&self) -> Self {
        Self {
            pairs: self.pairs[self.case..].to_vec(),
            case: 0,
        }
    }

    fn render(&self, out: &mut String) {
        for (name, value) in &self.pairs {
            out.push(',');
            render_pair(out, name, value);
        }
    }
}

fn render_pair(out: &mut String, name: &str, value: &str) {
    out.push_str(name);
    out.push_str("=\"");
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// An increment accumulated since the last checkpoint.
struct Pending {
    value: f64,
    /// The batch total gauge it feeds
    batch_key: String,
}

/// A stamped sample waiting to be pushed.
struct Line {
    key: String,
    value: f64,
    timestamp_ms: i64,
}

#[derive(Default)]
struct State {
    /// Keyed by the rendered `name{labels}` so payloads are deterministic
    gauges: BTreeMap<String, f64>,
    /// A gauge changed since the last push. The heartbeat sends them regardless.
    gauges_dirty: bool,
    pending: BTreeMap<String, Pending>,
    batch_totals: BTreeMap<String, f64>,
    queue: VecDeque<Line>,
    /// Last timestamp used per series, so two samples never share a millisecond
    last_timestamp_ms: BTreeMap<String, i64>,
    dropped_lines: u64,
    push_failures: u64,
    /// Failures since the last success. Only the first is logged loudly.
    consecutive_failures: u64,
    flush_requested: bool,
    /// Pushes attempted, so a flush can wait for its own
    generation: u64,
    /// A payload has been built and is being sent. Changes made now miss it.
    pushing: bool,
}

impl State {
    fn stamp(&mut self, key: &str, wanted_ms: i64) -> i64 {
        let last = self.last_timestamp_ms.get(key).copied();
        let timestamp_ms = last.map_or(wanted_ms, |last| wanted_ms.max(last + 1));
        self.last_timestamp_ms.insert(key.to_string(), timestamp_ms);
        timestamp_ms
    }
}

/// The in-memory registry and its push thread.
pub struct Metrics {
    url: String,
    /// Rendered `host="..."` opening every label set
    host_label: String,
    state: Mutex<State>,
    wake: Condvar,
    /// Nanoseconds recorded under every phase except [`Phase::Other`], so a caller timing a larger
    /// unit of work can attribute the rest to it, see [`Metrics::accounted`]
    accounted_ns: AtomicU64,
}

static GLOBAL: OnceLock<Option<Arc<Metrics>>> = OnceLock::new();

/// Enable metrics from the environment. Does nothing without `AOC_BENCH_METRICS_PUSH_URL`.
///
/// `host` is the value of the `host` label on every series. Later calls are ignored.
pub fn init_from_env(host: &str) {
    GLOBAL.get_or_init(|| {
        let url = std::env::var(PUSH_URL_ENV).ok()?;
        if !url.starts_with("http://") {
            warn!(
                url,
                "ignoring {PUSH_URL_ENV}: only plain http:// URLs are supported"
            );
            return None;
        }
        let interval = std::env::var(PUSH_INTERVAL_ENV)
            .ok()
            .and_then(|s| s.parse().ok())
            .map_or(DEFAULT_PUSH_INTERVAL, Duration::from_secs)
            .max(MIN_PUSH_INTERVAL);
        info!(url, interval_s = interval.as_secs(), "pushing metrics");
        Some(Metrics::start(&url, host, interval))
    });
}

/// The metrics enabled by [`init_from_env`], if any.
#[must_use]
pub fn global() -> Option<&'static Metrics> {
    GLOBAL.get().and_then(Option::as_ref).map(Arc::as_ref)
}

/// Ask the push thread to push what changed now, without waiting, if metrics are enabled.
pub fn flush() {
    if let Some(metrics) = global() {
        metrics.flush();
    }
}

impl Metrics {
    /// Build a registry whose push thread POSTs to `url` every `interval`.
    #[must_use]
    pub fn start(url: &str, host: &str, interval: Duration) -> Arc<Self> {
        let mut host_label = String::new();
        render_pair(&mut host_label, "host", host);
        let metrics = Arc::new(Self {
            url: url.to_string(),
            host_label,
            state: Mutex::default(),
            wake: Condvar::new(),
            accounted_ns: AtomicU64::new(0),
        });
        let pusher = Arc::clone(&metrics);
        thread::Builder::new()
            .name("metrics-push".into())
            .spawn(move || pusher.push_loop(interval))
            .expect("spawn metrics push thread");
        metrics
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn push_loop(&self, interval: Duration) {
        loop {
            let heartbeat = {
                let mut state = self.lock();
                let deadline = Instant::now() + interval;
                loop {
                    if std::mem::take(&mut state.flush_requested) {
                        break false;
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        break true;
                    }
                    state = self
                        .wake
                        .wait_timeout(state, deadline - now)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
            };
            self.push(heartbeat);
            {
                let mut state = self.lock();
                state.generation += 1;
                state.pushing = false;
            }
            self.wake.notify_all();
        }
    }

    /// Add to an increment. It is stamped and queued by the next [`Metrics::checkpoint`].
    pub fn increment(&self, name: &str, labels: &Labels, value: f64) {
        let key = self.series_key(name, labels);
        let mut state = self.lock();
        if let Some(pending) = state.pending.get_mut(&key) {
            pending.value += value;
            return;
        }
        let batch_name = name.replacen(PREFIX, BATCH_PREFIX, 1);
        let batch_key = self.series_key(&batch_name, &labels.without_case());
        state.pending.insert(key, Pending { value, batch_key });
    }

    /// Record `duration` under `phase` for the case `labels` describe.
    pub fn add_time(&self, phase: Phase, labels: &Labels, duration: Duration) {
        let labels = labels.clone().with("phase", phase.label());
        self.increment(TIME_SECONDS, &labels, duration.as_secs_f64());
        if phase != Phase::Other {
            let ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
            self.accounted_ns.fetch_add(ns, Ordering::Relaxed);
        }
    }

    /// Time recorded so far under every phase but [`Phase::Other`].
    ///
    /// A caller that times a larger unit of work subtracts the difference between two snapshots
    /// from its wall time and records the rest as [`Phase::Other`], so the phases add up.
    #[must_use]
    pub fn accounted(&self) -> Duration {
        Duration::from_nanos(self.accounted_ns.load(Ordering::Relaxed))
    }

    /// Stamp every pending increment with `timestamp` and queue it, and move the batch totals on.
    ///
    /// Called when a unit of work ends, so its samples carry the time it finished whatever the
    /// push cadence. Increments that add to zero are dropped.
    pub fn checkpoint(&self, timestamp: jiff::Timestamp) {
        let wanted_ms = timestamp.as_millisecond();
        let mut state = self.lock();
        let pending = std::mem::take(&mut state.pending);
        for (key, Pending { value, batch_key }) in pending {
            if value == 0.0 {
                continue;
            }
            let timestamp_ms = state.stamp(&key, wanted_ms);
            state.queue.push_back(Line {
                key,
                value,
                timestamp_ms,
            });
            let total = state.batch_totals.entry(batch_key.clone()).or_insert(0.0);
            *total += value;
            let total = *total;
            state.gauges.insert(batch_key, total);
            state.gauges_dirty = true;
        }
        let excess = state.queue.len().saturating_sub(MAX_QUEUED_LINES);
        if excess > 0 {
            state.queue.drain(..excess);
            state.dropped_lines += excess as u64;
            let key = self.series_key(DROPPED_LINES, &Labels::new());
            let dropped = count(usize::try_from(state.dropped_lines).unwrap_or(usize::MAX));
            state.gauges.insert(key, dropped);
        }
    }

    pub fn gauge_set(&self, name: &str, labels: &Labels, value: f64) {
        let key = self.series_key(name, labels);
        let mut state = self.lock();
        state.gauges.insert(key, value);
        state.gauges_dirty = true;
    }

    /// Set a gauge to a count. Exact below 2^53, which no count here approaches.
    #[allow(clippy::cast_precision_loss)]
    pub fn gauge_count(&self, name: &str, labels: &Labels, count: u64) {
        self.gauge_set(name, labels, count as f64);
    }

    /// Set a gauge to a time, as seconds since the epoch.
    #[allow(clippy::cast_precision_loss)]
    pub fn gauge_timestamp(&self, name: &str, labels: &Labels, timestamp: jiff::Timestamp) {
        self.gauge_set(name, labels, timestamp.as_second() as f64);
    }

    /// Drop every series of gauge `name`, whatever its labels.
    pub fn gauge_clear(&self, name: &str) {
        let prefix = format!("{name}{{");
        let mut state = self.lock();
        state.gauges.retain(|key, _| !key.starts_with(&prefix));
        state.gauges_dirty = true;
    }

    /// Ask the push thread to push what changed, without waiting.
    pub fn flush(&self) {
        self.lock().flush_requested = true;
        self.wake.notify_all();
    }

    /// Push what changed and wait for that push to be attempted, or for `timeout`.
    ///
    /// For the end of a process: the queue is sent in order, so waiting for one attempt covers
    /// everything queued before the call.
    pub fn flush_and_wait(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        // A push already in flight was built before this call, so it does not count
        let target = state.generation + if state.pushing { 2 } else { 1 };
        state.flush_requested = true;
        self.wake.notify_all();
        while state.generation < target {
            let now = Instant::now();
            if now >= deadline {
                warn!("timed out waiting for the final metrics push");
                return;
            }
            state = self
                .wake
                .wait_timeout(state, deadline - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    fn series_key(&self, name: &str, labels: &Labels) -> String {
        let mut key = String::with_capacity(name.len() + 32);
        key.push_str(name);
        key.push('{');
        key.push_str(&self.host_label);
        labels.render(&mut key);
        key.push('}');
        key
    }

    /// Render the payload: every queued sample, then the gauges when `all_gauges` is set or any
    /// changed. Returns the number of queued lines it carries, to drop once acknowledged.
    fn build_payload(state: &mut State, now_ms: i64, all_gauges: bool) -> (String, usize) {
        let mut body = String::new();
        for line in &state.queue {
            let _ = writeln!(body, "{} {} {}", line.key, line.value, line.timestamp_ms);
        }
        let taken = state.queue.len();
        if all_gauges || state.gauges_dirty {
            let gauges: Vec<(String, f64)> = state
                .gauges
                .iter()
                .map(|(key, value)| (key.clone(), *value))
                .collect();
            for (key, value) in gauges {
                let timestamp_ms = state.stamp(&key, now_ms);
                let _ = writeln!(body, "{key} {value} {timestamp_ms}");
            }
            state.gauges_dirty = false;
        }
        (body, taken)
    }

    fn push(&self, all_gauges: bool) {
        let (body, taken) = {
            let mut state = self.lock();
            state.pushing = true;
            let now_ms = jiff::Timestamp::now().as_millisecond();
            Self::build_payload(&mut state, now_ms, all_gauges)
        };
        if body.is_empty() {
            return;
        }
        match self.post(&body) {
            Ok(()) => {
                let mut state = self.lock();
                state.queue.drain(..taken);
                if std::mem::take(&mut state.consecutive_failures) > 0 {
                    info!("metrics push recovered");
                }
            }
            Err(error) => {
                let mut state = self.lock();
                state.gauges_dirty = true;
                state.push_failures += 1;
                let key = self.series_key(PUSH_FAILURES, &Labels::new());
                let failures = count(usize::try_from(state.push_failures).unwrap_or(usize::MAX));
                state.gauges.insert(key, failures);
                if state.consecutive_failures > 0 {
                    debug!(%error, "metrics push failed again");
                } else {
                    warn!(%error, "metrics push failed");
                }
                state.consecutive_failures += 1;
            }
        }
    }

    fn post(&self, body: &str) -> Result<(), PushError> {
        let response = minreq::post(&self.url)
            .with_timeout(HTTP_TIMEOUT_SECS)
            .with_header("Content-Type", "text/plain")
            .with_body(body)
            .send()?;
        if (200..300).contains(&response.status_code) {
            Ok(())
        } else {
            Err(PushError::Status(response.status_code))
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum PushError {
    #[error(transparent)]
    Http(#[from] minreq::Error),
    #[error("HTTP {0}")]
    Status(u16),
}

/// A local endpoint for tests that records every push.
#[cfg(test)]
pub(crate) mod test_support {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    /// Serve `status` to every request on a fresh port, handing each request's body back in
    /// arrival order. The body is recorded before the response, so once a push is acknowledged
    /// its body can be received without waiting.
    pub fn sink(status: &'static str) -> (String, mpsc::Receiver<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = stream.read(&mut buf).unwrap();
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&request);
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let len: usize = head
                            .lines()
                            .find_map(|l| l.strip_prefix("Content-Length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if body.len() >= len {
                            break;
                        }
                    }
                }
                let request = String::from_utf8(request).unwrap();
                assert!(request.starts_with("POST /import?keep=query HTTP/1.1\r\n"));
                assert!(request.contains("\r\nContent-Type: text/plain\r\n"));
                let body = request.split_once("\r\n\r\n").unwrap().1.to_string();
                if tx.send(body).is_err() {
                    return;
                }
                stream
                    .write_all(
                        format!(
                            "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        )
                        .as_bytes(),
                    )
                    .unwrap();
            }
        });
        (format!("http://127.0.0.1:{port}/import?keep=query"), rx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigFile;
    use jiff::Timestamp;
    use tempfile::TempDir;

    fn metrics() -> Arc<Metrics> {
        // A port nothing listens on: pushes fail, which the tests below never trigger
        Metrics::start(
            "http://127.0.0.1:9/import",
            "testhost",
            Duration::from_secs(3600),
        )
    }

    fn payload(m: &Metrics, now_ms: i64, all_gauges: bool) -> (String, usize) {
        let mut state = m.lock();
        Metrics::build_payload(&mut state, now_ms, all_gauges)
    }

    fn ts(ms: i64) -> Timestamp {
        Timestamp::from_millisecond(ms).unwrap()
    }

    #[test]
    fn labels_render_escaped() {
        let m = Metrics::start("http://127.0.0.1:9/", "te\"st", Duration::from_secs(3600));
        let labels = Labels::new().with("bench", "a\"b\\c\nd");
        assert_eq!(
            m.series_key("x", &labels),
            "x{host=\"te\\\"st\",bench=\"a\\\"b\\\\c\\nd\"}"
        );
        assert_eq!(m.series_key("x", &Labels::new()), "x{host=\"te\\\"st\"}");
    }

    #[test]
    fn case_labels_follow_metric_label_opt_in() {
        let json = r#"{
            "config_keys": {
                "commit": { "values": ["abc"] },
                "threads": { "values": ["1", "n"], "metric_label": true }
            },
            "benchmarks": [{ "benchmark": "b", "command": ["true"], "config": {} }]
        }"#;
        let dir = TempDir::new().unwrap();
        let config_file = ConfigFile::from_str(dir.path(), None, json).unwrap();
        let config = config_file
            .config_from_string("commit=abc,threads=n")
            .unwrap();
        let labels = Labels::case(&"b".try_into().unwrap(), &config);
        assert_eq!(
            labels.pairs,
            vec![
                ("bench".to_string(), "b".to_string()),
                ("config_threads".to_string(), "n".to_string())
            ]
        );
        assert_eq!(labels.case, 2);
        let with_phase = labels.with("phase", "wait");
        assert_eq!(
            with_phase.without_case(),
            Labels::new().with("phase", "wait")
        );
    }

    #[test]
    fn increments_are_stamped_at_checkpoint_and_total_per_batch() {
        let m = metrics();
        let labels = Labels::case(&"b".try_into().unwrap(), &Config::new());
        m.add_time(Phase::Wait, &labels, Duration::from_millis(500));
        m.add_time(Phase::Measurement, &labels, Duration::from_secs(2));
        m.add_time(Phase::Measurement, &labels, Duration::from_secs(1));
        m.increment(RUNS, &labels, 0.0);
        assert_eq!(m.accounted(), Duration::from_millis(3500));

        // Nothing is sent before a checkpoint
        assert_eq!(payload(&m, 9, false).0, "");

        m.checkpoint(ts(1000));
        let (body, taken) = payload(&m, 1500, false);
        assert_eq!(taken, 2);
        assert_eq!(
            body,
            "aoc_bench_time_seconds{host=\"testhost\",bench=\"b\",phase=\"measurement\"} 3 1000\n\
             aoc_bench_time_seconds{host=\"testhost\",bench=\"b\",phase=\"wait\"} 0.5 1000\n\
             aoc_bench_batch_time_seconds{host=\"testhost\",phase=\"measurement\"} 3 1500\n\
             aoc_bench_batch_time_seconds{host=\"testhost\",phase=\"wait\"} 0.5 1500\n"
        );

        // A second group adds to the totals and is stamped at its own checkpoint
        m.add_time(Phase::Measurement, &labels, Duration::from_secs(1));
        m.checkpoint(ts(2000));
        m.lock().queue.drain(..taken);
        let (body, _) = payload(&m, 2500, false);
        assert_eq!(
            body,
            "aoc_bench_time_seconds{host=\"testhost\",bench=\"b\",phase=\"measurement\"} 1 2000\n\
             aoc_bench_batch_time_seconds{host=\"testhost\",phase=\"measurement\"} 4 2500\n\
             aoc_bench_batch_time_seconds{host=\"testhost\",phase=\"wait\"} 0.5 2500\n"
        );
    }

    #[test]
    fn samples_of_a_series_never_share_a_millisecond() {
        let m = metrics();
        let labels = Labels::new().with("bench", "b");
        m.increment(RUNS, &labels, 1.0);
        m.checkpoint(ts(1000));
        m.increment(RUNS, &labels, 1.0);
        m.checkpoint(ts(1000));
        m.increment(RUNS, &labels, 1.0);
        m.checkpoint(ts(900));
        let (body, _) = payload(&m, 1000, false);
        let stamps: Vec<&str> = body
            .lines()
            .filter(|l| l.starts_with("aoc_bench_runs{"))
            .map(|l| l.rsplit(' ').next().unwrap())
            .collect();
        assert_eq!(stamps, ["1000", "1001", "1002"]);
    }

    #[test]
    fn gauges_go_on_change_or_heartbeat() {
        let m = metrics();
        m.gauge_set(ACTIVE, &Labels::new(), 1.0);
        assert_eq!(
            payload(&m, 5, false).0,
            "aoc_bench_active{host=\"testhost\"} 1 5\n"
        );
        // Unchanged: a flush sends nothing, a heartbeat repeats it
        assert_eq!(payload(&m, 6, false).0, "");
        assert_eq!(
            payload(&m, 6, true).0,
            "aoc_bench_active{host=\"testhost\"} 1 6\n"
        );
        m.gauge_set(
            GROUP_START_TIMESTAMP,
            &Labels::new().with("bench", "a"),
            1.0,
        );
        m.gauge_set(GROUP_CASES, &Labels::new(), 3.0);
        m.gauge_clear(GROUP_START_TIMESTAMP);
        assert_eq!(
            payload(&m, 7, false).0,
            "aoc_bench_active{host=\"testhost\"} 1 7\n\
             aoc_bench_group_cases{host=\"testhost\"} 3 7\n"
        );
    }

    #[test]
    fn queue_is_bounded() {
        let m = metrics();
        for i in 0..(MAX_QUEUED_LINES + 10) {
            m.increment(RUNS, &Labels::new().with("bench", &i.to_string()), 1.0);
        }
        m.checkpoint(ts(1));
        let state = m.lock();
        assert_eq!(state.queue.len(), MAX_QUEUED_LINES);
        assert_eq!(state.dropped_lines, 10);
        assert_eq!(
            state.gauges["aoc_bench_metrics_dropped_lines{host=\"testhost\"}"],
            10.0
        );
    }

    #[test]
    fn flush_posts_the_payload_and_drops_acknowledged_lines() {
        let (url, rx) = test_support::sink("204 No Content");
        let m = Metrics::start(&url, "testhost", Duration::from_secs(3600));
        let labels = Labels::case(&"b".try_into().unwrap(), &Config::new());
        m.increment(RUNS, &labels, 2.0);
        m.checkpoint(ts(1000));
        m.gauge_set(ACTIVE, &Labels::new(), 1.0);
        m.flush_and_wait(Duration::from_secs(10));

        let body = rx.recv().unwrap();
        assert!(body.starts_with("aoc_bench_runs{host=\"testhost\",bench=\"b\"} 2 1000\n"));
        assert!(body.contains("aoc_bench_batch_runs{host=\"testhost\"} 2 "));
        let state = m.lock();
        assert!(state.queue.is_empty());
        assert_eq!(state.consecutive_failures, 0);
    }

    #[test]
    fn failed_push_keeps_the_lines_for_an_identical_retry() {
        let (url, rx) = test_support::sink("500 Internal Server Error");
        let m = Metrics::start(&url, "testhost", Duration::from_secs(3600));
        m.increment(RUNS, &Labels::new(), 1.0);
        m.checkpoint(ts(1000));
        m.flush_and_wait(Duration::from_secs(10));
        let first_body = rx.recv().unwrap();

        let state = m.lock();
        assert_eq!(state.consecutive_failures, 1);
        assert_eq!(state.queue.len(), 1);
        assert_eq!(state.push_failures, 1);
        drop(state);
        let (retry, _) = payload(&m, 5000, false);
        let first_line = first_body.lines().next().unwrap();
        assert_eq!(retry.lines().next().unwrap(), first_line);
        assert!(retry.contains("aoc_bench_metrics_push_failures{host=\"testhost\"} 1 5000\n"));
    }
}
