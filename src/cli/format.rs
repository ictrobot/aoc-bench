use aoc_bench::stable::{Change, ChangeDirection};

pub fn format_duration_ns(ns: f64) -> String {
    if ns >= 1_000_000_000.0 {
        format!("{:.2} s", ns / 1_000_000_000.0)
    } else if ns >= 1_000_000.0 {
        format!("{:.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.2} µs", ns / 1_000.0)
    } else {
        format!("{ns:.0} ns")
    }
}

pub fn format_ci(ns: f64) -> String {
    format!("±{}", format_duration_ns(ns))
}

pub fn format_delta(change: Change) -> String {
    let pct = change.rel_change * 100.0;
    match change.direction {
        ChangeDirection::Regression => format!("+{pct:.2}%"),
        ChangeDirection::Improvement => format!("-{pct:.2}%"),
    }
}
