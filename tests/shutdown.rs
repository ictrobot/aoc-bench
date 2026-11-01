//! Runs in its own process: a second signal would terminate it.

#[cfg(unix)]
#[test]
fn sigterm_sets_stop_flag_without_terminating() {
    use aoc_bench::shutdown;
    use std::sync::atomic::Ordering;

    let stop = shutdown::install_handler().unwrap();
    assert!(!stop.load(Ordering::Relaxed));

    signal_hook::low_level::raise(signal_hook::consts::SIGTERM).unwrap();

    assert!(stop.load(Ordering::Relaxed));
}
