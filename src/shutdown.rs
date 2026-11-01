//! Cooperative shutdown on SIGTERM / SIGINT.
//!
//! The first signal only sets the returned flag. Long-running loops poll it and finish their
//! current unit of work before returning, so an in-progress run series is recorded rather than
//! discarded. A second signal terminates the process immediately.

use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

/// A flag that is never set, for callers that do not handle signals.
#[must_use]
pub fn never() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

/// Install the SIGTERM / SIGINT handler and return the flag it sets.
#[cfg(unix)]
pub fn install_handler() -> io::Result<Arc<AtomicBool>> {
    use signal_hook::consts::{SIGINT, SIGTERM};
    use signal_hook::flag;

    let stop = never();
    for signal in [SIGTERM, SIGINT] {
        // Registered first so it runs first: if the flag is already set (second signal), fall back
        // to the default action and terminate. Otherwise the second handler sets the flag.
        flag::register_conditional_default(signal, Arc::clone(&stop))?;
        flag::register(signal, Arc::clone(&stop))?;
    }
    Ok(stop)
}

/// No signal handling on platforms without POSIX signals. The flag is never set.
#[cfg(not(unix))]
pub fn install_handler() -> io::Result<Arc<AtomicBool>> {
    Ok(never())
}
