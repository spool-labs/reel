//! Picks the configured backend, falling back to posix with a warning when no ring sets up

use std::sync::Arc;

use crate::config::{IoBackend, ReelConfig};
use crate::io::posix_backend::PosixBackend;
use crate::io::{ReelIo, ServingBackend};

/// Outcome of resolving a configured backend against the runtime ring setup
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BackendDecision {
    /// Posix chosen as configured
    Posix,
    /// A ring was set up and serves the volume
    Ring,
    /// A ring was requested and could not be set up, so posix serves
    RingUnavailable,
}

/// Choose a backend from config, falling back to posix when the ring cannot be set up
pub fn select_backend(config: &ReelConfig) -> Arc<dyn ReelIo> {
    // The log line asks the built backend what it is, so it cannot say ring while posix serves.
    if wants_ring(config.io_backend) {
        if let Some(ring) = open_ring(config) {
            announce(BackendDecision::Ring, ring.serving());
            return ring;
        }
        let posix = Arc::new(fallback_posix(config));
        announce(BackendDecision::RingUnavailable, posix.serving());
        return posix;
    }
    let posix = Arc::new(fallback_posix(config));
    announce(BackendDecision::Posix, posix.serving());
    posix
}

/// The posix backend for a volume without a ring, still direct when the config wants it
fn fallback_posix(config: &ReelConfig) -> PosixBackend {
    PosixBackend::with_direct(wants_direct(config))
}

/// Whether this volume's descriptors bypass the page cache, which only Linux supports
fn wants_direct(config: &ReelConfig) -> bool {
    cfg!(target_os = "linux") && config.io_backend.is_direct()
}

#[cfg(target_os = "linux")]
fn open_ring(config: &ReelConfig) -> Option<Arc<dyn ReelIo>> {
    match crate::io::uring_backend::UringBackend::new(wants_direct(config), config.uring) {
        Ok(ring) => Some(Arc::new(ring)),
        Err(error) => {
            tracing::warn!("io_uring setup failed: {error}");
            None
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn open_ring(_config: &ReelConfig) -> Option<Arc<dyn ReelIo>> {
    None
}

fn wants_ring(backend: IoBackend) -> bool {
    match backend {
        IoBackend::Posix => false,
        IoBackend::Uring | IoBackend::UringDirect => true,
    }
}

/// Log which backend took the volume, with a warning when the ring fell back to posix
fn announce(decision: BackendDecision, serving: ServingBackend) {
    match decision {
        BackendDecision::Posix | BackendDecision::Ring => {
            tracing::info!("reel volume served by the {serving} backend");
        }
        BackendDecision::RingUnavailable => {
            tracing::warn!(
                "io_uring backend requested but the ring is unavailable, \
                 so the {serving} backend serves this volume instead"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_for(backend: IoBackend) -> ReelConfig {
        ReelConfig {
            io_backend: backend,
            ..ReelConfig::default()
        }
    }

    // a posix request is served by posix everywhere, with nothing to downgrade
    #[test]
    fn posix_serves_what_it_asked_for() {
        let selected = select_backend(&config_for(IoBackend::Posix));
        assert_eq!(selected.serving(), ServingBackend::Posix);
        assert!(!selected.serving().is_ring());
    }

    // off linux a ring request is served by posix
    #[cfg(not(target_os = "linux"))]
    #[test]
    fn a_ring_request_is_posix_without_a_ring() {
        for backend in [IoBackend::Uring, IoBackend::UringDirect] {
            let selected = select_backend(&config_for(backend));
            assert!(
                !selected.serving().is_ring(),
                "{backend:?} reported a ring on a platform without one",
            );
        }
    }

    // the backend reports itself, so a ring only serves a volume that asked for one
    #[test]
    fn serving_is_the_outcome_not_the_request() {
        for backend in [IoBackend::Posix, IoBackend::Uring, IoBackend::UringDirect] {
            let config = config_for(backend);
            let serving = select_backend(&config).serving();
            if serving.is_ring() {
                assert!(
                    wants_ring(config.io_backend),
                    "a ring served a volume that never asked for one",
                );
            }
        }
    }
}
