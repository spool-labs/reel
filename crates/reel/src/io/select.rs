//! Backend selection: the configured choice, with a posix downgrade
//!
//! A ring the kernel refuses to set up, or a platform without one, downgrades to
//! posix with one warning rather than failing the volume open.

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
    /// A ring was requested and could not be set up, so posix serves instead
    RingUnavailable,
}

/// Choose a backend from config, downgrading to posix when the ring cannot serve
/// the request
pub fn select_backend(config: &ReelConfig) -> Arc<dyn ReelIo> {
    // What is announced is asked of the backend that was built, not derived from
    // the config beside it, so the line cannot say ring while posix serves.
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

/// The posix backend a volume runs when the ring is not serving it
///
/// A direct volume that loses the ring keeps its direct descriptors: the backend
/// choice picks who submits the op, not whether the page cache stands behind it.
fn fallback_posix(config: &ReelConfig) -> PosixBackend {
    PosixBackend::with_direct(wants_direct(config))
}

/// Whether this volume's descriptors bypass the page cache
///
/// Only Linux has an open flag that means it, so a direct request anywhere else
/// resolves to a buffered volume.
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

/// Say which backend took the volume, on every arm rather than only the bad one
///
/// A silent success and a silent downgrade look identical in a log, so an
/// operator reading one could not tell a ring from the fallback under it. The
/// downgrade stays a warning because it is the one arm that loses what was
/// asked for.
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

    // posix is taken as configured and never asks the kernel for a ring
    #[test]
    fn posix_wants_no_ring() {
        assert!(!wants_ring(IoBackend::Posix));
    }

    // either ring arm asks for a ring, and only the direct one asks for direct
    #[test]
    fn ring_arms_want_a_ring() {
        assert!(wants_ring(IoBackend::Uring));
        assert!(wants_ring(IoBackend::UringDirect));

        assert!(!config_for(IoBackend::Uring).io_backend.is_direct());
        assert!(config_for(IoBackend::UringDirect).io_backend.is_direct());
    }

    // every configured backend yields a usable single owner backend
    #[test]
    fn selection_builds_backend() {
        for backend in [IoBackend::Posix, IoBackend::Uring, IoBackend::UringDirect] {
            let selected: Arc<dyn ReelIo> = select_backend(&config_for(backend));
            assert_eq!(Arc::strong_count(&selected), 1);
        }
    }

    // a posix request is served by posix everywhere, with nothing to downgrade
    #[test]
    fn posix_serves_what_it_asked_for() {
        let selected = select_backend(&config_for(IoBackend::Posix));
        assert_eq!(selected.serving(), ServingBackend::Posix);
        assert!(!selected.serving().is_ring());
    }

    // off linux there is no ring to have, and the volume says so rather than
    // repeating the request back
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

    // the answer is the backend's own, so it disagrees with the request whenever
    // the ring could not be had
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
