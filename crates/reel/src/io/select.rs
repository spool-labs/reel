//! Backend selection: the configured choice, with a posix downgrade
//!
//! A ring the kernel refuses to set up, or a platform without one, downgrades to
//! posix with one warning rather than failing the volume open.

use std::sync::Arc;

use crate::config::{IoBackend, ReelConfig};
use crate::io::posix_backend::PosixBackend;
use crate::io::ReelIo;

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
    if wants_ring(config.io_backend) {
        if let Some(ring) = open_ring(config) {
            announce(BackendDecision::Ring);
            return ring;
        }
        announce(BackendDecision::RingUnavailable);
        return Arc::new(fallback_posix(config));
    }
    announce(BackendDecision::Posix);
    Arc::new(fallback_posix(config))
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

fn announce(decision: BackendDecision) {
    match decision {
        BackendDecision::Posix | BackendDecision::Ring => {}
        BackendDecision::RingUnavailable => {
            tracing::warn!(
                "io_uring backend requested but the ring is unavailable, using the posix backend"
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
}
