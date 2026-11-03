//! Counters, latency histograms and the heap, written as one JSON line per tick

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Log2 buckets of nanoseconds, up to about 9 s
const BUCKETS: usize = 34;

pub struct Lane {
    count: AtomicU64,
    bytes: AtomicU64,
    max_ns: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl Default for Lane {
    fn default() -> Self {
        Lane {
            count: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            max_ns: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Lane {
    pub fn record(&self, took: Duration, bytes: usize) {
        let ns = took.as_nanos().min(u64::MAX as u128) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
        let bucket = (64 - ns.max(1).leading_zeros() as usize).min(BUCKETS - 1);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
    }

    /// This tick's numbers, and the lane reset for the next
    fn drain(&self, seconds: f64) -> serde_json::Value {
        let count = self.count.swap(0, Ordering::Relaxed);
        let bytes = self.bytes.swap(0, Ordering::Relaxed);
        let max = self.max_ns.swap(0, Ordering::Relaxed);
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|bucket| bucket.swap(0, Ordering::Relaxed))
            .collect();
        let quantile = |q: f64| -> u64 {
            let want = (count as f64 * q).ceil() as u64;
            let mut seen = 0;
            for (bucket, n) in counts.iter().enumerate() {
                seen += n;
                if seen >= want && want > 0 {
                    return 1u64 << bucket;
                }
            }
            0
        };
        serde_json::json!({
            "per_s": (count as f64 / seconds).round(),
            "mib_s": ((bytes as f64 / seconds) / 1048576.0 * 10.0).round() / 10.0,
            "p50_us": quantile(0.5) / 1000,
            "p99_us": quantile(0.99) / 1000,
            "max_us": max / 1000,
        })
    }
}

#[derive(Default)]
pub struct Stats {
    pub put: Lane,
    pub delete: Lane,
    pub batch: Lane,
    pub get: Lane,
    pub range: Lane,
    pub walk: Lane,
    pub maintain: Lane,
    pub flush: Lane,
}

impl Stats {
    pub fn tick(&self, seconds: f64) -> serde_json::Value {
        serde_json::json!({
            "put": self.put.drain(seconds),
            "delete": self.delete.drain(seconds),
            "batch": self.batch.drain(seconds),
            "get": self.get.drain(seconds),
            "range": self.range.drain(seconds),
            "walk": self.walk.drain(seconds),
            "maintain": self.maintain.drain(seconds),
            "flush": self.flush.drain(seconds),
        })
    }
}

#[cfg(target_os = "linux")]
fn anonymous_kibibytes(rollup: &str) -> Option<u64> {
    let line = rollup.lines().find(|line| line.starts_with("Anonymous:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

/// The process's anonymous memory and what the allocator holds live, in MiB
pub fn heap() -> serde_json::Value {
    #[cfg(target_os = "linux")]
    {
        let anon = std::fs::read_to_string("/proc/self/smaps_rollup")
            .ok()
            .and_then(|text| anonymous_kibibytes(&text))
            .map(|kibibytes| kibibytes / 1024);
        #[cfg(target_env = "gnu")]
        let (in_use, held) = {
            // SAFETY: mallinfo2 reads the allocator's own counters
            let info = unsafe { libc::mallinfo2() };
            let in_use = (info.uordblks + info.hblkhd) / 1048576;
            let held = info.fordblks / 1048576;
            (Some(in_use as u64), Some(held as u64))
        };
        #[cfg(not(target_env = "gnu"))]
        let (in_use, held): (Option<u64>, Option<u64>) = (None, None);
        serde_json::json!({ "anon_mib": anon, "in_use_mib": in_use, "free_held_mib": held })
    }
    #[cfg(not(target_os = "linux"))]
    {
        // SAFETY: getrusage fills the struct it is handed
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };
        serde_json::json!({ "max_rss_mib": usage.ru_maxrss as u64 / 1048576 })
    }
}
