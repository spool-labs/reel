//! What the compaction rate and the rewrite threshold are worth, per segment
//!
//! Selection always rewrites the deadest eligible segment, so the threshold decides
//! what is allowed rather than what is chosen, and lowering it costs anything only
//! once nothing sits above the base ratio. Two death shapes bracket the answer:
//! spread evenly over live bytes, which ages every segment together, and weighted
//! toward older segments, which builds the gradient greedy selection wants. The
//! device is a number rather than a drive, so nothing here says anything about io.
//!
//! Run with:
//!   cargo test -p reel --test compact_rate_sim -- --nocapture

use reel::{CompactRate, GcPressure, GcTier, RateLimiter};

const MB: f64 = 1_000_000.0;

/// Segments in the volume, and the bytes each one holds
const SEGMENTS: usize = 1_000;
const SEGMENT_BYTES: f64 = 1_000.0 * MB;

/// Live bytes the volume settles at, so dead space is the only thing moving
const LIVE_TARGET: f64 = 600.0 * 1_000.0 * MB;

/// What the drive under the volume can actually move, in MB/s
const DEVICE_MBPS: f64 = 230.0;

/// Ticks per run, one simulated day at one second each
const TICKS: usize = 86_400;

/// Rewrite threshold a relaxed volume selects segments at
const BASE_DEAD_RATIO: f64 = 0.50;

/// Threshold the escalated tier lowers to
const ESCALATED_RATIO: f64 = 0.20;

/// One sealed segment's accounting, the same split the index keeps
#[derive(Clone, Copy)]
struct Segment {
    live: f64,
    dead: f64,
    age: f64,
}

impl Segment {
    fn total(&self) -> f64 {
        self.live + self.dead
    }

    fn dead_fraction(&self) -> f64 {
        if self.total() == 0.0 {
            0.0
        } else {
            self.dead / self.total()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Law {
    /// Today: one rate whatever the tier, threshold drops when escalated
    Fixed,
    /// Today's threshold, with the rate scaled by what a reclaimed byte costs
    /// at the segment actually being rewritten
    CostScaled,
    /// Today's rate, with the threshold never lowered
    HoldRatio,
}

impl Law {
    fn label(self) -> &'static str {
        match self {
            Law::Fixed => "fixed",
            Law::CostScaled => "cost scaled",
            Law::HoldRatio => "hold ratio",
        }
    }
}

#[derive(Clone, Copy)]
enum Death {
    /// Every live byte is equally likely to die, so segments age together
    Uniform,
    /// Older segments shed bytes faster, which is what expiry looks like
    Aged,
}

impl Death {
    fn label(self) -> &'static str {
        match self {
            Death::Uniform => "uniform",
            Death::Aged => "aged",
        }
    }

    /// Relative weight of this segment in the next byte to die
    fn weight(self, segment: &Segment) -> f64 {
        match self {
            Death::Uniform => segment.live,
            // Linear in age, so the oldest segments carry most of the deaths.
            Death::Aged => segment.live * segment.age,
        }
    }
}

struct Outcome {
    filled_at: Option<usize>,
    peak_dead: f64,
    final_dead: f64,
    written_mb: f64,
    starved_ticks: usize,
    mean_target_ratio: f64,
    #[allow(dead_code)]
    rewrites: usize,
}

fn run(law: Law, death: Death, debt_mbps: f64) -> Outcome {
    let pressure = GcPressure::new((SEGMENTS as f64 * SEGMENT_BYTES) as u64, 0, BASE_DEAD_RATIO);
    let base_mbps = RateLimiter::for_compaction(CompactRate::Auto).target_mbps() as f64;

    // Start full of sealed segments holding the live target, spread evenly, with
    // ages fanned out so the aged shape has a gradient to work with from tick 0.
    let per_segment = LIVE_TARGET / SEGMENTS as f64;
    let mut segments: Vec<Segment> = (0..SEGMENTS)
        .map(|i| Segment {
            live: per_segment,
            dead: 0.0,
            age: (i + 1) as f64,
        })
        .collect();

    let mut peak_dead = 0.0f64;
    let mut written = 0.0f64;
    let mut starved_ticks = 0usize;
    let mut target_ratio_sum = 0.0f64;
    let mut rewrites = 0usize;
    let mut filled_at = None;

    for tick in 0..TICKS {
        // Ingest replaces what dies, so the volume holds its live set and dead bytes
        // are the only thing accumulating. Without it nothing is under pressure.
        let arriving = debt_mbps * MB;
        if let Some(slot) = segments
            .iter_mut()
            .find(|segment| segment.total() + arriving <= SEGMENT_BYTES)
        {
            slot.live += arriving;
            if slot.age > 0.0 {
                slot.age = 0.0;
            }
        }

        let total_weight: f64 = segments.iter().map(|segment| death.weight(segment)).sum();
        let dying = debt_mbps * MB;
        if total_weight > 0.0 {
            for segment in segments.iter_mut() {
                let share = death.weight(segment) / total_weight * dying;
                let killed = share.min(segment.live);
                segment.live -= killed;
                segment.dead += killed;
                segment.age += 1.0;
            }
        }

        let live: f64 = segments.iter().map(|segment| segment.live).sum();
        let dead: f64 = segments.iter().map(|segment| segment.dead).sum();
        let used = live + dead;
        let dead_fraction = if used == 0.0 { 0.0 } else { dead / used };
        peak_dead = peak_dead.max(dead_fraction);

        if used >= SEGMENTS as f64 * SEGMENT_BYTES {
            filled_at = Some(tick);
            break;
        }

        // Foreground takes its cut of the device first.
        let foreground = 40.0 * MB;
        let is_hot = true;
        let tier = pressure.tier(dead_fraction, is_hot);
        if !pressure.should_compact(dead_fraction, is_hot) {
            continue;
        }

        let threshold = match law {
            Law::HoldRatio => BASE_DEAD_RATIO,
            Law::Fixed | Law::CostScaled => {
                if tier == GcTier::Escalated {
                    ESCALATED_RATIO
                } else {
                    BASE_DEAD_RATIO
                }
            }
        };

        // The real selection: deadest eligible segment, not merely an eligible one.
        let target = segments
            .iter()
            .enumerate()
            .filter(|(_, segment)| segment.total() > 0.0 && segment.dead_fraction() >= threshold)
            .max_by(|a, b| a.1.dead_fraction().total_cmp(&b.1.dead_fraction()))
            .map(|(index, segment)| (index, segment.dead_fraction()));

        let Some((index, ratio)) = target else {
            starved_ticks += 1;
            continue;
        };

        // Cost scaling grants the extra bandwidth a poorer segment needs, so reclaim
        // throughput holds rather than the write budget.
        let cost = (1.0 - ratio) / ratio;
        let rate_mbps = match law {
            Law::Fixed | Law::HoldRatio => base_mbps,
            Law::CostScaled => base_mbps * cost.max(1.0),
        };

        let headroom = (DEVICE_MBPS * MB - foreground).max(0.0);
        let budget = (rate_mbps * MB).min(headroom);

        // A pass that cannot afford the whole segment does the share it can.
        let segment = segments[index];
        let affordable = (budget / segment.live.max(1.0)).min(1.0);
        let copied = segment.live * affordable;
        let freed = segment.dead * affordable;

        segments[index].live -= copied;
        segments[index].dead -= freed;
        if segments[index].total() < 1.0 {
            segments[index] = Segment {
                live: 0.0,
                dead: 0.0,
                age: 0.0,
            };
        }

        // The live bytes land in a fresh segment, which is why compaction moves bytes
        // without freeing them all.
        if let Some(slot) = segments.iter_mut().find(|segment| segment.total() == 0.0) {
            slot.live += copied;
            slot.age = 0.0;
        }

        written += copied;
        target_ratio_sum += ratio;
        rewrites += 1;
    }

    let live: f64 = segments.iter().map(|segment| segment.live).sum();
    let dead: f64 = segments.iter().map(|segment| segment.dead).sum();

    Outcome {
        filled_at,
        peak_dead,
        final_dead: if live + dead == 0.0 {
            0.0
        } else {
            dead / (live + dead)
        },
        written_mb: written / MB,
        starved_ticks,
        mean_target_ratio: if rewrites == 0 {
            0.0
        } else {
            target_ratio_sum / rewrites as f64
        },
        rewrites,
    }
}

// the three rate laws, priced against both death shapes and two debt rates
#[test]
fn compare_thresholds_and_rates() {
    for death in [Death::Uniform, Death::Aged] {
        for debt_mbps in [20.0, 60.0] {
            println!("\n{} death, {:.0} MB/s debt", death.label(), debt_mbps);
            println!(
                "{:<13} {:>12} {:>10} {:>10} {:>12} {:>12} {:>11}",
                "law",
                "volume full",
                "peak dead",
                "end dead",
                "written GB",
                "mean target",
                "nothing due"
            );

            for law in [Law::Fixed, Law::CostScaled, Law::HoldRatio] {
                let out = run(law, death, debt_mbps);
                let full = match out.filled_at {
                    Some(tick) => format!("{:.1}h", tick as f64 / 3600.0),
                    None => "no".to_string(),
                };
                println!(
                    "{:<13} {full:>12} {:>9.1}% {:>9.1}% {:>12.0} {:>11.2} {:>10.1}%",
                    law.label(),
                    out.peak_dead * 100.0,
                    out.final_dead * 100.0,
                    out.written_mb / 1000.0,
                    out.mean_target_ratio,
                    out.starved_ticks as f64 / TICKS as f64 * 100.0,
                );
            }
        }
    }
}

// selection is greedy, so a lower threshold only widens the candidate set and
// never picks a worse segment while a better one exists
#[test]
fn a_lower_threshold_never_changes_the_choice_while_a_better_segment_exists() {
    let segments = [
        Segment {
            live: 10.0,
            dead: 90.0,
            age: 1.0,
        },
        Segment {
            live: 70.0,
            dead: 30.0,
            age: 1.0,
        },
    ];

    let pick = |threshold: f64| {
        segments
            .iter()
            .filter(|segment| segment.dead_fraction() >= threshold)
            .max_by(|a, b| a.dead_fraction().total_cmp(&b.dead_fraction()))
            .map(|segment| segment.dead_fraction())
    };

    assert_eq!(pick(BASE_DEAD_RATIO), Some(0.9));
    assert_eq!(pick(ESCALATED_RATIO), Some(0.9));
}
