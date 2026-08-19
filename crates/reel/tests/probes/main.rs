//! Measurement probes, run one at a time so no probe times another's noise
//!
//! A bare run executes only the probes that assert. The rest are opt-in and run
//! when the argument names them: `cargo test -p reel --test probes -- <substring>`
//! runs every probe whose name contains it, in release where the module's doc
//! asks for one.

mod bias_report;
mod carried_price;
mod compact_throughput;
mod cue_speed;
mod door_price;
mod drain_depth;
mod erase_probe;
mod filter_cost;
mod handoff_cost;
mod handover_cost;
mod interference;
mod layout;
mod mapped_reads;
mod merge_rate;
mod negative_price;
mod open_time;
mod past_ram;
mod playback_speed;
mod preallocate_cost;
mod publish_cost;
mod repoint_hold;
mod segment_price;
mod tick_facts;

struct Probe {
    name: &'static str,
    run: fn(),
    opt_in: bool,
}

const fn default(name: &'static str, run: fn()) -> Probe {
    Probe {
        name,
        run,
        opt_in: false,
    }
}

const fn opt_in(name: &'static str, run: fn()) -> Probe {
    Probe {
        name,
        run,
        opt_in: true,
    }
}

const PROBES: &[Probe] = &[
    opt_in(
        "bias_report::what_this_machine_argues_for",
        bias_report::what_this_machine_argues_for,
    ),
    default(
        "bias_report::the_rule_is_a_function_of_its_facts",
        bias_report::the_rule_is_a_function_of_its_facts,
    ),
    opt_in(
        "carried_price::a_bulk_load_meets_a_hot_working_set",
        carried_price::a_bulk_load_meets_a_hot_working_set,
    ),
    opt_in(
        "compact_throughput::reclaim_throughput_by_rate",
        compact_throughput::reclaim_throughput_by_rate,
    ),
    opt_in("cue_speed::cue_cost", cue_speed::cue_cost),
    opt_in("cue_speed::read_cost", cue_speed::read_cost),
    opt_in("cue_speed::write_under_cue", cue_speed::write_under_cue),
    opt_in(
        "door_price::a_probe_never_waits",
        door_price::a_probe_never_waits,
    ),
    opt_in(
        "drain_depth::report_drain_depth",
        drain_depth::report_drain_depth,
    ),
    opt_in(
        "drain_depth::one_writer_is_never_batchable",
        drain_depth::one_writer_is_never_batchable,
    ),
    opt_in(
        "erase_probe::erase_reclaim_share",
        erase_probe::erase_reclaim_share,
    ),
    default(
        "filter_cost::filters_remove_the_searches",
        filter_cost::filters_remove_the_searches,
    ),
    default(
        "filter_cost::nothing_written_is_lost",
        filter_cost::nothing_written_is_lost,
    ),
    default(
        "filter_cost::tombstones_are_filtered_in",
        filter_cost::tombstones_are_filtered_in,
    ),
    opt_in("filter_cost::miss_time", filter_cost::miss_time),
    opt_in("filter_cost::miss_time_posix", filter_cost::miss_time_posix),
    opt_in("filter_cost::miss_time_cold", filter_cost::miss_time_cold),
    opt_in("filter_cost::filter_cost", filter_cost::filter_cost),
    default(
        "filter_cost::a_held_footer_costs_no_blocks",
        filter_cost::a_held_footer_costs_no_blocks,
    ),
    default(
        "filter_cost::a_blocked_hit_costs_a_run_of_block_loads",
        filter_cost::a_blocked_hit_costs_a_run_of_block_loads,
    ),
    opt_in(
        "filter_cost::paged_lookup_block_reads",
        filter_cost::paged_lookup_block_reads,
    ),
    opt_in(
        "handoff_cost::batching_arithmetic",
        handoff_cost::batching_arithmetic,
    ),
    opt_in(
        "handoff_cost::aligned_alloc_cost",
        handoff_cost::aligned_alloc_cost,
    ),
    opt_in(
        "handover_cost::handover_cost_by_segment_count",
        handover_cost::handover_cost_by_segment_count,
    ),
    opt_in(
        "interference::tails_under_a_paced_copier",
        interference::tails_under_a_paced_copier,
    ),
    opt_in("layout::what_a_volume_holds", layout::what_a_volume_holds),
    opt_in(
        "mapped_reads::mapped_over_unmapped_cold_random",
        mapped_reads::mapped_over_unmapped_cold_random,
    ),
    opt_in(
        "mapped_reads::mapped_over_unmapped_warm",
        mapped_reads::mapped_over_unmapped_warm,
    ),
    opt_in(
        "merge_rate::merge_by_run_count",
        merge_rate::merge_by_run_count,
    ),
    opt_in(
        "negative_price::a_miss_is_answered_without_the_lock",
        negative_price::a_miss_is_answered_without_the_lock,
    ),
    opt_in(
        "open_time::open_time_by_segment_count",
        open_time::open_time_by_segment_count,
    ),
    opt_in(
        "playback_speed::walk_by_segment_count",
        playback_speed::walk_by_segment_count,
    ),
    opt_in(
        "preallocate_cost::preallocation_cost_by_shape",
        preallocate_cost::preallocation_cost_by_shape,
    ),
    opt_in("publish_cost::batch_publish", publish_cost::batch_publish),
    opt_in(
        "publish_cost::batch_alloc_only",
        publish_cost::batch_alloc_only,
    ),
    opt_in(
        "publish_cost::read_under_publish",
        publish_cost::read_under_publish,
    ),
    opt_in(
        "publish_cost::whole_set_read_under_publish",
        publish_cost::whole_set_read_under_publish,
    ),
    opt_in(
        "repoint_hold::hold_by_batch_size",
        repoint_hold::hold_by_batch_size,
    ),
    opt_in("segment_price::read_side", segment_price::read_side),
    opt_in("segment_price::write_side", segment_price::write_side),
    opt_in("segment_price::tick_side", segment_price::tick_side),
    opt_in(
        "tick_facts::pricing_the_stack_by_segment_count",
        tick_facts::pricing_the_stack_by_segment_count,
    ),
];

#[cfg(target_os = "linux")]
const LINUX_PROBES: &[Probe] = &[
    default(
        "erase_probe::an_erased_volume_rebuilds_every_live_key",
        erase_probe::an_erased_volume_rebuilds_every_live_key,
    ),
    default(
        "erase_probe::a_second_erase_reaches_past_the_first_holes",
        erase_probe::a_second_erase_reaches_past_the_first_holes,
    ),
    opt_in(
        "past_ram::cold_reads_past_ram",
        past_ram::cold_reads_past_ram,
    ),
];

#[cfg(not(target_os = "linux"))]
const LINUX_PROBES: &[Probe] = &[];

fn main() {
    let filter = std::env::args().nth(1);
    let mut ran = 0usize;
    for probe in PROBES.iter().chain(LINUX_PROBES) {
        let named = filter.as_deref().is_some_and(|f| probe.name.contains(f));
        if (probe.opt_in || filter.is_some()) && !named {
            continue;
        }
        println!("probe {}", probe.name);
        (probe.run)();
        ran += 1;
    }
    println!("{ran} probes ran");
}
