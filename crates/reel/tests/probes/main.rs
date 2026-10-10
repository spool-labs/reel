//! Measurement probes, run one at a time, where a bare run skips the opt-in ones
//! `cargo test -p tape-reel --test probes -- <substring>` runs every probe matching it

mod bias_report;
mod erase_probe;
mod filter_cost;
mod interference;
mod open_time;
mod past_ram;

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
        "filter_cost::a_bounded_cache_keeps_its_working_set",
        filter_cost::a_bounded_cache_keeps_its_working_set,
    ),
    opt_in(
        "filter_cost::paged_lookup_block_reads",
        filter_cost::paged_lookup_block_reads,
    ),
    opt_in(
        "interference::tails_under_a_paced_copier",
        interference::tails_under_a_paced_copier,
    ),
    opt_in(
        "open_time::open_time_by_segment_count",
        open_time::open_time_by_segment_count,
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
