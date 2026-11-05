use reel_api::maintenance_report::*;

// a new report lists each kind of job once
#[test]
fn job_list() {
    let report = MaintenanceReport::new();

    for (index, kind) in JOB_KINDS.iter().enumerate() {
        assert_eq!(report.jobs[index].kind, *kind);
    }
}
