//! Reads the lines cargo and the test binaries print

/// Prefix of the line a test binary prints when it finishes
pub const RESULT_PREFIX: &str = "test result: ";
/// Status a passing test binary reports
pub const RESULT_OK: &str = "ok";
/// Text between the status and the counts
pub const STATUS_END: &str = ". ";
/// Text between two counts
pub const COUNT_END: &str = "; ";
/// Label of the tests that passed
pub const PASSED_LABEL: &str = "passed";
/// Label of the tests that failed
pub const FAILED_LABEL: &str = "failed";
/// Label of the tests marked ignored
pub const IGNORED_LABEL: &str = "ignored";
/// Label of the tests a filter left out
pub const FILTERED_LABEL: &str = "filtered out";
/// Prefix of the line cargo prints before it runs a test binary
pub const RUNNING_PREFIX: &str = "Running ";

/// Number of tests in one bucket of a summary
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TestCount(pub u64);

/// Totals one test binary printed when it finished
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Summary {
    pub is_ok: bool,

    pub passed: TestCount,
    pub failed: TestCount,
    pub ignored: TestCount,
    pub filtered: TestCount,
}

/// Parse a `test result:` line, or return nothing for any other line
pub fn parse_summary(line: &str) -> Option<Summary> {
    let rest = line.trim().strip_prefix(RESULT_PREFIX)?;
    let (status, counts) = rest.split_once(STATUS_END)?;

    let mut summary = Summary {
        is_ok: status == RESULT_OK,
        ..Summary::default()
    };
    for part in counts.split(COUNT_END) {
        let Some((number, label)) = part.split_once(' ') else {
            continue;
        };
        let parsed: Result<u64, _> = number.parse();
        let Ok(count) = parsed else {
            continue;
        };
        match label {
            PASSED_LABEL => summary.passed = TestCount(count),
            FAILED_LABEL => summary.failed = TestCount(count),
            IGNORED_LABEL => summary.ignored = TestCount(count),
            FILTERED_LABEL => summary.filtered = TestCount(count),
            _other => {},
        }
    }
    Some(summary)
}

/// Parse a `Running` line into the name of the test target cargo started
pub fn parse_target(line: &str) -> Option<String> {
    let rest = line.trim().strip_prefix(RUNNING_PREFIX)?;
    let (_source, binary) = rest.split_once('(')?;
    let path = binary.strip_suffix(')')?;

    let file = path.rsplit(['/', '\\']).next()?;
    let (name, _hash) = file.rsplit_once('-')?;
    Some(normalize_target(name))
}

/// Give a target name the spelling cargo uses for its binary
pub fn normalize_target(name: &str) -> String {
    name.trim().replace('-', "_")
}

#[cfg(test)]
mod tests {
    use super::*;

    // a passing summary line gives its four counts
    #[test]
    fn passing_summary() {
        let line = "test result: ok. 3 passed; 0 failed; 1 ignored; 0 measured; 2 filtered out; finished in 0.01s";

        let summary = parse_summary(line).expect("summary line should parse");

        assert!(summary.is_ok);
        assert_eq!(summary.passed, TestCount(3));
        assert_eq!(summary.ignored, TestCount(1));
        assert_eq!(summary.filtered, TestCount(2));
    }

    // a failing summary line is marked failed
    #[test]
    fn failing_summary() {
        let line = "test result: FAILED. 1 passed; 2 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s";

        let summary = parse_summary(line).expect("summary line should parse");

        assert!(!summary.is_ok);
        assert_eq!(summary.failed, TestCount(2));
    }

    // any other line gives no summary
    #[test]
    fn other_line() {
        assert_eq!(parse_summary("test simulation ... ok"), None);
    }

    // an integration target's name comes from its binary path
    #[test]
    fn target_name() {
        let line = "     Running tests/simulation.rs (target/debug/deps/simulation-0f3a9c21d4e5b677)";

        let target = parse_target(line);

        assert_eq!(target, Some("simulation".to_string()));
    }

    // a dash in a target name reads as an underscore
    #[test]
    fn dashed_target() {
        let line = "     Running tests/kill-loop.rs (target/debug/deps/kill_loop-0f3a9c21d4e5b677)";

        let target = parse_target(line);

        assert_eq!(target, Some(normalize_target("kill-loop")));
    }
}
