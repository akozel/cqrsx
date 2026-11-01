//! A unit test that must run alone in a process of its own: the child process runs one test again with the scenario's
//! name in its environment, so no `set_var` touches the environment of parallel tests.
use std::process::Command;

/// The scenario this process was started for, `None` in the process that started it.
pub fn scenario() -> Option<String> {
    std::env::var("CQRSX_TEST_SCENARIO").ok()
}

/// Runs the unit test `test` (its `module_path!()` and name) again in a child process with the `scenario`, after
/// `configure` set its environment; the child must pass exactly that test. The child starts without the variables
/// `unset` (those of the configurations under test), whatever the environment of the parent.
pub fn isolated(unset: &[&str], test: &str, scenario: &str, configure: impl FnOnce(&mut Command)) {
    let mut child = Command::new(std::env::current_exe().unwrap());
    child.args(["--exact", test.split_once("::").unwrap().1, "--nocapture"]);
    for name in unset {
        child.env_remove(name);
    }
    child.env("CQRSX_TEST_SCENARIO", scenario);
    configure(&mut child);
    let result = child.output().unwrap();
    assert!(
        result.status.success(),
        "isolated {scenario} test failed:\n{}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(
        String::from_utf8_lossy(&result.stdout).contains("1 passed"),
        "the requested unit test must run"
    );
}
