//! Real-environment integration test (SPEC.md §9.3): three `ananke-server echo`
//! processes on loopback must satisfy the protocol invariants, journalling to real
//! files under cargo's target directory.

use std::process::{Command, Stdio};

#[test]
fn three_processes_echo_each_other() {
    // A port base derived from the pid keeps parallel test runs apart.
    let base = 20_000 + u16::try_from(std::process::id() % 20_000).unwrap();
    let addrs: Vec<String> = (0..3).map(|i| format!("127.0.0.1:{}", base + i)).collect();
    let journals: Vec<String> = (0..3)
        .map(|i| {
            format!(
                "{}/echo-{}-{i}",
                env!("CARGO_TARGET_TMPDIR"),
                std::process::id()
            )
        })
        .collect();

    let children: Vec<_> = (0..3)
        .map(|i| {
            let peers: Vec<&str> = addrs
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, a)| a.as_str())
                .collect();
            Command::new(env!("CARGO_BIN_EXE_ananke-server"))
                .args([
                    "echo",
                    "--listen",
                    &addrs[i],
                    "--peers",
                    &peers.join(","),
                    "--duration-secs",
                    "2",
                    "--journal",
                    &journals[i],
                ])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("spawn ananke-server")
        })
        .collect();

    for (i, child) in children.into_iter().enumerate() {
        let output = child.wait_with_output().expect("wait for ananke-server");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "node {i} failed:\n{stdout}\n{stderr}"
        );
        assert!(
            stdout.trim_end().ends_with("ok"),
            "node {i} reported a violation:\n{stdout}"
        );
        assert!(stdout.contains("unknown=0 garbage=0"), "node {i}: {stdout}");
        assert!(
            stdout.contains("journal ") && stdout.contains(" corrupt=0 "),
            "node {i} did not journal cleanly: {stdout}"
        );
    }
}
