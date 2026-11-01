#![cfg(feature = "test-fixtures")]
use ovf_dock::ovf::{MachineOutput, hide_console, read_lines};
use std::process::Stdio;

#[tokio::test]
async fn reads_real_subprocess_machine_output_without_a_shell() {
    let mut cmd = tokio::process::Command::new(env!("CARGO_BIN_EXE_mock-ovftool"));
    cmd.args([
        "--machineOutput",
        "source with spaces",
        "destination with spaces",
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .stdin(Stdio::null());
    hide_console(&mut cmd);
    let mut child = cmd.spawn().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let reader = tokio::spawn(read_lines(child.stdout.take().unwrap(), tx));
    assert!(child.wait().await.unwrap().success());
    reader.await.unwrap();
    let mut parser = MachineOutput::default();
    let mut progress = Vec::new();
    while let Some(line) = rx.recv().await {
        if let Some(p) = parser.feed(&line) {
            progress.push(p);
        }
    }
    assert_eq!(progress, vec![0.0, 0.25, 0.5, 0.75, 1.0]);
    assert!(parser.success);
    assert!(!parser.failed);
}
