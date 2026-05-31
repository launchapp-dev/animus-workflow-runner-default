//! Spawns the plugin binary and exercises the v0.5 JSON-RPC stdio contract
//! end-to-end. Covers:
//!
//! 1. `--manifest` prints a valid manifest with both workflow_runner methods
//!    and no hyphenated method names.
//! 2. `initialize` succeeds when `init_extensions.project_binding.project_root`
//!    is provided, and the response advertises the workflow_runner kind
//!    capability struct (crate_version + extra blob).
//! 3. `initialize` returns a JSON-RPC error when `project_binding` is absent.
//! 4. `workflow/execute` against a tempdir project returns a structured error
//!    (workflow YAML missing) — but the error travels through the wire
//!    envelope, not as a transport crash, proving the dispatch loop is alive.

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};
use tempfile::TempDir;

fn binary_path() -> std::path::PathBuf {
    let target_dir =
        std::env::var("CARGO_BIN_EXE_animus-workflow-runner-default").expect("cargo provides the bin path under test");
    std::path::PathBuf::from(target_dir)
}

#[test]
fn manifest_flag_prints_workflow_runner_manifest() {
    let output = Command::new(binary_path()).arg("--manifest").output().expect("run --manifest");
    assert!(output.status.success(), "expected --manifest to exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let manifest: Value = serde_json::from_str(stdout.trim()).expect("manifest is valid JSON");
    assert_eq!(manifest["plugin_kind"], "workflow_runner");
    let methods = manifest["capabilities"].as_array().expect("capabilities array");
    assert!(methods.iter().any(|m| m == "workflow/execute"));
    assert!(methods.iter().any(|m| m == "workflow/run_phase"));
    for method in methods {
        let s = method.as_str().unwrap();
        assert!(!s.contains('-'), "method name '{s}' must not contain a hyphen");
    }
}

fn send_lines(child_stdin: &mut std::process::ChildStdin, lines: &[Value]) {
    for line in lines {
        let mut s = serde_json::to_string(line).expect("encode frame");
        s.push('\n');
        child_stdin.write_all(s.as_bytes()).expect("write stdin");
    }
    child_stdin.flush().expect("flush stdin");
}

fn read_response_with_id(reader: &mut BufReader<std::process::ChildStdout>, id: i64) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut line = String::new();
    loop {
        if std::time::Instant::now() > deadline {
            panic!("timeout waiting for response id={id}");
        }
        line.clear();
        let n = reader.read_line(&mut line).expect("read stdout");
        if n == 0 {
            panic!("plugin closed stdout before responding to id={id}");
        }
        let frame: Value = match serde_json::from_str(line.trim()) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if frame.get("id").and_then(Value::as_i64) == Some(id) {
            return frame;
        }
    }
}

#[test]
fn initialize_then_workflow_execute_round_trip() {
    let project = TempDir::new().expect("tempdir for project root");

    let mut child = Command::new(binary_path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn plugin binary");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    // 1. initialize
    let init_request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocol_version": "1.1.0",
            "host_info": { "name": "stdio-smoke-test", "version": "0.0.0" },
            "capabilities": { "progress": false, "cancellation": false, "streaming": false },
            "init_extensions": {
                "project_binding": {
                    "project_root": project.path().display().to_string(),
                    "repo_scope": "stdio-smoke"
                }
            }
        }
    });
    send_lines(&mut stdin, &[init_request]);

    let init_response = read_response_with_id(&mut reader, 1);
    assert!(init_response.get("error").is_none(), "initialize should succeed; got {init_response:?}",);
    let result = init_response.get("result").expect("initialize result");
    assert_eq!(result["plugin_info"]["plugin_kind"], "workflow_runner");
    let kind_caps = result.get("kind_capabilities").and_then(Value::as_object).expect("kind_capabilities");
    let wfr = kind_caps.get("workflow_runner").expect("workflow_runner entry");
    assert!(wfr.get("crate_version").is_some(), "missing crate_version");
    let extra = wfr.get("extra").expect("workflow_runner extra");
    assert_eq!(extra["phase_decision_parsing"], true);

    // 2. health/check
    let health_request = json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "health/check",
        "params": {}
    });
    send_lines(&mut stdin, &[health_request]);
    let health_response = read_response_with_id(&mut reader, 2);
    assert_eq!(health_response["result"]["status"], "healthy");

    // 3. workflow/execute against an empty project root — the runner will
    //    fail because workflow YAML is missing, but the error must flow
    //    back through the JSON-RPC envelope (proving dispatch works).
    let execute_request = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "workflow/execute",
        "params": {
            "task_id": "TASK-SMOKE",
            "workflow_ref": "standard",
            "vars": {}
        }
    });
    send_lines(&mut stdin, &[execute_request]);
    let exec_response = read_response_with_id(&mut reader, 3);
    // It is allowed to be either ok with an error-shaped payload OR an error
    // envelope; what matters is the plugin process stays alive and responds.
    assert!(
        exec_response.get("result").is_some() || exec_response.get("error").is_some(),
        "workflow/execute must return either a result or an error: {exec_response:?}",
    );

    // 4. shutdown + exit
    let shutdown = json!({ "jsonrpc": "2.0", "id": 4, "method": "shutdown", "params": {} });
    send_lines(&mut stdin, &[shutdown]);
    let _ = read_response_with_id(&mut reader, 4);
    let _ = stdin.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"exit\"}\n");
    let _ = stdin.flush();
    drop(stdin);

    // Give the child a chance to exit cleanly; fall back to kill.
    let status = match child.wait() {
        Ok(s) => s,
        Err(_) => {
            let _ = child.kill();
            child.wait().expect("reap child")
        }
    };
    let _ = status; // any exit is acceptable post-shutdown
}

#[test]
fn initialize_without_project_binding_errors() {
    let mut child = Command::new(binary_path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn plugin binary");

    let mut stdin = child.stdin.take().expect("stdin");
    let mut reader = BufReader::new(child.stdout.take().expect("stdout"));

    let init_request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocol_version": "1.1.0",
            "host_info": { "name": "stdio-smoke-test", "version": "0.0.0" },
            "capabilities": { "progress": false, "cancellation": false, "streaming": false },
            "init_extensions": {}
        }
    });
    send_lines(&mut stdin, &[init_request]);

    let response = read_response_with_id(&mut reader, 1);
    assert!(response.get("error").is_some(), "initialize without project_binding must error: got {response:?}",);

    let _ = stdin.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"exit\"}\n");
    drop(stdin);
    let _ = child.wait();
}
