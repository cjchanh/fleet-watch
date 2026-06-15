//! Parity gate: the Rust port must agree with the Python reference
//! (`fleet_watch.referee`) on every golden vector. Vectors in
//! `tests/golden_vectors.json` are generated FROM the live Python helpers
//! (see PATCHSET_PLAN_2026-06-13.md PS-A verification), so this asserts
//! cross-implementation behavioral identity, not hand-derived expectations.

use fleet_kernel::{overlap_paths, paths_overlap};
use serde_json::Value;
use std::fs;

fn load() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden_vectors.json");
    let text = fs::read_to_string(path).expect("golden_vectors.json must exist");
    serde_json::from_str(&text).expect("golden_vectors.json must be valid JSON")
}

#[test]
fn paths_overlap_matches_python() {
    let data = load();
    let cases = data["paths_overlap"]
        .as_array()
        .expect("paths_overlap array");
    assert!(!cases.is_empty(), "expected non-empty vector set");
    for case in cases {
        let l = case["l"].as_str().unwrap();
        let r = case["r"].as_str().unwrap();
        let expected = case["overlap"].as_bool().unwrap();
        assert_eq!(
            paths_overlap(l, r),
            expected,
            "paths_overlap({l:?}, {r:?}) disagreed with Python reference"
        );
    }
}

#[test]
fn overlap_paths_matches_python() {
    let data = load();
    let cases = data["overlap_paths"]
        .as_array()
        .expect("overlap_paths array");
    assert!(!cases.is_empty(), "expected non-empty vector set");
    for case in cases {
        let requested: Vec<String> = case["requested"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        let held: Vec<String> = case["held"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        let expected: Vec<String> = case["out"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            overlap_paths(&requested, &held),
            expected,
            "overlap_paths({requested:?}, {held:?}) disagreed with Python reference"
        );
    }
}
