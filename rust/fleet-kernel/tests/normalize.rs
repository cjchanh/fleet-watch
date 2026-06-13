//! Parity gate for `normalize_write_scopes` vs the live Python
//! `fleet_watch.referee.normalize_write_scopes`. Vectors in
//! `tests/normalize_vectors.json` are generated from Python over STABLE machine
//! paths (the fleet-watch repo, `$HOME`, `/tmp`); regenerate on this host if the
//! Python reference changes. Resolution is filesystem-dependent (symlinks,
//! existence), so this asserts cross-implementation identity on the host that
//! generated the vectors.

use fleet_kernel::normalize_write_scopes;
use serde_json::Value;
use std::fs;

#[test]
fn normalize_matches_python() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/normalize_vectors.json");
    let text = fs::read_to_string(path).expect("normalize_vectors.json must exist");
    let cases: Value =
        serde_json::from_str(&text).expect("normalize_vectors.json must be valid JSON");
    let arr = cases.as_array().expect("top-level array");
    assert!(!arr.is_empty(), "expected non-empty vector set");
    for case in arr {
        let repo_dir = case["repo_dir"].as_str(); // None for JSON null
        let scopes: Vec<String> = case["scopes"]
            .as_array()
            .expect("scopes array")
            .iter()
            .map(|v| v.as_str().expect("scope is string").to_string())
            .collect();
        let expected: Vec<String> = case["expected"]
            .as_array()
            .expect("expected array")
            .iter()
            .map(|v| v.as_str().expect("expected entry is string").to_string())
            .collect();
        assert_eq!(
            normalize_write_scopes(repo_dir, &scopes),
            expected,
            "normalize_write_scopes(repo_dir={repo_dir:?}, scopes={scopes:?}) disagreed with Python"
        );
    }
}

#[test]
fn normalize_empty_is_empty() {
    assert!(normalize_write_scopes(Some("/Users/cj"), &[]).is_empty());
    assert!(normalize_write_scopes(None, &[]).is_empty());
}

#[test]
fn normalize_dedups_nonexistent_absolute() {
    // A non-existent absolute path resolves lexically (no filesystem symlink),
    // so this case is host-independent.
    let scopes = vec![
        "/zzz_fk_nonexistent/a".to_string(),
        "/zzz_fk_nonexistent/a".to_string(),
    ];
    assert_eq!(
        normalize_write_scopes(None, &scopes),
        vec!["/zzz_fk_nonexistent/a".to_string()]
    );
}
