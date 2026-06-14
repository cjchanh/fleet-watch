//! Fleet Watch governance kernel — Rust port (PS-A…PS-D, preempt, reconciler).
//!
//! The deterministic decision core of Fleet Watch, ported from the proven
//! Python `fleet_watch.{referee, events, registry}`; every function is
//! parity-tested against the live Python reference.
//!
//! Landed:
//!   * PS-A — `Decision` contract + lexical path-overlap helpers
//!     (`paths_overlap`, `overlap_paths`), zero I/O.
//!   * PS-B — `normalize_write_scopes` + a Python-faithful `resolve`
//!     (filesystem path resolution, `strict=False` semantics).
//!   * PS-C — `events`: hash-chain ledger core (`compute_event_hash`,
//!     `verify_chain`), SHA-256 parity with Python `_compute_hash`.
//!   * PS-B2 — `registry` (read-only rusqlite layer) + `checks`
//!     (`check_port`, `check_gpu_budget`), fail-closed on DB error.
//!   * PS-D — `ledger`: the write path (`log_event`, `get_last_hash`,
//!     `claim_port`, `release_process`); appends hash-linked rows, fail-closed
//!     (unknown type rejected; un-loggable claim → deny).
//!   * PS-D-preempt — `preempt`: kill authority (`preempt_port`) behind an
//!     injected [`preempt::Signaller`]; production uses libc, tests use a mock
//!     that issues no real signal. Testimony before kill; priority gate
//!     fail-closed.
//!   * reconciler — `check_repo` / `check_repo_with_session`: the single-writer
//!     core. GCs dead+stale leases (`close_session_lease` + CLEAN), auto-releases
//!     dead-PID process holders, and runs write-scope overlap + exclusive /
//!     cooperative checks. Dead-PID detection via the injected `Signaller`;
//!     fail-closed on any DB error.
//!
//! Deferred by design (see PATCHSET_PLAN_2026-06-13.md):
//!   * `fleet guard --json` CLI cutover (PS-E) — wire the kernel into the live
//!     entry point behind a shadow-parity gate.
//!
//! Fail-closed (Invariant #5): `Decision` has NO `Default` impl. Every value is
//! constructed explicitly via [`Decision::allow`] / [`Decision::deny`], so no
//! code path can mint an allow-by-default verdict.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::fs;
use std::path::{Component, Path, PathBuf};

pub mod checks;
pub mod events;
pub mod ledger;
pub mod preempt;
pub mod reconciler;
pub mod registry;

/// Outcome of a claim or guard decision. Mirrors `fleet_watch.referee.Decision`.
///
/// The holder/holders/stale_holders fields are opaque JSON in PS-A (the pure
/// helpers never construct them); they are typed precisely when PS-B ports the
/// registry-reading `check_*` functions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub allowed: bool,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub holder: Option<Value>,
    #[serde(default)]
    pub holders: Vec<Value>,
    #[serde(default)]
    pub overlap_paths: Vec<String>,
    #[serde(default)]
    pub stale_holders: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safe_mode: Option<String>,
}

impl Decision {
    /// Construct an ALLOW verdict. Explicit by design — there is no `Default`.
    pub fn allow(reason: impl Into<String>) -> Self {
        Decision {
            allowed: true,
            reason: reason.into(),
            holder: None,
            holders: Vec::new(),
            overlap_paths: Vec::new(),
            stale_holders: Vec::new(),
            safe_mode: None,
        }
    }

    /// Construct a DENY verdict. Explicit by design — there is no `Default`.
    pub fn deny(reason: impl Into<String>) -> Self {
        Decision {
            allowed: false,
            reason: reason.into(),
            holder: None,
            holders: Vec::new(),
            overlap_paths: Vec::new(),
            stale_holders: Vec::new(),
            safe_mode: None,
        }
    }
}

/// True when two paths overlap (one is the other, or one contains the other).
///
/// Faithful port of `fleet_watch.referee._paths_overlap`. Python uses
/// `PurePath` equality + `Path.relative_to`, both of which are **component-wise**
/// and lexical (no filesystem access). Rust `Path::starts_with` is likewise
/// component-wise, so `/a/b` and `/a/bc` correctly do NOT overlap (they are
/// siblings, not prefix-related) — the load-bearing single-writer property
/// (Phase 0 finding F1). Component normalization matches Python: trailing
/// slashes, `.` segments, and repeated `/` are folded away (e.g. `/a/b/` and
/// `/a/b` overlap), which the golden-vector oracle pins down. Total function:
/// never panics (F3).
pub fn paths_overlap(left: &str, right: &str) -> bool {
    let l = Path::new(left);
    let r = Path::new(right);
    // `starts_with` is reflexive, so equality is subsumed; both directions
    // cover "left under right" and "right under left".
    l.starts_with(r) || r.starts_with(l)
}

/// Collect the scopes that overlap between `requested` and `held`.
///
/// Faithful port of `fleet_watch.referee._overlap_paths`, preserving append
/// order (request scope before held scope) and de-duplication semantics so the
/// output matches the Python reference byte-for-byte.
pub fn overlap_paths(requested: &[String], held: &[String]) -> Vec<String> {
    let mut overlaps: Vec<String> = Vec::new();
    for request_scope in requested {
        for held_scope in held {
            if paths_overlap(request_scope, held_scope) {
                if !overlaps.contains(request_scope) {
                    overlaps.push(request_scope.clone());
                }
                if !overlaps.contains(held_scope) {
                    overlaps.push(held_scope.clone());
                }
            }
        }
    }
    overlaps
}

/// Expand a leading `~` (or `~/...`) to `$HOME`, matching Python
/// `Path.expanduser` for the common forms. `~user` is not expanded (passed
/// through), matching the no-pwd-entry fallback.
fn expanduser(raw: &str) -> PathBuf {
    if raw == "~" {
        if let Some(home) = home_dir() {
            return home;
        }
    } else if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(raw)
}

fn home_dir() -> Option<PathBuf> {
    env::var_os("HOME")
        .map(PathBuf::from)
        .filter(|p| !p.as_os_str().is_empty())
}

/// Collapse `.` and `..` lexically (no filesystem access). Root/prefix is
/// preserved; a `..` with nothing above root is dropped.
fn lexical_collapse(path: &Path) -> PathBuf {
    let mut out: Vec<Component> = Vec::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                if matches!(out.last(), Some(Component::Normal(_))) {
                    out.pop();
                }
            }
            other => out.push(other),
        }
    }
    let mut result = PathBuf::new();
    for comp in out {
        result.push(comp.as_os_str());
    }
    result
}

/// Resolve like Python `Path.resolve(strict=False)`: canonicalize the longest
/// existing ancestor (so symlinks such as `/tmp -> /private/tmp` are followed),
/// then re-attach the non-existent tail with lexical `.`/`..` collapse. Relative
/// inputs are absolutized against the current directory, mirroring Python.
/// Total function — never panics, never requires the path to exist (F3).
///
/// Known edge (documented, not a bug): a `..` in the non-existent tail that
/// would re-cross a symlinked component of the existing prefix is collapsed
/// lexically rather than re-resolved; realistic scope inputs (repo
/// subdirectories) do not hit this.
fn resolve(path: &Path) -> PathBuf {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    };
    let mut tail: Vec<std::ffi::OsString> = Vec::new();
    let mut cur = abs.clone();
    loop {
        if let Ok(real) = fs::canonicalize(&cur) {
            let mut result = real;
            for name in tail.iter().rev() {
                result.push(name);
            }
            return lexical_collapse(&result);
        }
        match cur.file_name() {
            Some(name) => {
                tail.push(name.to_os_string());
                if !cur.pop() {
                    break;
                }
            }
            None => break,
        }
    }
    lexical_collapse(&abs)
}

/// Resolve write scopes to stable absolute paths for overlap checks. Faithful
/// port of `fleet_watch.referee.normalize_write_scopes`: each scope is
/// `~`-expanded, joined onto the resolved `repo_dir` base when relative, then
/// resolved; results are de-duplicated preserving first-seen order.
///
/// `repo_dir = None` (or empty) means no base — relative scopes then resolve
/// against the current directory, exactly as Python's `Path.resolve()` does.
/// Feeds [`overlap_paths`]; correct resolution is single-writer-load-bearing.
pub fn normalize_write_scopes(repo_dir: Option<&str>, write_scopes: &[String]) -> Vec<String> {
    if write_scopes.is_empty() {
        return Vec::new();
    }
    let base: Option<PathBuf> = repo_dir
        .filter(|s| !s.is_empty())
        .map(|s| resolve(&expanduser(s)));
    let mut resolved: Vec<String> = Vec::new();
    for raw in write_scopes {
        let mut path = expanduser(raw);
        if path.is_relative() {
            if let Some(base) = &base {
                path = base.join(path);
            }
        }
        let value = resolve(&path).to_string_lossy().into_owned();
        if !resolved.contains(&value) {
            resolved.push(value);
        }
    }
    resolved
}

/// Resolve a repo path to its canonical absolute form.
///
/// Mirrors Python `str(Path(repo_dir).resolve())` (strict=False): follows
/// real symlinks on the longest existing prefix, then appends any non-existent
/// tail with lexical collapse.  Used by `reconciler::check_repo_with_session`
/// to produce the `resolved_repo_dir` that matches what the Python referee
/// stores in SQLite.
pub fn resolve_repo(repo_dir: &str) -> String {
    resolve(&expanduser(repo_dir))
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexical_collapse_handles_parent_and_current() {
        assert_eq!(
            lexical_collapse(Path::new("/a/b/../c")),
            PathBuf::from("/a/c")
        );
        assert_eq!(lexical_collapse(Path::new("/a/./b")), PathBuf::from("/a/b"));
        assert_eq!(lexical_collapse(Path::new("/../a")), PathBuf::from("/a"));
    }

    #[test]
    fn decision_is_fail_closed_by_construction() {
        // No Default impl exists; explicit constructors only.
        assert!(Decision::allow("ok").allowed);
        assert!(!Decision::deny("blocked").allowed);
    }

    #[test]
    fn equal_paths_overlap() {
        assert!(paths_overlap("/a/b", "/a/b"));
    }

    #[test]
    fn parent_child_overlap_both_directions() {
        assert!(paths_overlap("/a/b", "/a"));
        assert!(paths_overlap("/a", "/a/b"));
    }

    #[test]
    fn sibling_prefix_does_not_overlap() {
        // The adversarial case (Phase 0 F1): string-prefix would wrongly say
        // True; component-wise correctly says False.
        assert!(!paths_overlap("/a/b", "/a/bc"));
        assert!(!paths_overlap("/a/bc", "/a/b"));
    }

    #[test]
    fn disjoint_paths_do_not_overlap() {
        assert!(!paths_overlap("/a/b", "/c/d"));
    }

    #[test]
    fn property_overlap_is_symmetric() {
        let samples = [
            ("/a/b", "/a"),
            ("/a/b", "/a/bc"),
            ("/x", "/x/y/z"),
            ("/a/b", "/c/d"),
            ("/a/b", "/a/b"),
        ];
        for (a, b) in samples {
            assert_eq!(
                paths_overlap(a, b),
                paths_overlap(b, a),
                "overlap must be symmetric for {a:?},{b:?}"
            );
        }
    }

    #[test]
    fn property_overlap_is_reflexive() {
        for p in ["/", "/a", "/a/b/c", "relative/x"] {
            assert!(paths_overlap(p, p), "a path must overlap itself: {p:?}");
        }
    }

    #[test]
    fn overlap_paths_preserves_order_and_dedup() {
        let requested = vec!["/a/b".to_string()];
        let held = vec!["/a".to_string()];
        assert_eq!(overlap_paths(&requested, &held), vec!["/a/b", "/a"]);
        // No overlap → empty.
        assert!(overlap_paths(&["/a/b".to_string()], &["/c".to_string()]).is_empty());
        // Empty requested → empty.
        assert!(overlap_paths(&[], &["/a".to_string()]).is_empty());
    }
}
