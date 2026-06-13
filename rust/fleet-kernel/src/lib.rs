//! Fleet Watch governance kernel — Rust port (PS-A scope).
//!
//! This crate is the deterministic decision core of Fleet Watch, ported from
//! the proven Python `fleet_watch.referee`. PS-A intentionally ships ONLY the
//! genuinely pure surface — the `Decision` contract and the lexical
//! path-overlap helpers — so the port can be parity-proven with zero I/O.
//!
//! Deferred by design (see PATCHSET_PLAN_2026-06-13.md):
//!   * `normalize_write_scopes` — touches the filesystem (`expanduser`/`resolve`),
//!     so it belongs to PS-B (read path), not the pure scaffold.
//!   * `check_*` / `claim_*` / kill authority — PS-B and PS-D, each gated.
//!
//! Fail-closed (Invariant #5): `Decision` has NO `Default` impl. Every value is
//! constructed explicitly via [`Decision::allow`] / [`Decision::deny`], so no
//! code path can mint an allow-by-default verdict.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

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

#[cfg(test)]
mod tests {
    use super::*;

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
