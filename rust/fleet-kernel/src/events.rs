//! Hash-chained event log types and verification — Rust port of
//! `fleet_watch.events`.
//!
//! Ported items (PS-C):
//!   * `GENESIS_HASH` — sentinel that anchors the first event.
//!   * `compute_event_hash` — byte-identical to Python `_compute_hash`;
//!     SHA-256 over `"{prev}|{ts}|{type}|{detail}"` encoded as UTF-8.
//!   * `Event` — value object for a single chain entry (pure data, no DB).
//!   * `verify_chain` — pure slice verifier; mirrors Python `verify_chain`
//!     semantics: empty slice → (true, 0); first mismatch → (false, 0);
//!     fully valid → (true, len).
//!
//! Intentionally omitted (DB layer, not a pure-core concern):
//!   * `log_event`, `get_events`, `get_last_hash` — all require a SQLite
//!     connection; belong in a registry layer above this module.
//!
//! Fail-closed (Invariant #5): `verify_chain` never panics on any input.

use hex::encode as hex_encode;
use sha2::{Digest, Sha256};

/// The sentinel hash that precedes the very first event. Matches Python
/// `GENESIS_HASH = "genesis"` exactly.
pub const GENESIS_HASH: &str = "genesis";

/// Compute the SHA-256 hex digest for one event, byte-identical to the Python
/// reference implementation:
///
/// ```python
/// hashlib.sha256(f"{prev_hash}|{timestamp}|{event_type}|{detail}".encode()).hexdigest()
/// ```
///
/// The format string uses `|` as a separator; the payload is encoded as UTF-8
/// (Python's default for `.encode()`). Output is lowercase hex, 64 characters.
///
/// Total function — never panics (F3).
pub fn compute_event_hash(
    prev_hash: &str,
    timestamp: &str,
    event_type: &str,
    detail: &str,
) -> String {
    let payload = format!("{prev_hash}|{timestamp}|{event_type}|{detail}");
    let digest = Sha256::digest(payload.as_bytes());
    hex_encode(digest)
}

/// A single event in the hash-chained audit log. Pure data — no DB handles.
///
/// Field names mirror the SQLite column names in `fleet_watch/events.py` so
/// that a registry layer can deserialize rows directly into this type.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub timestamp: String,
    pub event_type: String,
    /// JSON-serialised detail string, e.g. `"{}"` or `"{\"pid\":1234}"`.
    pub detail: String,
    pub prev_hash: String,
    pub hash: String,
}

/// Verify the integrity of a hash chain.
///
/// Mirrors Python `verify_chain` semantics exactly:
/// * Empty slice → `(true, 0)`.
/// * Walk events in order (assumed pre-sorted by id / insertion order).
///   For each event:
///
///   1. `prev_hash` must equal the expected previous hash (starting from
///      `GENESIS_HASH`).
///   2. `sha256(prev_hash|timestamp|event_type|detail)` must equal the
///      stored `hash`.
///
///   On the **first** mismatch in either check: return `(false, 0)`.
/// * If all events pass: return `(true, events.len())`.
///
/// The `(false, 0)` return on mismatch matches the Python implementation
/// exactly — it returns `(False, 0)` immediately regardless of how far the
/// walk had progressed before encountering the bad row.
///
/// Total function — never panics on any input (F3).
pub fn verify_chain(events: &[Event]) -> (bool, usize) {
    if events.is_empty() {
        return (true, 0);
    }

    let mut expected_prev = GENESIS_HASH.to_owned();
    for event in events {
        // Check 1: linkage — prev_hash must equal the expected previous hash.
        if event.prev_hash != expected_prev {
            return (false, 0);
        }
        // Check 2: integrity — recompute must equal stored hash.
        let computed = compute_event_hash(
            &event.prev_hash,
            &event.timestamp,
            &event.event_type,
            &event.detail,
        );
        if computed != event.hash {
            return (false, 0);
        }
        expected_prev = event.hash.clone();
    }

    (true, events.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── unit: compute_event_hash ────────────────────────────────────────────

    #[test]
    fn genesis_hash_constant_matches_python() {
        assert_eq!(GENESIS_HASH, "genesis");
    }

    #[test]
    fn hash_format_matches_python_pipe_separated() {
        // Validates the `|` separator and UTF-8 encoding by checking one
        // well-known vector against a manually computed Python reference.
        let h = compute_event_hash("genesis", "2026-06-13T00:00:00+00:00", "REGISTER", "{}");
        assert_eq!(
            h,
            "2cea8bc2897dcc195169909aed51df6c447379c94991775000dcfbe76f4d49dd"
        );
    }

    #[test]
    fn hash_empty_detail_matches_python() {
        // Empty string detail — different from `"{}"`.
        let h = compute_event_hash("genesis", "2026-06-13T00:00:00+00:00", "REGISTER", "");
        assert_eq!(
            h,
            "ac54a521be754f11a4a92d2a13d44b7a767e65c350ebb2d68d3f0cae69b89e92"
        );
    }

    #[test]
    fn hash_is_deterministic() {
        let a = compute_event_hash("p", "t", "KILL", "d");
        let b = compute_event_hash("p", "t", "KILL", "d");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_differs_on_any_field_change() {
        let base = compute_event_hash("genesis", "2026-06-13T00:00:00+00:00", "REGISTER", "{}");
        assert_ne!(
            base,
            compute_event_hash("GENESIS", "2026-06-13T00:00:00+00:00", "REGISTER", "{}")
        );
        assert_ne!(
            base,
            compute_event_hash("genesis", "2026-06-13T00:00:01+00:00", "REGISTER", "{}")
        );
        assert_ne!(
            base,
            compute_event_hash("genesis", "2026-06-13T00:00:00+00:00", "HEARTBEAT", "{}")
        );
        assert_ne!(
            base,
            compute_event_hash("genesis", "2026-06-13T00:00:00+00:00", "REGISTER", "{x}")
        );
    }

    // ── unit: verify_chain ──────────────────────────────────────────────────

    #[test]
    fn empty_chain_is_valid() {
        assert_eq!(verify_chain(&[]), (true, 0));
    }

    #[test]
    fn single_valid_event_passes() {
        let h = compute_event_hash(
            GENESIS_HASH,
            "2026-06-13T10:00:00+00:00",
            "REGISTER",
            "{\"pid\":1}",
        );
        let events = vec![Event {
            timestamp: "2026-06-13T10:00:00+00:00".into(),
            event_type: "REGISTER".into(),
            detail: "{\"pid\":1}".into(),
            prev_hash: GENESIS_HASH.into(),
            hash: h,
        }];
        assert_eq!(verify_chain(&events), (true, 1));
    }

    #[test]
    fn single_event_wrong_prev_hash_is_invalid() {
        let h = compute_event_hash(GENESIS_HASH, "2026-06-13T10:00:00+00:00", "REGISTER", "{}");
        let events = vec![Event {
            timestamp: "2026-06-13T10:00:00+00:00".into(),
            event_type: "REGISTER".into(),
            detail: "{}".into(),
            prev_hash: "wrong_prev".into(), // linkage break
            hash: h,
        }];
        assert_eq!(verify_chain(&events), (false, 0));
    }

    #[test]
    fn single_event_wrong_stored_hash_is_invalid() {
        let events = vec![Event {
            timestamp: "2026-06-13T10:00:00+00:00".into(),
            event_type: "REGISTER".into(),
            detail: "{}".into(),
            prev_hash: GENESIS_HASH.into(),
            hash: "deadbeef".into(), // integrity break
        }];
        assert_eq!(verify_chain(&events), (false, 0));
    }
}
