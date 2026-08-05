//! Read-only guard checks — `check_port` and `check_gpu_budget`.
//!
//! # THIS IS NOT A PORT OF THE CURRENT PYTHON. DO NOT TREAT IT AS ONE.
//!
//! These functions were transcribed from `fleet_watch.referee` as it stood in
//! June 2026. That Python no longer exists. Both checks have since gained an
//! authority this kernel does not implement, and the divergence is deliberate —
//! the OS-truth layer was NOT ported (see the section below).
//!
//! The phrase "faithful port" must never reappear in this file. It was the
//! original false claim, and `tests/test_rust_kernel_divergence.py` fails on it.
//!
//! What diverged, and why the difference is a FAIL-OPEN in this kernel's
//! direction:
//!
//! * `check_port` — Python now consults the OS socket table in addition to the
//!   registry: it binds the port on loopback, and refuses when the bind fails,
//!   when the port is privileged, or when the probe itself could not run. This
//!   kernel still answers from the registry alone, so for a port held by a LIVE
//!   but UNREGISTERED listener, Python returns DENY and this returns
//!   `allow("port available")`. That was the original defect
//!   (`fleet guard --json --port 8765` -> allowed:true while `bind()` raised
//!   EADDRINUSE), and it is still live here.
//!
//! * `check_gpu_budget` — Python now reads live VRAM residency from local
//!   runtimes and refuses when telemetry cannot be read. This kernel performs
//!   ledger arithmetic over DECLARED MB only, so an unregistered consumer
//!   holding the device is invisible to it.
//!
//! Consequence for anyone comparing the two: the vectors in
//! `tests/checks_vectors.json` encode THIS kernel's registry-only behaviour and
//! carry a `mirrors_python` flag saying whether each one still matches Python.
//! `tests/gen_checks_vectors.py` regenerates them against the REAL referee and
//! refuses to silently re-bless a divergent case. `scripts/shadow_parity.py`
//! classifies each disagreement as expected-divergence or regression. A green
//! `cargo test` here means "this kernel is self-consistent", never "this kernel
//! agrees with the guard the operator actually runs".
//!
//! Fail-closed contract (still holds): any `rusqlite::Error` (DB unreadable,
//! row missing, type mismatch) produces `Decision::deny("registry unreadable:
//! <err>")`. No `unwrap`, no `panic`, no allow-on-error path.

use rusqlite::Connection;

use crate::registry;
use crate::Decision;

// ── check_port ───────────────────────────────────────────────────────────────

/// Return whether `port` is free ACCORDING TO THE REGISTRY ONLY.
///
/// DIVERGENT from `fleet_watch.referee.check_port`, which additionally probes
/// the OS socket table and refuses a port a live unregistered listener holds.
/// The Python snippet this function used to quote as its source has been
/// deleted; quoting it here again would re-assert a parity that does not exist.
///
/// Concretely: for a port with a live unregistered listener, Python denies and
/// this allows. Do not wire this into a guard path without porting
/// `referee.probe_port` first.
///
/// Error path: any registry failure → `deny("registry unreadable: <err>")`.
pub fn check_port(conn: &Connection, port: i64) -> Decision {
    match registry::get_process_by_port(conn, port) {
        Err(e) => Decision::deny(format!("registry unreadable: {e}")),
        Ok(None) => Decision::allow("port available"),
        Ok(Some(h)) => Decision::deny(format!(
            "port {port} claimed by PID {pid} ({name})",
            pid = h.pid,
            name = h.name,
        )),
    }
}

// ── check_gpu_budget ─────────────────────────────────────────────────────────

/// Return whether `gpu_mb` fits the DECLARED ledger. Reads no device telemetry.
///
/// DIVERGENT from `fleet_watch.referee.check_gpu_budget`, which now combines
/// this ledger with live VRAM residency and fails closed when telemetry cannot
/// be read. The Python snippet this function used to quote has been deleted.
///
/// Concretely: with an empty ledger and a runtime holding the device, Python
/// denies and this allows.
///
/// Error path: any registry failure → `deny("registry unreadable: <err>")`.
pub fn check_gpu_budget(conn: &Connection, gpu_mb: i64) -> Decision {
    if gpu_mb <= 0 {
        return Decision::allow("no GPU claim");
    }
    let budget = match registry::get_gpu_budget(conn) {
        Ok(b) => b,
        Err(e) => return Decision::deny(format!("registry unreadable: {e}")),
    };
    if gpu_mb <= budget.available_mb {
        return Decision::allow(format!(
            "{gpu_mb}MB fits in {avail}MB available",
            avail = budget.available_mb,
        ));
    }
    Decision::deny(format!(
        "GPU budget exceeded: requesting {gpu_mb}MB but only \
         {avail}MB available \
         ({alloc}MB allocated of {allocatable}MB allocatable)",
        avail = budget.available_mb,
        alloc = budget.allocated_mb,
        allocatable = budget.total_mb - budget.reserve_mb,
    ))
}
