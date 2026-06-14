//! Read-only shadow-parity probe (PS-E, pre-cutover evidence).
//!
//! Opens a registry DB, runs ONE kernel decision, and prints it as JSON so a
//! harness can diff the Rust kernel against the Python referee on real data.
//!
//! SAFETY:
//!   * `check_port` / `check_gpu_budget` open the DB **read-only** — safe even
//!     against the live registry.
//!   * `check_repo` GCs stale leases and auto-releases dead PIDs, so it opens
//!     the DB read-WRITE and MUST be pointed at a throwaway SNAPSHOT, never the
//!     live `~/.fleet-watch/registry.db`. The harness enforces this.
//!
//! This binary never promotes anything: it only reports a decision.

use fleet_kernel::{checks, preempt::RealSignaller, reconciler, registry};
use std::path::Path;
use std::process::exit;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: kernel_shadow <db> <check_port|check_gpu_budget|check_repo> ...");
        exit(2);
    }
    let db = &args[1];
    let decision = match args[2].as_str() {
        "check_port" => {
            let port: i64 = args[3].parse().expect("port must be an integer");
            let conn = registry::open_readonly(Path::new(db)).expect("open read-only");
            checks::check_port(&conn, port)
        }
        "check_gpu_budget" => {
            let mb: i64 = args[3].parse().expect("mb must be an integer");
            let conn = registry::open_readonly(Path::new(db)).expect("open read-only");
            checks::check_gpu_budget(&conn, mb)
        }
        "check_repo" => {
            // MUTATING — `db` MUST be a throwaway snapshot, not the live DB.
            let ts = &args[3];
            let repo = &args[4];
            let session = args.get(5).map(String::as_str);
            let mut conn = rusqlite::Connection::open(db).expect("open read-write snapshot");
            reconciler::check_repo_with_session(
                &mut conn,
                ts,
                repo,
                session,
                &[],
                false,
                &RealSignaller,
            )
        }
        other => {
            eprintln!("unknown command: {other}");
            exit(2);
        }
    };
    println!(
        "{}",
        serde_json::to_string(&decision).expect("serialize Decision")
    );
}
