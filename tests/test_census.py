"""Tests for `fleet census` — probes, verdict engine, receipt contract, drift.

Unit-heavy and negative-first: the interesting cases are the ones where the
machine lies or a probe returns nothing. A census that silently drops the item
it could not parse is worse than no census at all.
"""

from __future__ import annotations

import json
import plistlib
from pathlib import Path

import pytest

from fleet_watch import census
from fleet_watch.census import domains, probes, receipt, verdicts

# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def _write_plist(directory: Path, name: str, data: dict) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.plist"
    with open(path, "wb") as handle:
        plistlib.dump(data, handle)
    return path


def _proc(pid: int, command: str, **kwargs) -> probes.ProcInfo:
    return probes.ProcInfo(
        pid=pid,
        ppid=kwargs.get("ppid", 1),
        stat=kwargs.get("stat", "S"),
        etime_seconds=kwargs.get("etime_seconds", 60),
        cpu_percent=kwargs.get("cpu_percent", 0.0),
        rss_kb=kwargs.get("rss_kb", 1024),
        user=kwargs.get("user", "cj"),
        command=command,
    )


def _snapshot(**kwargs) -> probes.SystemSnapshot:
    machine = kwargs.pop(
        "machine", probes.MachineInfo(host="testhost", os="Darwin 25.5.0", cores=8, ram_gb=16)
    )
    return probes.SystemSnapshot(machine=machine, **kwargs)


def _minimal_payload(verdict: str = "keep") -> dict:
    item = {
        "label": "com.example.job",
        "path": "~/Library/LaunchAgents/com.example.job.plist",
        "status": "running",
        "evidence": "launchctl list: PID 1",
        "verdict": verdict,
        "reason": "it runs",
    }
    totals = {"items": 1, "keep": 0, "investigate": 0, "close": 0, "remove": 0}
    totals[verdict] = 1
    return {
        "schema_version": receipt.SCHEMA_VERSION,
        "generated_at": "2026-07-27T12:00:00Z",
        "host": "testhost",
        "machine": {"os": "Darwin 25.5.0", "cores": 8, "ram_gb": 16},
        "totals": totals,
        "domains": [
            {
                "domain_id": "user-launch-agents",
                "domain": "user LaunchAgents (~/Library/LaunchAgents)",
                "summary": "one job",
                "totals": {"items": 1},
                "items": [item],
            }
        ],
        "drift": {
            "prior_receipt": None,
            "prior_status": "absent",
            "excluded_domains": [],
            "new_items": [],
            "disappeared": [],
            "verdict_changes": [],
        },
    }


# --------------------------------------------------------------------------
# probes — parsing
# --------------------------------------------------------------------------


def test_parse_launchctl_list_reads_pid_exit_and_signal_death():
    parsed = probes.parse_launchctl_list(
        "PID\tStatus\tLabel\n"
        "1502\t0\tcom.logi.cp-dev-mgr\n"
        "-\t78\tcom.cds.signalcheck-refresh\n"
        "-\t-9\tcom.cj.boot-reconciler\n"
    )
    assert parsed["com.logi.cp-dev-mgr"].pid == 1502
    assert parsed["com.cds.signalcheck-refresh"].pid is None
    assert parsed["com.cds.signalcheck-refresh"].last_exit == 78
    assert parsed["com.cj.boot-reconciler"].last_exit == -9
    assert "Label" not in parsed


def test_parse_launchctl_list_on_empty_output_returns_nothing():
    assert probes.parse_launchctl_list("") == {}
    assert probes.parse_launchctl_list("PID\tStatus\tLabel\n") == {}


def test_parse_print_disabled_handles_both_output_dialects():
    parsed = probes.parse_print_disabled(
        'disabled services = {\n'
        '  "ai.cds.sovereign-stack" => disabled\n'
        '  "com.docker.helper" => enabled\n'
        '  "legacy.job" => true\n'
        "}\n"
    )
    assert parsed["ai.cds.sovereign-stack"] is True
    assert parsed["com.docker.helper"] is False
    assert parsed["legacy.job"] is True


@pytest.mark.parametrize(
    "raw,expected",
    [("00:30", 30), ("05:00", 300), ("01:00:00", 3600), ("01-05:00:01", 104401), ("bogus", 0)],
)
def test_parse_etime(raw, expected):
    assert probes.parse_etime(raw) == expected


def test_parse_ps_extracts_full_command_with_spaces():
    procs = probes.parse_ps(
        "    1     0 Ss   01-05:00:33   0.7  29376 root             /sbin/launchd\n"
        " 70256     1 R    00:12         99.3 145456 cj               node tests/bench.js 5\n"
    )
    assert [p.pid for p in procs] == [1, 70256]
    assert procs[1].command == "node tests/bench.js 5"
    assert procs[1].cpu_percent == 99.3
    assert procs[0].etime_seconds == 104433


def test_parse_lsof_dedupes_dual_stack_and_prefers_the_exposed_binding():
    listeners = probes.parse_lsof_fields(
        "p646\ncrapportd\nf11\nn127.0.0.1:56365\nf16\nn*:56365\n"
        "p888\ncIPNExtension\nf12\nn127.0.0.1:49160\n"
    )
    assert len(listeners) == 2
    rapportd = next(item for item in listeners if item.pid == 646)
    assert rapportd.address == "*" and rapportd.is_wildcard
    ipn = next(item for item in listeners if item.pid == 888)
    assert ipn.is_loopback and ipn.port == 49160


def test_parse_lsof_on_empty_output_invents_nothing():
    assert probes.parse_lsof_fields("") == []


def test_parse_crontab_skips_comments_and_env_assignments():
    entries = probes.parse_crontab(
        "# a comment\nPATH=/usr/bin\n0 8 * * * /Users/cj/tools/run.sh briefing\n"
        "@reboot /Users/cj/tools/boot.sh\n"
    )
    assert len(entries) == 2
    assert entries[0].schedule == "0 8 * * *"
    assert entries[0].command.startswith("/Users/cj/tools/run.sh")
    assert entries[1].schedule == "@reboot"


def test_parse_btm_selects_login_items_and_apps_only():
    items = probes.parse_btm(
        " Records for UID 501 : ABC\n"
        " #1:\n"
        "                 Name: nextdns\n"
        "                 Type: legacy daemon (0x10010)\n"
        "          Disposition: [enabled, allowed, notified] (0xb)\n"
        "           Identifier: 16.nextdns\n"
        " #2:\n"
        "                 Name: Rectangle\n"
        "                 Type: app (0x10)\n"
        "          Disposition: [enabled, allowed, notified] (0xb)\n"
        "           Identifier: com.knollsoft.Rectangle\n"
        "      Executable Path: /Applications/Rectangle.app\n"
    )
    assert [item.name for item in items] == ["Rectangle"]
    assert items[0].uid == 501 and items[0].enabled


def test_read_plist_dir_on_missing_directory_returns_empty_not_error(tmp_path):
    assert probes.read_plist_dir(tmp_path / "does-not-exist") == []


def test_unparseable_plist_is_surfaced_not_dropped(tmp_path):
    path = tmp_path / "com.broken.job.plist"
    path.write_bytes(b"<<< this is not a plist >>>")
    parsed = probes.parse_plist_file(path)
    assert parsed.parse_error is not None
    assert parsed.label == "com.broken.job"


def test_plist_label_key_wins_over_filename(tmp_path):
    path = _write_plist(
        tmp_path, "com.logi.optionsplus", {"Label": "com.logi.cp-dev-mgr", "Program": "/bin/ls"}
    )
    assert probes.parse_plist_file(path).label == "com.logi.cp-dev-mgr"


# --------------------------------------------------------------------------
# probes — target resolution (regression: the space-in-path false positive)
# --------------------------------------------------------------------------


def test_interpreter_script_path_containing_a_space_is_not_truncated(tmp_path):
    """Regression: `bash "/Signal Check/refresh.sh"` is ONE path, not two.

    Truncating at the space reported a healthy job as missing and marked it
    `remove` — the most destructive verdict the engine can emit.
    """
    script_dir = tmp_path / "Signal Check"
    script_dir.mkdir()
    script = script_dir / "refresh_signalcheck.sh"
    script.write_text("#!/bin/bash\n")
    plist = probes.ParsedPlist(
        path=tmp_path / "com.cds.signalcheck-refresh.plist",
        label="com.cds.signalcheck-refresh",
        target="/bin/bash",
        program_arguments=("/bin/bash", str(script)),
        triggers=("StartCalendarInterval",),
        keys=("Label", "ProgramArguments"),
    )
    target, exists, note = probes.resolve_job_target(plist)
    assert exists is True
    assert target == str(script)
    assert "script exists" in note


def test_interpreter_command_string_resolves_to_its_first_token(tmp_path):
    script = tmp_path / "run.sh"
    script.write_text("#!/bin/bash\n")
    plist = probes.ParsedPlist(
        path=tmp_path / "job.plist",
        label="job",
        target="/bin/bash",
        program_arguments=("/bin/bash", "-c", f"{script} --flag"),
    )
    target, exists, _ = probes.resolve_job_target(plist)
    assert exists is True and target == str(script)


def test_missing_script_behind_a_present_interpreter_is_reported_missing(tmp_path):
    plist = probes.ParsedPlist(
        path=tmp_path / "job.plist",
        label="job",
        target="/bin/bash",
        program_arguments=("/bin/bash", "/Users/cj/gone/watchdog.sh"),
    )
    target, exists, note = probes.resolve_job_target(plist)
    assert exists is False
    assert target == "/Users/cj/gone/watchdog.sh"
    assert "script it runs is missing" in note


def test_job_with_no_program_reports_undeclared_not_missing(tmp_path):
    plist = probes.ParsedPlist(path=tmp_path / "job.plist", label="job")
    target, exists, note = probes.resolve_job_target(plist)
    assert target is None and exists is None
    assert "no Program" in note


@pytest.mark.parametrize(
    "command,expected",
    [
        ("/Applications/Spotify.app/Contents/MacOS/Spotify", "Spotify.app"),
        (
            "/opt/homebrew/Cellar/python@3.14/3.14.5/Frameworks/Python.framework/"
            "Versions/3.14/Resources/Python.app/Contents/MacOS/Python /Users/cj/srv.py",
            "Python srv.py",
        ),
        ("/opt/homebrew/bin/git fsmonitor--daemon run", "git fsmonitor--daemon"),
        ("/usr/bin/python3 -c from multiprocessing import x", "python3 -c (inline)"),
        ("/usr/bin/python3 -m http.server", "python3 -m http.server"),
        ("/sbin/launchd", "launchd"),
        ("", "unknown"),
    ],
)
def test_cluster_key(command, expected):
    assert probes.cluster_key(command) == expected


def test_synthetic_labels_are_recognised():
    assert probes.is_synthetic_label("application.com.google.Chrome.951.292")
    assert probes.is_synthetic_label("com.apple.progressd")
    assert not probes.is_synthetic_label("com.ollama.ollama")


def test_run_probe_on_missing_executable_fails_closed():
    result = probes.run_probe(["fleet-watch-no-such-binary-xyz"])
    assert result.ok is False
    assert "not found" in (result.error or "")
    assert "probe returned nothing" in result.evidence


# --------------------------------------------------------------------------
# verdict engine
# --------------------------------------------------------------------------


def test_judgment_rejects_an_off_contract_status():
    with pytest.raises(ValueError):
        verdicts.Judgment(status="fine", verdict="keep", reason="r", rule="x")
    with pytest.raises(ValueError):
        verdicts.Judgment(status="running", verdict="delete", reason="r", rule="x")


def test_unparseable_plist_is_unknown_and_investigated(tmp_path):
    plist = probes.ParsedPlist(
        path=tmp_path / "j.plist", label="j", parse_error="PermissionError: denied"
    )
    judgment = verdicts.judge_launchd_agent(plist, None, False, None)
    assert (judgment.status, judgment.verdict) == ("unknown", "investigate")
    assert judgment.rule == "user-agent/unparseable"


def test_missing_target_is_stale_and_a_remove_candidate(tmp_path):
    plist = probes.ParsedPlist(
        path=tmp_path / "j.plist", label="com.x.j", target="/gone", keys=("Label",)
    )
    judgment = verdicts.judge_launchd_agent(plist, None, False, False, "/gone", "")
    assert (judgment.status, judgment.verdict) == ("stale", "remove")
    assert judgment.close_command is not None
    assert "bootout" in judgment.close_command


def test_nonzero_last_exit_is_failing_and_signal_death_is_named(tmp_path):
    plist = probes.ParsedPlist(path=tmp_path / "j.plist", label="com.x.j", keys=("Label",))
    entry = probes.LaunchctlEntry("com.x.j", pid=None, last_exit=-9)
    judgment = verdicts.judge_launchd_agent(plist, entry, False, True)
    assert (judgment.status, judgment.verdict) == ("failing", "investigate")
    assert "signal 9" in judgment.reason


def test_spent_one_shot_is_investigated_but_a_normal_idle_job_is_kept(tmp_path):
    entry = probes.LaunchctlEntry("x", pid=None, last_exit=0)
    one_shot = probes.ParsedPlist(
        path=tmp_path / "a.plist", label="com.x.db-migrate", keys=("Label",)
    )
    routine = probes.ParsedPlist(
        path=tmp_path / "b.plist", label="com.x.sync", keys=("Label",)
    )
    assert verdicts.judge_launchd_agent(one_shot, entry, False, True).verdict == "investigate"
    assert verdicts.judge_launchd_agent(routine, entry, False, True).verdict == "keep"


def test_explicitly_disabled_job_is_kept_not_flagged(tmp_path):
    plist = probes.ParsedPlist(path=tmp_path / "j.plist", label="com.x.j", keys=("Label",))
    judgment = verdicts.judge_launchd_agent(plist, None, True, True)
    assert judgment.verdict == "keep"
    assert judgment.rule == "user-agent/disabled-on-purpose"


def test_loaded_label_without_a_plist_is_an_orphan():
    judgment = verdicts.judge_orphan_label(
        probes.LaunchctlEntry("com.ollama.ollama", pid=None, last_exit=0)
    )
    assert (judgment.status, judgment.verdict) == ("orphan", "investigate")


def test_daemon_that_declares_keepalive_but_is_absent_from_ps_is_dead(tmp_path):
    plist = probes.ParsedPlist(
        path=tmp_path / "d.plist",
        label="com.x.d",
        target="/usr/bin/true",
        triggers=("RunAtLoad", "KeepAlive"),
        keys=("Label",),
    )
    judgment = verdicts.judge_global_daemon(plist, True, None)
    assert (judgment.status, judgment.verdict) == ("dead", "investigate")


def test_on_demand_daemon_says_load_state_is_unverified(tmp_path):
    plist = probes.ParsedPlist(
        path=tmp_path / "d.plist",
        label="com.x.d",
        target="/usr/bin/true",
        triggers=("on-demand",),
        keys=("Label",),
    )
    judgment = verdicts.judge_global_daemon(plist, True, None)
    assert judgment.verdict == "keep"
    assert "sudo" in judgment.reason


def test_stale_unmanaged_listener_is_a_close_candidate_but_a_managed_one_is_not():
    listener = probes.Listener(pid=42, command="devserver", address="127.0.0.1", port=8080)
    old = _proc(42, "/usr/bin/devserver", etime_seconds=30 * 86400, cpu_percent=0.0)
    unmanaged = verdicts.judge_listener(listener, old, launchd_backed=False)
    assert (unmanaged.status, unmanaged.verdict) == ("stale", "close")
    assert unmanaged.close_command == "kill 42"
    managed = verdicts.judge_listener(listener, old, launchd_backed=True)
    assert managed.verdict == "keep"


def test_listener_bound_to_all_interfaces_is_investigated():
    listener = probes.Listener(pid=7, command="svc", address="*", port=9000)
    judgment = verdicts.judge_listener(listener, _proc(7, "/usr/bin/svc"), False)
    assert judgment.verdict == "investigate"
    assert "beyond loopback" in judgment.reason


def test_listener_whose_pid_is_gone_is_unknown_not_assumed_healthy():
    listener = probes.Listener(pid=999, command="ghost", address="127.0.0.1", port=1)
    judgment = verdicts.judge_listener(listener, None, False)
    assert (judgment.status, judgment.verdict) == ("unknown", "investigate")


def test_registry_row_holding_a_dead_pid_is_flagged():
    assert verdicts.judge_registry_process(1234, alive=False).verdict == "investigate"
    assert verdicts.judge_registry_process(1234, alive=True).verdict == "keep"


# --------------------------------------------------------------------------
# domains
# --------------------------------------------------------------------------


def test_user_agent_domain_keeps_an_unparseable_plist_as_an_item(tmp_path):
    broken = probes.ParsedPlist(
        path=tmp_path / "com.broken.plist", label="com.broken", parse_error="boom"
    )
    domain = domains.build_user_launch_agents(_snapshot(user_agents=[broken]))
    assert [item.label for item in domain.items] == ["com.broken"]
    assert domain.items[0].status == "unknown"
    assert domain.totals["unparseable_plists"] == 1


def test_orphan_check_ignores_synthetic_application_labels():
    snapshot = _snapshot(
        launchctl={
            "application.com.google.Chrome.1.2": probes.LaunchctlEntry(
                "application.com.google.Chrome.1.2", 732, 0
            ),
            "com.ollama.ollama": probes.LaunchctlEntry("com.ollama.ollama", None, 0),
        }
    )
    domain = domains.build_user_launch_agents(snapshot)
    assert [item.label for item in domain.items] == ["com.ollama.ollama"]
    assert domain.totals["orphan_loaded_no_plist"] == 1


def test_library_launch_agents_use_launchctl_rather_than_ps_inference(tmp_path):
    plist = probes.ParsedPlist(
        path=probes.GLOBAL_LAUNCH_AGENTS / "com.logi.optionsplus.plist",
        label="com.logi.cp-dev-mgr",
        target="/usr/bin/true",
        triggers=("RunAtLoad", "KeepAlive"),
        keys=("Label",),
    )
    snapshot = _snapshot(
        global_daemons=[plist],
        launchctl={"com.logi.cp-dev-mgr": probes.LaunchctlEntry("com.logi.cp-dev-mgr", 1502, 0)},
    )
    domain = domains.build_global_daemons(snapshot)
    assert domain.items[0].verdict == "keep"
    assert domain.items[0].rule == "global-agent/running"
    assert domain.totals["library_launch_agents"] == 1


def test_empty_snapshot_produces_domains_with_probe_returned_nothing_summaries():
    built = domains.build_domains(_snapshot())
    assert [domain.domain_id for domain in built] == list(domains.DOMAIN_IDS)
    assert all(domain.items == [] for domain in built)
    assert any("probe" in domain.summary for domain in built)


def test_process_domain_clusters_and_flags_zombies():
    procs = [_proc(i, "/opt/homebrew/bin/git fsmonitor--daemon run") for i in range(1, 60)]
    procs.append(_proc(900, "/usr/bin/defunct", stat="Z+"))
    domain = domains.build_processes(_snapshot(processes=procs))
    cluster = next(i for i in domain.items if i.label == "git fsmonitor--daemon")
    assert cluster.verdict == "investigate"
    assert domain.totals["zombies"] == 1


# --------------------------------------------------------------------------
# receipt contract
# --------------------------------------------------------------------------


def test_valid_payload_passes_validation():
    assert receipt.validate(_minimal_payload()) == []


def test_zero_item_receipt_is_a_refusal():
    payload = _minimal_payload()
    payload["domains"][0]["items"] = []
    payload["totals"] = {k: 0 for k in payload["totals"]}
    errors = receipt.validate(payload)
    assert any("degenerate" in error for error in errors)


def test_validation_rejects_off_contract_enums_and_bad_totals():
    payload = _minimal_payload()
    payload["domains"][0]["items"][0]["status"] = "sortof-running"
    assert any("status" in error for error in receipt.validate(payload))

    payload = _minimal_payload()
    payload["totals"]["keep"] = 99
    assert any("totals.keep" in error for error in receipt.validate(payload))

    payload = _minimal_payload()
    payload["generated_at"] = "yesterday"
    assert any("generated_at" in error for error in receipt.validate(payload))

    assert receipt.validate(["not", "an", "object"])


def test_missing_required_key_is_reported_without_crashing():
    payload = _minimal_payload()
    del payload["drift"]
    assert receipt.validate(payload) == ["missing required key: drift"]


def test_write_receipt_lands_dated_file_and_latest_pointer(tmp_path):
    dated, latest = receipt.write_receipt(_minimal_payload(), tmp_path)
    assert dated.name == "census-20260727T120000Z.json"
    assert latest.name == receipt.LATEST_NAME
    assert json.loads(latest.read_text()) == json.loads(dated.read_text())
    assert not list(tmp_path.glob(".*tmp"))


def test_a_bad_payload_never_clobbers_a_good_latest_json(tmp_path):
    receipt.write_receipt(_minimal_payload(), tmp_path)
    good = (tmp_path / receipt.LATEST_NAME).read_text()

    bad = _minimal_payload()
    bad["domains"][0]["items"] = []
    bad["totals"] = {k: 0 for k in bad["totals"]}
    with pytest.raises(receipt.CensusRefusal):
        receipt.write_receipt(bad, tmp_path)

    assert (tmp_path / receipt.LATEST_NAME).read_text() == good


def test_load_latest_reports_absent_invalid_and_ok(tmp_path):
    assert receipt.load_latest(tmp_path) == (None, None, "absent")

    (tmp_path / receipt.LATEST_NAME).write_text("{not json")
    _, path, status = receipt.load_latest(tmp_path)
    assert path is not None and status.startswith("invalid")

    (tmp_path / receipt.LATEST_NAME).write_text(json.dumps({"schema_version": "x"}))
    _, _, status = receipt.load_latest(tmp_path)
    assert status.startswith("invalid")

    receipt.write_receipt(_minimal_payload(), tmp_path)
    payload, _, status = receipt.load_latest(tmp_path)
    assert status == "ok" and payload is not None


def test_drift_reports_new_disappeared_and_verdict_changes():
    prior = _minimal_payload()
    prior["domains"][0]["items"].append(
        {
            "label": "com.gone.job",
            "path": "p",
            "status": "dead",
            "evidence": "e",
            "verdict": "investigate",
            "reason": "r",
        }
    )
    current = _minimal_payload(verdict="investigate")
    current["domains"][0]["items"].append(
        {
            "label": "com.new.job",
            "path": "p",
            "status": "running",
            "evidence": "e",
            "verdict": "keep",
            "reason": "r",
        }
    )

    drift = receipt.compute_drift(current, prior, "/tmp/latest.json")
    assert [entry["label"] for entry in drift["new_items"]] == ["com.new.job"]
    assert [entry["label"] for entry in drift["disappeared"]] == ["com.gone.job"]
    assert drift["verdict_changes"][0] == {
        "key": "user-launch-agents::com.example.job",
        "domain": "user LaunchAgents (~/Library/LaunchAgents)",
        "label": "com.example.job",
        "from": "keep",
        "to": "investigate",
    }


def test_drift_ignores_the_volatile_process_domain():
    def with_process_item(label: str) -> dict:
        payload = _minimal_payload()
        payload["domains"].append(
            {
                "domain_id": "processes",
                "domain": "live process census",
                "summary": "s",
                "totals": {},
                "items": [
                    {
                        "label": label,
                        "path": "p",
                        "status": "running",
                        "evidence": "e",
                        "verdict": "keep",
                        "reason": "r",
                    }
                ],
            }
        )
        return payload

    drift = receipt.compute_drift(
        with_process_item("node b.js"), with_process_item("node a.js"), "/tmp/latest.json"
    )
    assert drift["new_items"] == [] and drift["disappeared"] == []
    assert drift["excluded_domains"] == ["processes"]


def test_drift_against_no_prior_receipt_is_empty_not_fabricated():
    drift = receipt.compute_drift(_minimal_payload(), None, None, "absent")
    assert drift["prior_receipt"] is None
    assert drift["prior_status"] == "absent"
    assert drift["new_items"] == []


# --------------------------------------------------------------------------
# orchestration
# --------------------------------------------------------------------------


def test_run_census_on_an_empty_snapshot_refuses_and_writes_nothing(tmp_path):
    result = census.run_census(receipt_dir=tmp_path, snapshot=_snapshot())
    assert result.ok is False
    assert any("degenerate" in error for error in result.refusal)
    assert list(tmp_path.glob("*.json")) == []


def test_run_census_builds_a_valid_receipt_from_a_synthetic_machine(tmp_path):
    plist = probes.ParsedPlist(
        path=Path.home() / "Library/LaunchAgents/com.x.job.plist",
        label="com.x.job",
        target="/usr/bin/true",
        triggers=("RunAtLoad",),
        keys=("Label", "Program"),
    )
    snapshot = _snapshot(
        user_agents=[plist],
        launchctl={"com.x.job": probes.LaunchctlEntry("com.x.job", 4242, 0)},
        processes=[_proc(4242, "/usr/bin/true")],
    )
    result = census.run_census(receipt_dir=tmp_path, snapshot=snapshot)
    assert result.ok, result.refusal
    assert result.item_count >= 2
    assert receipt.validate(json.loads(result.latest_path.read_text())) == []
    assert result.payload["totals"]["items"] == result.item_count


def test_run_census_with_write_disabled_leaves_the_directory_empty(tmp_path):
    snapshot = _snapshot(processes=[_proc(1, "/sbin/launchd")])
    result = census.run_census(receipt_dir=tmp_path, snapshot=snapshot, write=False)
    assert result.ok and result.dated_path is None
    assert not tmp_path.exists() or list(tmp_path.iterdir()) == []


def test_staged_launchd_plist_is_valid_and_installs_nothing():
    text = census.render_launchd_plist("/usr/local/bin/fleet")
    parsed = plistlib.loads(text.encode())
    assert parsed["Label"] == census.LAUNCHD_LABEL
    assert parsed["ProgramArguments"][:2] == ["/usr/local/bin/fleet", "census"]
    assert parsed["RunAtLoad"] is False
    assert parsed["StartCalendarInterval"] == {"Hour": 9, "Minute": 0}
    # An XML comment body may not contain "--", so the install line lives on
    # stderr and in the contract doc; a plist launchd cannot parse is useless.
    comment_body = text.split("<!--", 1)[1].split("-->", 1)[0]
    assert "--" not in comment_body


def test_staged_plist_file_matches_the_renderer():
    """The in-repo staged file and the emitter must not drift apart."""
    staged = Path(__file__).resolve().parent.parent / (
        "contrib/launchd/io.fleet-watch.census.plist"
    )
    assert staged.exists(), f"staged launchd plist missing at {staged}"
    assert staged.read_text() == census.render_launchd_plist()


def test_domain_ids_are_stable_and_match_the_builders():
    assert domains.DOMAIN_IDS == (
        "user-launch-agents",
        "global-daemons",
        "processes",
        "network-listeners",
        "cron-login-items",
        "fleet-layer",
    )
    assert len(domains.DOMAIN_BUILDERS) == len(domains.DOMAIN_IDS)
