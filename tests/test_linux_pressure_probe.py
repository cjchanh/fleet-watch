"""Linux PSI classification and platform-specific probe dispatch."""

from pathlib import Path

import pytest

from fleet_watch import syshealth


@pytest.mark.parametrize(
    ("avg10", "expected"),
    [("3.00", 1), ("25.00", 2), ("60.00", 4), ("10.00", 2), ("40.00", 4)],
)
def test_linux_psi_levels(tmp_path, avg10, expected):
    path = tmp_path / "memory"
    path.write_text(
        f"some avg10={avg10} avg60=1.00 avg300=0.50 total=12345\n"
        "full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n"
    )
    probe = syshealth.get_vm_pressure_probe(system="Linux", psi_path=path)
    assert probe.value == expected
    assert probe.failure_reason is None


def test_linux_missing_psi_falls_back_to_healthy_meminfo(tmp_path):
    """PSI FileNotFoundError is absent telemetry, not blind: meminfo may admit."""
    psi_path = tmp_path / "missing-psi"
    meminfo_path = tmp_path / "meminfo"
    meminfo_path.write_text(
        "MemTotal:       10000 kB\nMemAvailable:    6000 kB\n",
        encoding="utf-8",
    )
    probe = syshealth.get_vm_pressure_probe(
        system="Linux", psi_path=psi_path, meminfo_path=meminfo_path,
    )
    assert probe.value == syshealth.VM_PRESSURE_NORMAL
    assert probe.failure_reason is None


def test_linux_missing_psi_and_meminfo_is_unavailable(tmp_path):
    """Both files missing stays fail-closed; provenance is the meminfo path."""
    psi_path = tmp_path / "missing-psi"
    meminfo_path = tmp_path / "missing-meminfo"
    probe = syshealth.get_vm_pressure_probe(
        system="Linux", psi_path=psi_path, meminfo_path=meminfo_path,
    )
    assert probe.value is None
    assert probe.failure_reason is not None
    assert probe.failure_reason.endswith("FileNotFoundError")
    assert str(meminfo_path) in probe.failure_reason
    assert str(psi_path) not in probe.failure_reason


@pytest.mark.parametrize("content", ["", "full avg10=0.00", "some avg10=nan",
                                     "some avg10=-1", "some avg10=101",
                                     "some avg10=oops"])
def test_linux_malformed_psi(tmp_path, content):
    path = tmp_path / "memory"
    path.write_text(content)
    probe = syshealth.get_vm_pressure_probe(system="Linux", psi_path=path)
    assert probe.value is None
    assert str(path) in probe.failure_reason


def test_linux_psi_permission_error_does_not_read_meminfo(tmp_path, monkeypatch):
    """PermissionError is blind telemetry: do not fall back to meminfo."""
    psi_path = tmp_path / "psi"
    psi_path.write_text(
        "some avg10=0.00 avg60=1.00 avg300=0.50 total=12345\n",
        encoding="utf-8",
    )
    meminfo_path = tmp_path / "no-such-meminfo"
    real_read_text = Path.read_text
    reads: list[str] = []

    def tracked_read_text(self, *args, **kwargs):
        reads.append(str(self))
        if Path(self) == psi_path:
            raise PermissionError
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", tracked_read_text)
    probe = syshealth.get_vm_pressure_probe(
        system="Linux", psi_path=psi_path, meminfo_path=meminfo_path,
    )
    assert probe.value is None
    assert probe.failure_reason is not None
    assert probe.failure_reason.endswith("PermissionError")
    assert str(psi_path) in probe.failure_reason
    assert str(meminfo_path) not in reads


def test_macos_uses_sysctl_runner(monkeypatch):
    calls = []

    def run(command):
        calls.append(command)
        return syshealth.ProbeResult(2)

    monkeypatch.setattr(syshealth, "_run_numeric_probe", run)
    assert syshealth.get_vm_pressure_probe(system="Darwin").value == 2
    assert calls == [["sysctl", "-n", "kern.memorystatus_vm_pressure_level"]]
