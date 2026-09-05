"""Linux PSI classification and platform-specific probe dispatch."""

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


def test_linux_missing_psi(monkeypatch):
    def missing(path, **kwargs):
        assert str(path) == "/proc/pressure/memory"
        raise FileNotFoundError

    monkeypatch.setattr(syshealth.Path, "read_text", missing)
    probe = syshealth.get_vm_pressure_probe(system="Linux")
    assert probe.value is None
    assert "/proc/pressure/memory" in probe.failure_reason


@pytest.mark.parametrize("content", ["", "full avg10=0.00", "some avg10=nan",
                                     "some avg10=-1", "some avg10=101",
                                     "some avg10=oops"])
def test_linux_malformed_psi(tmp_path, content):
    path = tmp_path / "memory"
    path.write_text(content)
    probe = syshealth.get_vm_pressure_probe(system="Linux", psi_path=path)
    assert probe.value is None
    assert str(path) in probe.failure_reason


def test_macos_uses_sysctl_runner(monkeypatch):
    calls = []

    def run(command):
        calls.append(command)
        return syshealth.ProbeResult(2)

    monkeypatch.setattr(syshealth, "_run_numeric_probe", run)
    assert syshealth.get_vm_pressure_probe(system="Darwin").value == 2
    assert calls == [["sysctl", "-n", "kern.memorystatus_vm_pressure_level"]]
