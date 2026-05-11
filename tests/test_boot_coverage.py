"""Tests for fleet_watch.boot_coverage — service persistence audit."""

import json
import plistlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import sys
sys.path.insert(0, str(Path.home() / "Workspace/active/fleet-watch"))
from fleet_watch import boot_coverage


class TestBootCoverage(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.orig_la_dir = boot_coverage.LAUNCH_AGENTS_DIR
        self.orig_receipt_dir = boot_coverage.RECEIPT_DIR

    def tearDown(self):
        boot_coverage.LAUNCH_AGENTS_DIR = self.orig_la_dir
        boot_coverage.RECEIPT_DIR = self.orig_receipt_dir

    def _mock_receipt_dir(self) -> Path:
        d = Path(self.tmp) / "receipts"
        d.mkdir()
        boot_coverage.RECEIPT_DIR = d
        return d

    def _mock_launch_agents_dir(self, plists: dict[str, dict] | None = None) -> Path:
        d = Path(self.tmp) / "LaunchAgents"
        d.mkdir()
        boot_coverage.LAUNCH_AGENTS_DIR = d
        if plists:
            for label, data in plists.items():
                path = d / f"{label}.plist"
                with open(path, "wb") as f:
                    plistlib.dump(data, f)
        return d

    def test_assess_gives_persistence_when_loaded(self):
        self._mock_receipt_dir()
        self._mock_launch_agents_dir({
            "com.cds.ollama-proxy": {
                "Label": "com.cds.ollama-proxy",
                "ProgramArguments": ["/usr/local/bin/ollama", "serve"],
            },
        })

        with mock.patch.object(boot_coverage, "list_launchd_agents", return_value={
            "com.cds.ollama-proxy": {
                "plist_path": "/tmp/LaunchAgents/com.cds.ollama-proxy.plist",
                "loaded": True,
                "pid": 1234,
            },
        }):
            results = boot_coverage.assess([
                {"pid": 1234, "name": "Ollama", "port": 11434},
            ])

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["verdict"], "HAS_PERSISTENCE")

    def test_assess_no_persistence_when_no_match(self):
        self._mock_receipt_dir()
        self._mock_launch_agents_dir({})

        with mock.patch.object(boot_coverage, "list_launchd_agents", return_value={}):
            results = boot_coverage.assess([
                {"pid": 5678, "name": "Mystery Process", "port": 9999},
            ])

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["verdict"], "NO_PERSISTENCE_WILL_DIE_ON_REBOOT")
        self.assertIsNotNone(results[0]["suggested_plist"])
        self.assertIsNotNone(results[0]["suggested_label"])

    def test_assess_unloaded_plist(self):
        self._mock_receipt_dir()
        self._mock_launch_agents_dir({
            "com.cds.archivist-http": {
                "Label": "com.cds.archivist-http",
                "ProgramArguments": ["/usr/bin/python3", "-m", "archivist"],
            },
        })

        with mock.patch.object(boot_coverage, "list_launchd_agents", return_value={
            "com.cds.archivist-http": {
                "plist_path": "/tmp/LaunchAgents/com.cds.archivist-http.plist",
                "loaded": False,
                "pid": None,
            },
        }):
            results = boot_coverage.assess([
                {"pid": 90965, "name": "Archivist HTTP", "port": 4242},
            ])

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["verdict"], "PLIST_PRESENT_BUT_UNLOADED")

    def test_empty_processes(self):
        self._mock_receipt_dir()
        results = boot_coverage.assess([])
        self.assertEqual(len(results), 0)

    def test_plist_template_emits_valid_plist_xml(self):
        template = boot_coverage._plist_template(
            "Test Service", "/usr/bin/test", "com.cds.test-service"
        )
        self.assertIn("com.cds.test-service", template)
        self.assertIn("/usr/bin/test", template)

    def test_run_creates_receipt(self):
        self._mock_receipt_dir()
        with mock.patch.object(boot_coverage, "assess", return_value=[
            {"pid": 1, "name": "Test", "port": None, "verdict": "HAS_PERSISTENCE",
             "launchd_label": "test", "plist_path": "/tmp/test.plist",
             "loaded": True, "suggested_plist": None, "suggested_label": None},
            {"pid": 2, "name": "Test2", "port": None, "verdict": "NO_PERSISTENCE_WILL_DIE_ON_REBOOT",
             "launchd_label": None, "plist_path": None,
             "loaded": False, "suggested_plist": "<plist/>", "suggested_label": "com.cds.test2"},
        ]):
            payload = boot_coverage.run(
                [{"pid": 1, "name": "Test"}, {"pid": 2, "name": "Test2"}]
            )

        self.assertEqual(payload["processes_assessed"], 2)
        self.assertIn("receipt_path", payload)
        self.assertTrue(Path(payload["receipt_path"]).exists())
        by_verdict = payload["by_verdict"]
        self.assertEqual(by_verdict.get("HAS_PERSISTENCE"), 1)
        self.assertEqual(by_verdict.get("NO_PERSISTENCE_WILL_DIE_ON_REBOOT"), 1)

    def test_list_launchd_agents_returns_dict(self):
        # Integration test: hits real launchctl
        agents = boot_coverage.list_launchd_agents()
        self.assertIsInstance(agents, dict)


if __name__ == "__main__":
    unittest.main()
