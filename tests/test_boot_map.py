"""Tests for fleet_watch.boot_map — census receipt -> graph -> local 3D render.

Covers the three things that make this artifact trustworthy:
  1. the transform is correct on a known census (exact node/edge counts),
  2. it REFUSES loudly on absent / invalid / degenerate input,
  3. it is deterministic and offline (same receipt -> same bytes, zero URLs).
"""

import json
import re
import shutil
import tempfile
import unittest
from pathlib import Path

import sys

# Resolve the repo from THIS file, never from $HOME — a worktree must test its
# own tree, not whatever is checked out at ~/Workspace/active/fleet-watch.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from click.testing import CliRunner  # noqa: E402

from fleet_watch import boot_map, boot_map_view  # noqa: E402
from fleet_watch.cli import cli  # noqa: E402

FIXTURE = REPO_ROOT / "tests" / "fixtures" / "census-sample.json"

# Pinned from the committed fixture. A drift here means the transform changed
# behaviour — that is the point of the pin.
EXPECTED_NODES = 39
EXPECTED_EDGES = 48
EXPECTED_KINDS = {
    "area": 4, "domain": 6, "host": 1, "job": 6, "listener": 2,
    "port": 2, "process": 4, "repo": 4, "target": 10,
}
EXPECTED_RELATIONS = {
    "contains": 18, "launches": 7, "lives_in": 16,
    "listens_on": 2, "managed_by": 1, "references": 4,
}


def build_fixture_graph() -> dict:
    census = boot_map.load_census(FIXTURE)
    return boot_map.build_graph(census, source_path=str(FIXTURE), source_sha256="fixture")


def node_by_label(graph: dict, label: str) -> dict:
    for node in graph["nodes"]:
        if node["label"] == label:
            return node
    raise AssertionError(f"no node labelled {label!r}")


class TestTransform(unittest.TestCase):
    """The graph is a faithful, complete projection of the census."""

    @classmethod
    def setUpClass(cls):
        cls.graph = build_fixture_graph()

    def test_fixture_is_present_and_non_degenerate(self):
        self.assertTrue(FIXTURE.is_file(), f"missing fixture {FIXTURE}")
        self.assertEqual(self.graph["census"]["item_count"], 12)
        self.assertEqual(self.graph["census"]["domain_count"], 6)

    def test_node_and_edge_counts(self):
        self.assertEqual(self.graph["stats"]["node_count"], EXPECTED_NODES)
        self.assertEqual(self.graph["stats"]["edge_count"], EXPECTED_EDGES)
        self.assertEqual(len(self.graph["nodes"]), EXPECTED_NODES)
        self.assertEqual(len(self.graph["links"]), EXPECTED_EDGES)

    def test_counts_by_kind(self):
        self.assertEqual(self.graph["stats"]["counts_by_kind"], EXPECTED_KINDS)

    def test_counts_by_relation(self):
        self.assertEqual(self.graph["stats"]["counts_by_relation"], EXPECTED_RELATIONS)

    def test_every_census_item_became_exactly_one_node(self):
        census_nodes = [n for n in self.graph["nodes"] if n.get("verdict_source") == "census"]
        self.assertEqual(len(census_nodes), 12)

    def test_status_totals_match_the_receipt(self):
        # No row invented, none dropped: statuses must sum to the item count.
        self.assertEqual(sum(self.graph["stats"]["counts_by_status"].values()), 12)

    def test_job_carries_status_and_verdict_from_receipt(self):
        job = node_by_label(self.graph, "com.demo.gateway")
        self.assertEqual(job["kind"], "job")
        self.assertEqual(job["status"], "running")
        self.assertEqual(job["verdict"], "keep")
        self.assertEqual(job["verdict_source"], "census")

    def test_job_launches_its_target_mined_from_evidence(self):
        job = node_by_label(self.graph, "com.demo.gateway")
        target = node_by_label(self.graph, "gateway.py")
        self.assertEqual(target["path"], "~/Workspace/active/demo-repo/bin/gateway.py")
        self.assertIn(
            (job["id"], target["id"], "launches"),
            [(link["source"], link["target"], link["relation"]) for link in self.graph["links"]],
        )

    def test_data_file_is_a_reference_not_a_launch(self):
        rels = {
            link["relation"]
            for link in self.graph["links"]
            if link["target"] == node_by_label(self.graph, "gateway.err")["id"]
        }
        self.assertEqual(rels, {"references"})

    def test_target_lives_in_its_repo(self):
        target = node_by_label(self.graph, "gateway.py")
        repo = node_by_label(self.graph, "~/Workspace/active/demo-repo")
        self.assertEqual(repo["kind"], "repo")
        self.assertIn(
            (target["id"], repo["id"], "lives_in"),
            [(link["source"], link["target"], link["relation"]) for link in self.graph["links"]],
        )

    def test_home_library_is_not_mis_rooted_into_system_library(self):
        # Regression: `~/Library/x` used to match from the inner slash and land
        # in the SYSTEM /Library area, hiding user agents among vendor daemons.
        self.assertEqual(node_by_label(self.graph, "~/Library")["kind"], "repo")
        self.assertEqual(node_by_label(self.graph, "/Library")["kind"], "area")
        self.assertEqual(node_by_label(self.graph, "/opt/homebrew")["kind"], "area")

    def test_own_plist_is_a_manifest_not_a_target(self):
        labels = {n["label"] for n in self.graph["nodes"]}
        self.assertNotIn("com.demo.gateway.plist", labels)

    def test_ports_carry_exposure(self):
        loopback = node_by_label(self.graph, "127.0.0.1:8123")
        exposed = node_by_label(self.graph, "0.0.0.0:8080")
        self.assertEqual(loopback["exposure"], "loopback")
        self.assertEqual(loopback["port"], 8123)
        self.assertEqual(exposed["exposure"], "all-interfaces")

    def test_process_is_linked_back_to_the_job_that_owns_it(self):
        process = node_by_label(self.graph, "demo relay process")
        job = node_by_label(self.graph, "com.demo.gateway")
        self.assertEqual(process["kind"], "process")
        self.assertIn(
            (process["id"], job["id"], "managed_by"),
            [(link["source"], link["target"], link["relation"]) for link in self.graph["links"]],
        )

    def test_derived_verdict_is_worst_of_what_reaches_the_node(self):
        # ~/.local holds only the removed watchdog target -> reads remove.
        self.assertEqual(node_by_label(self.graph, "~/.local")["verdict"], "remove")
        self.assertEqual(node_by_label(self.graph, "~/.local")["verdict_source"], "derived")
        # The user-agents domain holds one removed job -> the domain reads remove.
        domain = node_by_label(self.graph, "user LaunchAgents (~/Library/LaunchAgents)")
        self.assertEqual(domain["verdict"], "remove")

    def test_no_node_is_left_unrated(self):
        unrated = [n for n in self.graph["nodes"] if n.get("verdict") in (None, "unknown")]
        self.assertEqual(unrated, [], "every node must carry a verdict for colouring")

    def test_host_stays_neutral(self):
        host = [n for n in self.graph["nodes"] if n["kind"] == "host"][0]
        self.assertEqual(host["label"], "demo-host")
        self.assertEqual(host["verdict"], "keep")

    def test_missing_status_and_verdict_are_kept_and_flagged(self):
        node = node_by_label(self.graph, "com.demo.unrated")
        self.assertEqual(node["status"], "unknown")
        # Fail-closed direction: unrated means "a human must look", never "keep".
        self.assertEqual(node["verdict"], "investigate")
        codes = {w["code"] for w in self.graph["warnings"]}
        self.assertIn("missing_status", codes)
        self.assertIn("missing_verdict", codes)

    def test_off_enum_values_are_preserved_verbatim_and_flagged(self):
        node = node_by_label(self.graph, "demo.odd.entry")
        self.assertEqual(node["status"], "zombie")
        self.assertEqual(node["verdict"], "maybe")
        self.assertIs(node["status_valid"], False)
        self.assertIs(node["verdict_valid"], False)
        codes = {w["code"] for w in self.graph["warnings"]}
        self.assertIn("off_enum_status", codes)
        self.assertIn("off_enum_verdict", codes)

    def test_every_kind_and_relation_emitted_is_declared(self):
        # The renderer styles by kind and relation; an undeclared one would draw
        # as an unlabelled blob, so the declaration lists are enforced, not
        # decorative. Both directions: real output conforms, and a violation raises.
        for node in self.graph["nodes"]:
            self.assertIn(node["kind"], boot_map.NODE_KINDS)
        for link in self.graph["links"]:
            self.assertIn(link["relation"], boot_map.RELATIONS)

        graph = boot_map._Graph()
        with self.assertRaises(boot_map.BootMapError):
            graph.node("sprocket", "k", "l")
        a = graph.node("job", "a", "a")
        b = graph.node("job", "b", "b")
        with self.assertRaises(boot_map.BootMapError):
            graph.edge(a, b, "haunts", "EXTRACTED", "x")

    def test_links_carry_provenance(self):
        for link in self.graph["links"]:
            self.assertIn(link["confidence"], ("EXTRACTED", "INFERRED"))
            self.assertTrue(link["source_ref"], "every edge names the census row behind it")
            self.assertGreater(link["weight"], 0)

    def test_nested_job_labels_do_not_cross_link(self):
        # Launchd labels nest by design. A process owned by `...-graph` must not
        # also be handed to the shorter parent it merely starts with.
        graph = boot_map.build_graph(
            boot_map.parse_census(
                [
                    {"domain": "user LaunchAgents", "items": [
                        {"label": "com.demo.gateway", "path": "~/Library/LaunchAgents/com.demo.gateway.plist",
                         "status": "running", "verdict": "keep"},
                        {"label": "com.demo.gateway-graph", "path": "~/Library/LaunchAgents/com.demo.gateway-graph.plist",
                         "status": "running", "verdict": "keep"},
                    ]},
                    {"domain": "live process census", "items": [
                        {"label": "graph worker", "resource": "PID 77",
                         "status": "running", "verdict": "keep",
                         "evidence": "launchd label com.demo.gateway-graph owns it."},
                    ]},
                ]
            )
        )
        owners = {
            node_by_label(graph, "graph worker")["id"]: [],
        }
        for link in graph["links"]:
            if link["relation"] == "managed_by" and link["source"] in owners:
                owners[link["source"]].append(link["target"])
        worker = node_by_label(graph, "graph worker")["id"]
        self.assertEqual(
            owners[worker], [node_by_label(graph, "com.demo.gateway-graph")["id"]]
        )

    def test_a_plist_suffix_still_resolves_to_its_own_job(self):
        # `<label>.plist` names the same job; only sibling suffixes are foreign.
        graph = boot_map.build_graph(
            boot_map.parse_census(
                [
                    {"domain": "user LaunchAgents", "items": [
                        {"label": "com.demo.gateway", "path": "~/Library/LaunchAgents/com.demo.gateway.plist",
                         "status": "running", "verdict": "keep"},
                    ]},
                    {"domain": "live process census", "items": [
                        {"label": "worker", "resource": "PID 78", "status": "running", "verdict": "keep",
                         "evidence": "loaded from com.demo.gateway.plist"},
                    ]},
                ]
            )
        )
        self.assertEqual(graph["stats"]["counts_by_relation"].get("managed_by"), 1)

    def test_one_row_cannot_dominate_the_map(self):
        # A single verbose or hostile row must not blow the graph up. The clamp
        # is counted, not silent — bounded output still has to testify.
        flood = " ".join(f"/usr/bin/t{n}" for n in range(5000))
        graph = boot_map.build_graph(
            boot_map.parse_census(
                [{"domain": "d", "items": [
                    {"label": "flood", "status": "running", "verdict": "keep", "resource": flood}
                ]}]
            )
        )
        targets = [n for n in graph["nodes"] if n["kind"] == "target"]
        self.assertEqual(len(targets), boot_map.MAX_DECLARED_PATHS_PER_ITEM)
        self.assertEqual(graph["stats"]["extraction"]["declared_paths_capped"], 1)

    def test_endpoint_flood_is_also_clamped(self):
        flood = " ".join(f"127.0.0.1:{9000 + n}" for n in range(400))
        graph = boot_map.build_graph(
            boot_map.parse_census(
                [{"domain": "d", "items": [
                    {"label": "ports", "status": "running", "verdict": "keep", "resource": flood}
                ]}]
            )
        )
        ports = [n for n in graph["nodes"] if n["kind"] == "port"]
        self.assertEqual(len(ports), boot_map.MAX_DECLARED_ENDPOINTS_PER_ITEM)
        self.assertEqual(graph["stats"]["extraction"]["declared_endpoints_capped"], 1)

    def test_real_world_rows_are_nowhere_near_the_cap(self):
        # A clamp that fires on ordinary input is a truncation bug, not a bound.
        self.assertEqual(self.graph["stats"]["extraction"]["declared_paths_capped"], 0)
        self.assertEqual(self.graph["stats"]["extraction"]["declared_endpoints_capped"], 0)

    def test_no_dangling_edge_endpoints(self):
        ids = {n["id"] for n in self.graph["nodes"]}
        for link in self.graph["links"]:
            self.assertIn(link["source"], ids)
            self.assertIn(link["target"], ids)


class TestRefusals(unittest.TestCase):
    """Bad input fails closed. An empty map is never a valid render."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, payload: str) -> Path:
        path = self.tmp / "census.json"
        path.write_text(payload, encoding="utf-8")
        return path

    def test_missing_file(self):
        with self.assertRaises(boot_map.BootMapError) as ctx:
            boot_map.load_census(self.tmp / "nope.json")
        self.assertIn("NO CENSUS RECEIPT", str(ctx.exception))

    def test_explicit_missing_receipt_refuses(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.resolve_receipt_path(self.tmp / "nope.json")

    def test_empty_file(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.load_census(self._write("   "))

    def test_invalid_json(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.load_census(self._write("{not json,"))

    def test_wrong_root_type(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.parse_census("a census, honest")
        with self.assertRaises(boot_map.BootMapError):
            boot_map.parse_census(42)

    def test_object_without_domains(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.parse_census({"schema_version": "fleet-census/v1", "totals": {}})

    def test_degenerate_zero_domains(self):
        with self.assertRaises(boot_map.BootMapError) as ctx:
            boot_map.parse_census([])
        self.assertIn("degenerate", str(ctx.exception))

    def test_degenerate_zero_items(self):
        # Six pretty domains and nothing in them is a failed census, not a clean box.
        with self.assertRaises(boot_map.BootMapError) as ctx:
            boot_map.parse_census([{"domain": "empty", "items": []}])
        self.assertIn("degenerate", str(ctx.exception))

    def test_item_is_not_an_object(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.parse_census([{"domain": "d", "items": ["com.demo.job"]}])

    def test_item_without_label(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.parse_census([{"domain": "d", "items": [{"status": "running"}]}])

    def test_domain_without_name(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.parse_census([{"items": [{"label": "x"}]}])

    def test_items_wrong_type(self):
        with self.assertRaises(boot_map.BootMapError):
            boot_map.parse_census([{"domain": "d", "items": {"label": "x"}}])

    def test_bare_domains_array_shape_is_accepted(self):
        raw = json.loads(FIXTURE.read_text(encoding="utf-8"))
        census = boot_map.parse_census(raw["domains"])
        self.assertEqual(census.shape, "domains-array")
        self.assertEqual(len(census.items), 12)
        self.assertEqual(census.host, "")


class TestArtifacts(unittest.TestCase):
    """The emitted page is local, sovereign, deterministic, and non-empty."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_build_writes_all_three_artifacts(self):
        receipt = boot_map.build(receipt=FIXTURE, out_dir=self.tmp / "out")
        out = self.tmp / "out"
        self.assertTrue((out / "graph.json").is_file())
        self.assertTrue((out / "index.html").is_file())
        self.assertTrue((out / "receipt.json").is_file())
        self.assertEqual(receipt["decision"], "PASS")
        self.assertEqual(receipt["stats"]["node_count"], EXPECTED_NODES)
        self.assertEqual(receipt["source_receipt"]["item_count"], 12)
        self.assertEqual(len(receipt["outputs"]), 2)

    def test_determinism_same_receipt_same_bytes(self):
        first = boot_map.build(receipt=FIXTURE, out_dir=self.tmp / "a")
        second = boot_map.build(receipt=FIXTURE, out_dir=self.tmp / "b")
        self.assertEqual(
            [o["sha256"] for o in first["outputs"]],
            [o["sha256"] for o in second["outputs"]],
        )
        self.assertEqual(
            (self.tmp / "a" / "index.html").read_bytes(),
            (self.tmp / "b" / "index.html").read_bytes(),
        )
        self.assertEqual(
            (self.tmp / "a" / "graph.json").read_bytes(),
            (self.tmp / "b" / "graph.json").read_bytes(),
        )

    def test_html_has_zero_external_urls(self):
        boot_map.build(receipt=FIXTURE, out_dir=self.tmp / "out")
        html = (self.tmp / "out" / "index.html").read_text(encoding="utf-8")
        hits = re.findall(r"https?://[^\s\"'<>]*", html)
        self.assertEqual(hits, [], f"self-contained render must fetch nothing: {hits}")

    def test_html_declares_a_no_network_policy(self):
        html = boot_map_view.render_html(build_fixture_graph())
        self.assertIn("Content-Security-Policy", html)
        self.assertIn("default-src 'none'", html)
        self.assertIn("connect-src 'none'", html)

    def test_html_embeds_every_node(self):
        graph = build_fixture_graph()
        html = boot_map_view.render_html(graph)
        start = html.index('<script id="boot-map-data"')
        island = html[html.index(">", start) + 1: html.index("</script>", start)]
        data = json.loads(island.replace("<\\/", "</"))
        self.assertEqual(len(data["nodes"]), EXPECTED_NODES)
        self.assertEqual(len(data["links"]), EXPECTED_EDGES)
        self.assertIn("com.demo.gateway", [n["label"] for n in data["nodes"]])

    def test_html_carries_a_refusal_path(self):
        # The page must be able to say "no" — an empty data island renders a
        # REFUSAL panel, not a clean-looking empty canvas.
        html = boot_map_view.render_html(build_fixture_graph())
        self.assertIn("REFUSAL: ", html)
        self.assertIn("EMPTY GRAPH", html)
        self.assertIn("An empty map is not a clean machine.", html)

    def test_render_rejects_a_non_graph(self):
        with self.assertRaises(ValueError):
            boot_map_view.render_html({"not": "a graph"})

    def test_data_island_cannot_break_out_of_the_script_tag(self):
        graph = build_fixture_graph()
        graph["nodes"][0] = dict(graph["nodes"][0], label="</script><script>alert(1)</script>")
        html = boot_map_view.render_html(graph)
        start = html.index('<script id="boot-map-data"')
        end = html.index("</script>", start)
        island = html[html.index(">", start) + 1: end]
        self.assertNotIn("</script>", island)
        self.assertIn("<\\/script>", island)


NODE = shutil.which("node")
PROBE = REPO_ROOT / "scripts" / "verify_boot_map_render.js"


@unittest.skipIf(NODE is None, "node not on PATH — JS render path unverifiable here")
class TestRenderPath(unittest.TestCase):
    """The page must actually paint, and must actually refuse.

    Structure is not behaviour: a page can parse clean and paint nothing. The
    probe executes the emitted JS against a DOM stub and counts draw calls.
    """

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _probe(self, html_path: Path, expect: str):
        import subprocess

        return subprocess.run(
            [NODE, str(PROBE), str(html_path), "--expect", expect],
            capture_output=True, text=True, timeout=180,
        )

    def test_a_real_graph_paints(self):
        boot_map.build(receipt=FIXTURE, out_dir=self.tmp)
        result = self._probe(self.tmp / "index.html", "render")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('"observed": "render"', result.stdout)

    def test_hover_and_detail_panel_actually_run(self):
        # Painting is only half the page. The picker must resolve a real node
        # and the detail panel must build content from its optional attributes.
        boot_map.build(receipt=FIXTURE, out_dir=self.tmp)
        result = self._probe(self.tmp / "index.html", "render")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        payload = json.loads(result.stdout[result.stdout.index("{"): result.stdout.rindex("}") + 1])
        self.assertGreater(payload["handlers_fired"], 0)
        self.assertGreater(payload["detail_panel_elements"], 0)

    def test_an_empty_graph_refuses_instead_of_painting_nothing(self):
        empty = {"schema_version": boot_map.SCHEMA_VERSION, "census": {}, "nodes": [],
                 "links": [], "stats": {"node_count": 0, "edge_count": 0}, "warnings": []}
        page = self.tmp / "empty.html"
        page.write_text(boot_map_view.render_html(empty), encoding="utf-8")
        result = self._probe(page, "refusal")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('"draw_calls"', result.stdout)
        self.assertIn('"fill": 0', result.stdout)

    def test_a_corrupted_data_island_refuses(self):
        boot_map.build(receipt=FIXTURE, out_dir=self.tmp)
        html = (self.tmp / "index.html").read_text(encoding="utf-8")
        start = html.index('<script id="boot-map-data"')
        body = html.index(">", start) + 1
        end = html.index("</script>", start)
        page = self.tmp / "corrupt.html"
        page.write_text(html[:body] + '{"nodes": [tru' + html[end:], encoding="utf-8")
        result = self._probe(page, "refusal")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_the_probe_itself_can_fail(self):
        # A verifier that only ever passes is not a verifier.
        boot_map.build(receipt=FIXTURE, out_dir=self.tmp)
        result = self._probe(self.tmp / "index.html", "refusal")
        self.assertEqual(result.returncode, 3)
        self.assertIn("BLOCKED", result.stderr)


class TestCli(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.runner = CliRunner()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_boot_map_builds_from_an_explicit_receipt(self):
        out = self.tmp / "out"
        result = self.runner.invoke(cli, ["boot-map", "--receipt", str(FIXTURE), "--out", str(out)])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertIn(f"{EXPECTED_NODES} nodes", result.output)
        self.assertTrue((out / "index.html").is_file())

    def test_boot_map_json_receipt(self):
        out = self.tmp / "out"
        result = self.runner.invoke(
            cli, ["boot-map", "--receipt", str(FIXTURE), "--out", str(out), "--json"]
        )
        self.assertEqual(result.exit_code, 0, result.output)
        payload = json.loads(result.output)
        self.assertEqual(payload["schema_version"], boot_map.RECEIPT_SCHEMA_VERSION)
        self.assertEqual(payload["stats"]["node_count"], EXPECTED_NODES)

    def test_boot_map_refuses_a_missing_receipt_with_exit_3(self):
        result = self.runner.invoke(
            cli,
            ["boot-map", "--receipt", str(self.tmp / "absent.json"), "--out", str(self.tmp / "out")],
        )
        self.assertEqual(result.exit_code, 3)
        self.assertIn("REFUSAL", result.output)
        self.assertIn("NO CENSUS RECEIPT", result.output)
        self.assertFalse((self.tmp / "out").exists(), "a refusal must not leave an artifact behind")

    def test_boot_map_refuses_a_degenerate_receipt_with_exit_3(self):
        bad = self.tmp / "empty.json"
        bad.write_text(json.dumps({"schema_version": "fleet-census/v1", "domains": []}), encoding="utf-8")
        result = self.runner.invoke(cli, ["boot-map", "--receipt", str(bad), "--out", str(self.tmp / "out")])
        self.assertEqual(result.exit_code, 3)
        self.assertIn("degenerate", result.output)


if __name__ == "__main__":
    unittest.main()
