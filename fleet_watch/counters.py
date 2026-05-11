"""Audit-floor cycle counters for new Fleet Watch gates.

Each new gate enforces only after 10 audit cycles.
Counters are persisted in state.json.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from fleet_watch.registry import FLEET_DIR

DEFAULT_AUDIT_FLOOR = 10

COUNTER_KEYS = [
    "ollama_runner_discovery_cycles",
    "memory_pressure_gate_cycles",
    "orphan_detector_cycles",
    "pkill_cascade_cycles",
]


@dataclass
class GateCounters:
    ollama_runner_discovery: int = 0
    memory_pressure_gate: int = 0
    orphan_detector: int = 0
    pkill_cascade: int = 0

    def is_enforcing(self, gate: str, floor: int = DEFAULT_AUDIT_FLOOR) -> bool:
        current = getattr(self, gate, 0)
        return current >= floor

    def increment(self, gate: str) -> None:
        current = getattr(self, gate, 0)
        setattr(self, gate, current + 1)

    def to_dict(self) -> dict[str, int]:
        return {
            "ollama_runner_discovery_cycles": self.ollama_runner_discovery,
            "memory_pressure_gate_cycles": self.memory_pressure_gate,
            "orphan_detector_cycles": self.orphan_detector,
            "pkill_cascade_cycles": self.pkill_cascade,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> GateCounters:
        return cls(
            ollama_runner_discovery=d.get("ollama_runner_discovery_cycles", 0),
            memory_pressure_gate=d.get("memory_pressure_gate_cycles", 0),
            orphan_detector=d.get("orphan_detector_cycles", 0),
            pkill_cascade=d.get("pkill_cascade_cycles", 0),
        )


def load_counters() -> GateCounters:
    """Load gate counters from state.json. Returns zeros on failure."""
    state_path = FLEET_DIR / "state.json"
    if not state_path.exists():
        return GateCounters()
    try:
        data = json.loads(state_path.read_text())
    except (OSError, json.JSONDecodeError, ValueError):
        return GateCounters()
    if not isinstance(data, dict):
        return GateCounters()
    return GateCounters.from_dict(data.get("gate_counters", {}))


def save_counters(counters: GateCounters) -> None:
    """Merge counters into the existing state.json."""
    state_path = FLEET_DIR / "state.json"
    existing: dict[str, Any] = {}
    if state_path.exists():
        try:
            existing = json.loads(state_path.read_text())
        except (OSError, json.JSONDecodeError, ValueError):
            existing = {}
    if not isinstance(existing, dict):
        existing = {}
    existing["gate_counters"] = counters.to_dict()
    FLEET_DIR.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(existing, indent=2, default=str) + "\n")
