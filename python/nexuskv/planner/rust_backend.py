from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from nexuskv.connectors.base import ReusePlanner
from nexuskv.contracts.generated import MatchResult, PartialHitPlan, QueryKey, ReuseKey, CacheEntry
from nexuskv.contracts.serde import from_primitive, to_primitive


ROOT = Path(__file__).resolve().parents[3]


def _load_native_module():
    for candidate in (
        ROOT / "rust" / "target" / "debug" / "libnexuskv_planner_native.so",
        ROOT / "rust" / "target" / "release" / "libnexuskv_planner_native.so",
    ):
        if candidate.exists():
            spec = importlib.util.spec_from_file_location("nexuskv_planner_native", candidate)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
    raise ModuleNotFoundError("nexuskv_planner_native extension module not found; build bindings-py first")


class RustPlanner(ReusePlanner):
    def __init__(self) -> None:
        native = _load_native_module()
        self._planner = native.PyRustPlanner()

    def insert(self, reuse_key: ReuseKey, entry: CacheEntry) -> None:
        self._planner.insert(json.dumps(to_primitive(reuse_key)), json.dumps(to_primitive(entry)))

    def lookup(self, query: QueryKey) -> MatchResult | None:
        payload = self._planner.lookup(json.dumps(to_primitive(query)))
        if payload is None:
            return None
        return from_primitive(MatchResult, json.loads(payload))

    def plan_partial_hit(self, query: QueryKey) -> PartialHitPlan | None:
        payload = self._planner.plan_partial_hit(json.dumps(to_primitive(query)))
        if payload is None:
            return None
        return from_primitive(PartialHitPlan, json.loads(payload))
