from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request
from copy import deepcopy
from pathlib import Path

from nexuskv.contracts.generated import (
    CacheEntry,
    CompatibilitySignal,
    MatchClassification,
    MatchExtent,
    MatchResult,
    QueryKey,
    RemainingWork,
    ReuseKey,
)
from nexuskv.contracts.serde import from_primitive
from nexuskv.execution.catalog import BackendCatalog
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.integrations.locus_bridge import (
    BRIDGE_SCHEMA,
    PROTOCOL_RECEIPT_NAMESPACE,
    BridgeError,
    LocusBridgeHttpServer,
    LocusBridgeService,
    RegisteredState,
    ValidationEvidence,
    load_fixture,
)

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "locus_bridge" / "conformance.json"


class InMemoryPlanner:
    def __init__(self) -> None:
        self._entries = []

    def insert(self, reuse_key, entry) -> None:
        self._entries.append((reuse_key, entry))

    def lookup(self, query: QueryKey) -> MatchResult | None:
        for reuse_key, entry in self._entries:
            identity = reuse_key.identity
            requested = query.identity
            if (
                identity.tenant != requested.tenant
                or identity.namespace != requested.namespace
                or identity.model != requested.model
                or identity.engine_family != requested.engine_family
                or identity.semantic_type != requested.semantic_type
                or identity.block_id != requested.block_id
                or identity.page_id != requested.page_id
                or requested.tokens[: len(identity.tokens)] != identity.tokens
            ):
                continue
            exact = len(identity.tokens) == len(requested.tokens)
            return MatchResult(
                classification=MatchClassification.EXACT if exact else MatchClassification.PARTIAL,
                matched_key=ReuseKey(identity=identity),
                requested_key=query,
                matched_extent=MatchExtent(
                    units=len(identity.tokens),
                    granularity=entry.descriptor.granularity,
                ),
                entry=entry,
                remaining=RemainingWork(
                    tokens=list(requested.tokens[len(identity.tokens) :]),
                    fetch_required=not exact,
                    recompute_required=not exact,
                ),
                compatibility=CompatibilitySignal(
                    reusable=True,
                    fallback_to_recompute=False,
                    reason="test planner match",
                ),
            )
        return None

    def plan_partial_hit(self, query: QueryKey):
        return None


def fixture_payload() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def configured_service() -> LocusBridgeService:
    service = LocusBridgeService(InMemoryPlanner())
    load_fixture(FIXTURE, service)
    return service


class LocusBridgeServiceTests(unittest.TestCase):
    def test_fixture_drives_lookup_estimate_and_protocol_materialization(self) -> None:
        payload = fixture_payload()
        service = configured_service()

        lookup = service.lookup(deepcopy(payload["requests"]["lookup"]))
        match = lookup["match_result"]
        self.assertEqual(lookup["schema_version"], BRIDGE_SCHEMA)
        self.assertEqual(match["entry"]["identity"]["entry_id"], "nexus-state-1")
        self.assertEqual(
            match["validation"]["model_identity"],
            payload["entries"][0]["validation"]["model_identity"],
        )
        self.assertEqual(
            match["validation"]["input_semantic_identity"],
            payload["entries"][0]["validation"]["input_semantic_identity"],
        )
        self.assertTrue(match["validation"]["source_handle"])

        estimate_request = deepcopy(payload["requests"]["estimate"])
        estimate_request["source_handle"] = match["validation"]["source_handle"]
        estimate = service.estimate(estimate_request)
        self.assertEqual(estimate["locality"], "local")
        self.assertGreaterEqual(estimate["estimated_transfer_micros"], 0)

        materialize_request = deepcopy(payload["requests"]["materialize"])
        materialize_request["option_id"] = estimate["option_id"]
        materialize_request["option_handle"] = estimate["option_handle"]
        materialized = service.materialize(materialize_request)
        self.assertEqual(materialized["bytes_transferred"], 0)
        self.assertEqual(materialized["receipt"]["namespace"], PROTOCOL_RECEIPT_NAMESPACE)
        self.assertEqual(materialized["evidence"]["level"], "protocol")
        self.assertFalse(materialized["evidence"]["physical_transfer_verified"])

        with self.assertRaisesRegex(BridgeError, "already been consumed"):
            service.materialize(materialize_request)

    def test_lookup_rejects_unregistered_semantic_identity(self) -> None:
        payload = fixture_payload()
        request = deepcopy(payload["requests"]["lookup"])
        request["locus_input_semantic_identity"]["tokenizer"]["fingerprint"] = "wrong"

        with self.assertRaises(BridgeError) as captured:
            configured_service().lookup(request)

        self.assertEqual(captured.exception.status.value, 409)
        self.assertEqual(captured.exception.code, "validation_evidence_mismatch")

    def test_same_entry_id_remains_isolated_by_full_tenant_identity(self) -> None:
        payload = fixture_payload()
        service = configured_service()
        raw = deepcopy(payload["entries"][0])
        raw["reuse_key"]["identity"]["tenant"] = "tenant-b"
        raw["entry"]["identity"]["key"]["tenant"] = "tenant-b"
        raw["entry"]["location"]["locator"] = "node://node-b/state/nexus-state-1"
        validation = raw["validation"]
        service.register(
            RegisteredState(
                reuse_key=from_primitive(ReuseKey, raw["reuse_key"]),
                entry=from_primitive(CacheEntry, raw["entry"]),
                validation=ValidationEvidence(
                    model_identity=dict(validation["model_identity"]),
                    input_semantic_identity=dict(validation["input_semantic_identity"]),
                    input_fingerprint=str(validation["input_fingerprint"]),
                ),
            )
        )

        lookup_a = service.lookup(deepcopy(payload["requests"]["lookup"]))
        lookup_b_request = deepcopy(payload["requests"]["lookup"])
        lookup_b_request["identity"]["tenant"] = "tenant-b"
        lookup_b = service.lookup(lookup_b_request)
        handle_a = lookup_a["match_result"]["validation"]["source_handle"]
        handle_b = lookup_b["match_result"]["validation"]["source_handle"]
        self.assertNotEqual(handle_a, handle_b)

        estimate_b = deepcopy(payload["requests"]["estimate"])
        estimate_b["source_handle"] = handle_b
        estimate_b["source_locator"] = "node://node-b/state/nexus-state-1"
        estimate_b["target_residency"] = "node-b"
        self.assertEqual(service.estimate(estimate_b)["locality"], "local")

        estimate_b["source_handle"] = handle_a
        with self.assertRaises(BridgeError) as captured:
            service.estimate(estimate_b)
        self.assertEqual(captured.exception.code, "source_location_mismatch")

    def test_estimate_and_materialize_reject_tampered_handles_and_targets(self) -> None:
        payload = fixture_payload()
        service = configured_service()
        lookup = service.lookup(deepcopy(payload["requests"]["lookup"]))
        estimate_request = deepcopy(payload["requests"]["estimate"])
        estimate_request["source_handle"] = lookup["match_result"]["validation"]["source_handle"]

        tampered_estimate = deepcopy(estimate_request)
        tampered_estimate["source_handle"] = "tampered"
        with self.assertRaises(BridgeError) as captured:
            service.estimate(tampered_estimate)
        self.assertEqual(captured.exception.code, "source_handle_mismatch")

        estimate = service.estimate(estimate_request)
        request = deepcopy(payload["requests"]["materialize"])
        request["option_id"] = estimate["option_id"]
        request["option_handle"] = "tampered"

        with self.assertRaises(BridgeError) as captured:
            service.materialize(request)
        self.assertEqual(captured.exception.code, "materialization_option_mismatch")

        request["option_handle"] = estimate["option_handle"]
        request["target_engine_generation"] = 2
        with self.assertRaises(BridgeError) as captured:
            service.materialize(request)
        self.assertEqual(captured.exception.code, "materialization_target_mismatch")

        request["target_engine_generation"] = 1
        request["sink_value"] = ""
        with self.assertRaises(BridgeError) as captured:
            service.materialize(request)
        self.assertEqual(captured.exception.code, "invalid_field")

        request["sink_value"] = "sink-1"
        service.materialize(request)

    def test_execution_rejection_surfaces_as_retryable_bridge_failure(self) -> None:
        payload = fixture_payload()
        service = LocusBridgeService(
            InMemoryPlanner(), execution_runner=BaselineExecutionRunner(catalog=BackendCatalog())
        )
        load_fixture(FIXTURE, service)
        lookup = service.lookup(deepcopy(payload["requests"]["lookup"]))
        estimate_request = deepcopy(payload["requests"]["estimate"])
        estimate_request["source_handle"] = lookup["match_result"]["validation"]["source_handle"]
        estimate = service.estimate(estimate_request)
        request = deepcopy(payload["requests"]["materialize"])
        request["option_id"] = estimate["option_id"]
        request["option_handle"] = estimate["option_handle"]

        with self.assertRaises(BridgeError) as captured:
            service.materialize(request)

        self.assertEqual(captured.exception.status.value, 503)
        self.assertEqual(captured.exception.code, "materialization_unavailable")


class LocusBridgeHttpTests(unittest.TestCase):
    def test_http_server_enforces_bearer_auth_and_returns_structured_errors(self) -> None:
        server = LocusBridgeHttpServer(
            ("127.0.0.1", 0), configured_service(), api_key="bridge-secret"
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_address[1]}"
        try:
            request = urllib.request.Request(
                f"{base_url}/locus/v1/lookup",
                data=json.dumps(fixture_payload()["requests"]["lookup"]).encode(),
                headers={"content-type": "application/json"},
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as captured:
                urllib.request.urlopen(request, timeout=2)
            http_error = captured.exception
            self.assertEqual(http_error.code, 401)
            error = json.loads(http_error.read())
            http_error.close()
            self.assertEqual(error["error"]["code"], "unauthorized")

            request.add_header("authorization", "Bearer bridge-secret")
            with urllib.request.urlopen(request, timeout=2) as response:
                payload = json.load(response)
            self.assertEqual(payload["schema_version"], BRIDGE_SCHEMA)
            self.assertEqual(
                payload["match_result"]["entry"]["identity"]["entry_id"], "nexus-state-1"
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
