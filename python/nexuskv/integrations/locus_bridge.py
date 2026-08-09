from __future__ import annotations

import argparse
import hmac
import json
import os
import secrets
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from socketserver import TCPServer
from typing import Any, Protocol, cast

from nexuskv.connectors.base import (
    EngineRequestContext,
    LookupOutcome,
    LookupStatus,
    ReusePlanner,
)
from nexuskv.contracts.generated import (
    SCHEMA_VERSION,
    CacheEntry,
    CompatibilitySignal,
    Granularity,
    KeyIdentity,
    MatchClassification,
    MatchExtent,
    MatchResult,
    PartialHitPlan,
    PlanDisposition,
    QueryKey,
    RemainingWork,
    ReusableSlice,
    ReuseKey,
    TierKind,
)
from nexuskv.contracts.serde import from_primitive, to_primitive
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionStatus,
    MaterializationRequest,
    TransferStatus,
)
from nexuskv.planner.cost import CostEstimator

BRIDGE_SCHEMA = "locus.nexuskv-bridge.v1"
FIXTURE_SCHEMA = "locus.nexuskv-bridge.fixture.v1"
PROTOCOL_RECEIPT_NAMESPACE = "nexuskv.protocol-transfer-receipt.v1"
MAX_REQUEST_BYTES = 8 * 1024 * 1024
MAX_QUERY_TOKENS = 1_000_000


class MutableReusePlanner(ReusePlanner, Protocol):
    def insert(self, reuse_key: ReuseKey, entry: CacheEntry) -> None: ...


class BridgeError(Exception):
    def __init__(self, status: HTTPStatus, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


@dataclass(slots=True, frozen=True)
class ValidationEvidence:
    model_identity: dict[str, Any]
    input_semantic_identity: dict[str, Any]
    input_fingerprint: str


@dataclass(slots=True, frozen=True)
class RegisteredState:
    reuse_key: ReuseKey
    entry: CacheEntry
    validation: ValidationEvidence


@dataclass(slots=True)
class OptionRecord:
    option_id: str
    option_handle: str
    match: MatchResult
    target_id: str
    target_engine_id: str
    target_engine_generation: int
    target_residency: str
    expires_at_monotonic: float
    consumed: bool = False


class LocusBridgeService:
    """NexusKV-owned implementation of the Locus StateProvider wire contract.

    The baseline execution runner proves action selection and receipt plumbing. It
    does not claim physical state movement; materialize responses therefore carry
    a protocol evidence level, zero transferred bytes, and a protocol-only receipt
    namespace until a native backend reports verified movement.
    """

    def __init__(
        self,
        planner: MutableReusePlanner,
        *,
        execution_runner: BaselineExecutionRunner | None = None,
        cost_estimator: CostEstimator | None = None,
        option_ttl_seconds: float = 30.0,
    ) -> None:
        if option_ttl_seconds <= 0:
            raise ValueError("option_ttl_seconds must be positive")
        self._planner = planner
        self._execution_runner = execution_runner or BaselineExecutionRunner()
        self._cost_estimator = cost_estimator or CostEstimator()
        self._option_ttl_seconds = option_ttl_seconds
        self._states: dict[str, RegisteredState] = {}
        self._source_handles: dict[str, str] = {}
        self._states_by_source_handle: dict[str, RegisteredState] = {}
        self._options: dict[str, OptionRecord] = {}
        self._lock = threading.RLock()

    def register(self, state: RegisteredState) -> None:
        if state.reuse_key.identity != state.entry.identity.key:
            raise ValueError("reuse key does not match cache entry identity")
        if state.entry.descriptor.schema_version != SCHEMA_VERSION:
            raise ValueError(
                f"unsupported NexusKV descriptor schema: {state.entry.descriptor.schema_version}"
            )
        if state.entry.identity.key.model != state.validation.model_identity.get("model_revision"):
            raise ValueError("cache entry model does not match Locus model validation evidence")
        with self._lock:
            registry_key = _state_registry_key(state.entry)
            if registry_key in self._states:
                raise ValueError("duplicate bridge state identity")
            source_handle = secrets.token_urlsafe(32)
            self._planner.insert(state.reuse_key, state.entry)
            self._states[registry_key] = state
            self._source_handles[registry_key] = source_handle
            self._states_by_source_handle[source_handle] = state

    def lookup(self, body: dict[str, Any]) -> dict[str, Any]:
        _expect_fields(
            body,
            required={
                "schema_version",
                "nexuskv_schema_version",
                "identity",
                "locus_model_identity",
                "locus_input_semantic_identity",
                "input_fingerprint",
            },
        )
        _validate_schemas(body)
        identity = _require_object(body, "identity")
        _expect_fields(
            identity,
            required={"tenant", "namespace", "model", "engine_family", "semantic_type", "tokens"},
        )
        tokens = _require_int_list(identity, "tokens")
        if len(tokens) > MAX_QUERY_TOKENS:
            raise BridgeError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "query_too_large",
                f"tokens exceeds the {MAX_QUERY_TOKENS} item bridge limit",
            )
        try:
            query = QueryKey(
                identity=from_primitive(
                    KeyIdentity,
                    {
                        "tenant": _require_string(identity, "tenant"),
                        "namespace": _require_string(identity, "namespace"),
                        "model": _require_string(identity, "model"),
                        "engine_family": _require_string(identity, "engine_family"),
                        "semantic_type": _require_string(identity, "semantic_type"),
                        "tokens": tokens,
                        "block_id": None,
                        "page_id": None,
                    },
                )
            )
        except (KeyError, TypeError, ValueError) as error:
            raise BridgeError(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                "unsupported_identity",
                f"unsupported NexusKV identity: {error}",
            ) from error

        with self._lock:
            match = self._planner.lookup(query)
            if match is None:
                return {
                    "schema_version": BRIDGE_SCHEMA,
                    "nexuskv_schema_version": SCHEMA_VERSION,
                    "match_result": None,
                }
            registry_key = _state_registry_key(match.entry)
            state = self._states.get(registry_key)
            source_handle = self._source_handles.get(registry_key)

        if state is None or source_handle is None:
            raise BridgeError(
                HTTPStatus.CONFLICT,
                "validation_evidence_missing",
                "matched NexusKV state has no registered Locus compatibility evidence",
            )
        supplied_model = _require_object(body, "locus_model_identity")
        supplied_input = _require_object(body, "locus_input_semantic_identity")
        supplied_fingerprint = _require_string(body, "input_fingerprint")
        incompatible_fingerprint = (
            match.classification == MatchClassification.EXACT
            and supplied_fingerprint != state.validation.input_fingerprint
        )
        if (
            supplied_model != state.validation.model_identity
            or supplied_input != state.validation.input_semantic_identity
            or incompatible_fingerprint
        ):
            raise BridgeError(
                HTTPStatus.CONFLICT,
                "validation_evidence_mismatch",
                "request model or input semantics do not match registered state evidence",
            )

        response_match = cast(dict[str, Any], to_primitive(match))
        response_match["validation"] = {
            "model_identity": state.validation.model_identity,
            "input_semantic_identity": state.validation.input_semantic_identity,
            "source_handle": source_handle,
        }
        return {
            "schema_version": BRIDGE_SCHEMA,
            "nexuskv_schema_version": SCHEMA_VERSION,
            "match_result": response_match,
        }

    def estimate(self, body: dict[str, Any]) -> dict[str, Any]:
        _expect_fields(
            body,
            required={
                "schema_version",
                "source_state",
                "source_handle",
                "source_locator",
                "source_tier",
                "target_id",
                "target_engine_id",
                "target_engine_generation",
                "target_residency",
            },
        )
        _validate_bridge_schema(body)
        source_state = _require_string(body, "source_state")
        source_handle = _require_string(body, "source_handle")
        with self._lock:
            state = self._states_by_source_handle.get(source_handle)
        if state is None:
            raise BridgeError(
                HTTPStatus.CONFLICT,
                "source_handle_mismatch",
                "estimate source_handle does not authorize a matched state",
            )
        if source_state != state.entry.identity.entry_id:
            raise BridgeError(
                HTTPStatus.CONFLICT,
                "source_state_mismatch",
                "estimate source_state does not match its source_handle",
            )
        source_locator = _require_string(body, "source_locator")
        source_tier_text = _require_string(body, "source_tier")
        if (
            source_locator != state.entry.location.locator
            or source_tier_text != state.entry.location.tier.value
        ):
            raise BridgeError(
                HTTPStatus.CONFLICT,
                "source_location_mismatch",
                "estimate source locator or tier does not match the registered state",
            )
        target_generation = _require_positive_int(body, "target_engine_generation")
        target_id = _require_string(body, "target_id")
        target_engine_id = _require_string(body, "target_engine_id")
        target_residency = _require_string(body, "target_residency")
        local = _locator_is_local(source_locator, target_residency)
        token_count = len(state.entry.identity.key.tokens)
        payload_bytes = _payload_bytes(state.entry.descriptor.granularity, token_count)
        estimate = self._cost_estimator.estimate(
            token_count=token_count,
            payload_bytes=payload_bytes,
            source_tier=state.entry.location.tier,
            target_tier=TierKind.DEVICE,
        )
        estimated_transfer_micros = max(
            0,
            round((estimate.t_transfer_seconds + estimate.t_restore_seconds) * 1_000_000),
        )
        option_id = f"nexus-option-{secrets.token_hex(12)}"
        option_handle = secrets.token_urlsafe(32)
        match = _match_for_registered_state(state)
        record = OptionRecord(
            option_id=option_id,
            option_handle=option_handle,
            match=match,
            target_id=target_id,
            target_engine_id=target_engine_id,
            target_engine_generation=target_generation,
            target_residency=target_residency,
            expires_at_monotonic=time.monotonic() + self._option_ttl_seconds,
        )
        with self._lock:
            self._options[option_id] = record
        return {
            "schema_version": BRIDGE_SCHEMA,
            "option_id": option_id,
            "option_handle": option_handle,
            "locality": "local" if local else "remote",
            "topology_path": None if local else f"{source_tier_text}->device@{target_residency}",
            "estimated_transfer_micros": estimated_transfer_micros,
        }

    def materialize(self, body: dict[str, Any]) -> dict[str, Any]:
        _expect_fields(
            body,
            required={
                "schema_version",
                "option_id",
                "option_handle",
                "import_id",
                "target_id",
                "target_engine_id",
                "target_engine_generation",
                "sink_namespace",
                "sink_value",
            },
        )
        _validate_bridge_schema(body)
        option_id = _require_string(body, "option_id")
        option_handle = _require_string(body, "option_handle")
        import_id = _require_string(body, "import_id")
        sink_namespace = _require_string(body, "sink_namespace")
        sink_value = _require_string(body, "sink_value")
        with self._lock:
            option = self._options.get(option_id)
            if option is None:
                raise BridgeError(
                    HTTPStatus.CONFLICT,
                    "unknown_materialization_option",
                    "materialization option was not issued by this bridge",
                )
            if option.consumed:
                raise BridgeError(
                    HTTPStatus.CONFLICT,
                    "materialization_option_consumed",
                    "materialization option has already been consumed",
                )
            if time.monotonic() >= option.expires_at_monotonic:
                raise BridgeError(
                    HTTPStatus.CONFLICT,
                    "materialization_option_expired",
                    "materialization option has expired",
                )
            if not hmac.compare_digest(option.option_handle, option_handle):
                raise BridgeError(
                    HTTPStatus.CONFLICT,
                    "materialization_option_mismatch",
                    "materialization option handle does not match its option id",
                )
            target = (
                _require_string(body, "target_id"),
                _require_string(body, "target_engine_id"),
                _require_positive_int(body, "target_engine_generation"),
            )
            if target != (
                option.target_id,
                option.target_engine_id,
                option.target_engine_generation,
            ):
                raise BridgeError(
                    HTTPStatus.CONFLICT,
                    "materialization_target_mismatch",
                    "materialization target does not match the estimated option",
                )

            outcome = self._execute_protocol_materialization(option)
            result = outcome.primary.result
            if (
                result.status != BackendActionStatus.SUCCEEDED
                or result.executed_kind != BackendActionKind.MATERIALIZE
                or result.transfer_session is None
                or result.transfer_session.result.status != TransferStatus.COMPLETED
            ):
                raise BridgeError(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    "materialization_unavailable",
                    result.detail or "NexusKV execution boundary did not materialize the state",
                )
            option.consumed = True

        receipt_value = secrets.token_urlsafe(24)
        return {
            "schema_version": BRIDGE_SCHEMA,
            "bytes_transferred": 0,
            "receipt": {
                "namespace": PROTOCOL_RECEIPT_NAMESPACE,
                "value": receipt_value,
            },
            "evidence": {
                "level": "protocol",
                "physical_transfer_verified": False,
                "execution_backend": result.backend_name,
                "transfer_session": result.transfer_session.session_id,
                "sink_namespace": sink_namespace,
                "sink_value": sink_value,
                "import_id": import_id,
            },
        }

    def _execute_protocol_materialization(self, option: OptionRecord):
        match = option.match
        query = match.requested_key
        context = EngineRequestContext(
            tenant=query.identity.tenant,
            namespace=query.identity.namespace,
            model=query.identity.model,
            tokens=list(query.identity.tokens),
            descriptor=match.entry.descriptor,
            block_id=query.identity.block_id,
            page_id=query.identity.page_id,
        )
        partial_plan = None
        status = LookupStatus.HIT
        if match.classification != MatchClassification.EXACT:
            status = LookupStatus.PARTIAL
            partial_plan = _partial_plan_from_match(match)
        return self._execution_runner.execute(
            MaterializationRequest(
                hook="locus_import",
                context=context,
                lookup=LookupOutcome(
                    query=query,
                    status=status,
                    match=match,
                    partial_plan=partial_plan,
                ),
                preferred_backend=None,
                allow_store_after_stage=False,
                enable_prefetch=False,
            )
        )


def load_fixture(path: Path, service: LocusBridgeService) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema_version") != FIXTURE_SCHEMA:
        raise ValueError(f"fixture must use schema_version {FIXTURE_SCHEMA}")
    entries = payload.get("entries")
    if not isinstance(entries, list):
        raise ValueError("fixture entries must be an array")
    for raw in entries:
        if not isinstance(raw, dict):
            raise ValueError("fixture entry must be an object")
        validation = raw.get("validation")
        if not isinstance(validation, dict):
            raise ValueError("fixture validation must be an object")
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


class LocusBridgeHttpServer(ThreadingHTTPServer):
    daemon_threads = True

    def server_bind(self) -> None:
        # HTTPServer performs a reverse-DNS lookup during bind. The bridge only
        # needs the configured address and must start deterministically even when
        # cluster DNS is unavailable or slow.
        TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = host
        self.server_port = port

    def __init__(
        self,
        address: tuple[str, int],
        service: LocusBridgeService,
        *,
        api_key: str | None = None,
    ) -> None:
        super().__init__(address, LocusBridgeRequestHandler)
        self.service = service
        self.api_key = api_key


class LocusBridgeRequestHandler(BaseHTTPRequestHandler):
    server: LocusBridgeHttpServer

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/healthz":
            self._write_error(BridgeError(HTTPStatus.NOT_FOUND, "not_found", "route was not found"))
            return
        self._write_json(HTTPStatus.OK, {"status": "ok", "schema_version": BRIDGE_SCHEMA})

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._authorize()
            body = self._read_json_object()
            routes = {
                "/locus/v1/lookup": self.server.service.lookup,
                "/locus/v1/estimate": self.server.service.estimate,
                "/locus/v1/materialize": self.server.service.materialize,
            }
            handler = routes.get(self.path)
            if handler is None:
                raise BridgeError(HTTPStatus.NOT_FOUND, "not_found", "route was not found")
            self._write_json(HTTPStatus.OK, handler(body))
        except BridgeError as error:
            self._write_error(error)
        except Exception:
            self._write_error(
                BridgeError(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "bridge request failed internally",
                )
            )

    def log_message(self, format: str, *args: object) -> None:
        return

    def _authorize(self) -> None:
        api_key = self.server.api_key
        if api_key is None:
            return
        authorization = self.headers.get("authorization", "")
        prefix = "Bearer "
        supplied = authorization[len(prefix) :] if authorization.startswith(prefix) else ""
        if not hmac.compare_digest(api_key, supplied):
            raise BridgeError(HTTPStatus.UNAUTHORIZED, "unauthorized", "invalid bearer token")

    def _read_json_object(self) -> dict[str, Any]:
        length_text = self.headers.get("content-length")
        try:
            length = int(length_text or "")
        except ValueError as error:
            raise BridgeError(
                HTTPStatus.BAD_REQUEST, "invalid_content_length", "content-length is required"
            ) from error
        if length <= 0 or length > MAX_REQUEST_BYTES:
            raise BridgeError(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "request_too_large",
                f"request body must be between 1 and {MAX_REQUEST_BYTES} bytes",
            )
        try:
            body = json.loads(self.rfile.read(length))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BridgeError(
                HTTPStatus.BAD_REQUEST, "invalid_json", "request body must be valid JSON"
            ) from error
        if not isinstance(body, dict):
            raise BridgeError(
                HTTPStatus.BAD_REQUEST, "invalid_body", "request body must be a JSON object"
            )
        return body

    def _write_error(self, error: BridgeError) -> None:
        self._write_json(
            error.status,
            {
                "schema_version": BRIDGE_SCHEMA,
                "error": {"code": error.code, "message": error.message},
            },
        )

    def _write_json(self, status: HTTPStatus, body: dict[str, Any]) -> None:
        payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
        self.send_response(status.value)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def serve(
    service: LocusBridgeService,
    *,
    host: str,
    port: int,
    api_key: str | None = None,
    ready_file: Path | None = None,
) -> None:
    server = LocusBridgeHttpServer((host, port), service, api_key=api_key)
    bound_host, bound_port = server.server_address[:2]
    ready = {
        "schema_version": BRIDGE_SCHEMA,
        "base_url": f"http://{bound_host}:{bound_port}",
        "evidence_level": "protocol",
    }
    if ready_file is not None:
        ready_file.write_text(json.dumps(ready, sort_keys=True) + "\n", encoding="utf-8")
    else:
        print(json.dumps(ready, sort_keys=True), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the Locus NexusKV bridge protocol")
    parser.add_argument("--listen", default="127.0.0.1:9099", help="host:port, port 0 is allowed")
    parser.add_argument("--fixture", type=Path, help="optional registered-state fixture")
    parser.add_argument("--ready-file", type=Path, help="write bound base URL as JSON")
    parser.add_argument("--api-key-env", help="environment variable containing the bearer token")
    args = parser.parse_args()
    host, port = _parse_listen(args.listen)
    if args.api_key_env and args.api_key_env not in os.environ:
        parser.error(f"environment variable {args.api_key_env} is not set")
    from nexuskv.planner.rust_backend import RustPlanner

    service = LocusBridgeService(cast(MutableReusePlanner, RustPlanner()))
    if args.fixture is not None:
        load_fixture(args.fixture, service)
    serve(
        service,
        host=host,
        port=port,
        api_key=os.environ.get(args.api_key_env) if args.api_key_env else None,
        ready_file=args.ready_file,
    )


def _match_for_registered_state(state: RegisteredState) -> MatchResult:
    query = QueryKey(identity=state.reuse_key.identity)
    return MatchResult(
        classification=MatchClassification.EXACT,
        matched_key=state.reuse_key,
        requested_key=query,
        matched_extent=MatchExtent(
            units=len(query.identity.tokens),
            granularity=state.entry.descriptor.granularity,
        ),
        entry=state.entry,
        remaining=RemainingWork(tokens=[], fetch_required=False, recompute_required=False),
        compatibility=CompatibilitySignal(
            reusable=True,
            fallback_to_recompute=False,
            reason="registered state",
        ),
    )


def _partial_plan_from_match(match: MatchResult) -> PartialHitPlan:
    return PartialHitPlan(
        disposition=PlanDisposition.FULL_REUSE
        if not match.remaining.tokens
        else PlanDisposition.PARTIAL_REUSE,
        reusable=ReusableSlice(
            tokens=list(match.requested_key.identity.tokens[: match.matched_extent.units]),
            source_tier=match.entry.location.tier,
        ),
        remaining=RemainingWork(
            tokens=list(match.remaining.tokens),
            fetch_required=match.remaining.fetch_required,
            recompute_required=match.remaining.recompute_required,
        ),
        entry=match.entry,
    )


def _validate_schemas(body: dict[str, Any]) -> None:
    _validate_bridge_schema(body)
    if _require_string(body, "nexuskv_schema_version") != SCHEMA_VERSION:
        raise BridgeError(
            HTTPStatus.UNPROCESSABLE_ENTITY,
            "unsupported_nexuskv_schema",
            f"nexuskv_schema_version must be {SCHEMA_VERSION}",
        )


def _validate_bridge_schema(body: dict[str, Any]) -> None:
    if _require_string(body, "schema_version") != BRIDGE_SCHEMA:
        raise BridgeError(
            HTTPStatus.UNPROCESSABLE_ENTITY,
            "unsupported_bridge_schema",
            f"schema_version must be {BRIDGE_SCHEMA}",
        )


def _expect_fields(
    body: dict[str, Any], *, required: set[str], optional: set[str] | None = None
) -> None:
    missing = required - body.keys()
    if missing:
        raise BridgeError(
            HTTPStatus.BAD_REQUEST,
            "missing_field",
            f"missing required fields: {', '.join(sorted(missing))}",
        )
    unknown = body.keys() - required - (optional or set())
    if unknown:
        raise BridgeError(
            HTTPStatus.BAD_REQUEST,
            "unknown_field",
            f"unknown fields: {', '.join(sorted(unknown))}",
        )


def _require_object(body: dict[str, Any], name: str) -> dict[str, Any]:
    value = body.get(name)
    if not isinstance(value, dict):
        raise BridgeError(HTTPStatus.BAD_REQUEST, "invalid_field", f"{name} must be an object")
    return value


def _require_string(body: dict[str, Any], name: str) -> str:
    value = body.get(name)
    if not isinstance(value, str) or not value:
        raise BridgeError(
            HTTPStatus.BAD_REQUEST, "invalid_field", f"{name} must be a non-empty string"
        )
    return value


def _require_positive_int(body: dict[str, Any], name: str) -> int:
    value = body.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise BridgeError(
            HTTPStatus.BAD_REQUEST, "invalid_field", f"{name} must be a positive integer"
        )
    return value


def _require_int_list(body: dict[str, Any], name: str) -> list[int]:
    value = body.get(name)
    if not isinstance(value, list) or any(
        not isinstance(item, int) or isinstance(item, bool) or item < 0 or item > 0xFFFF_FFFF
        for item in value
    ):
        raise BridgeError(
            HTTPStatus.BAD_REQUEST,
            "invalid_field",
            f"{name} must be an array of unsigned 32-bit integers",
        )
    return value


def _locator_is_local(locator: str, target_residency: str) -> bool:
    return locator.startswith(f"node://{target_residency}/")


def _payload_bytes(granularity: Granularity, token_count: int) -> int:
    unit = (
        4096
        if granularity == Granularity.PAGE
        else 1024
        if granularity == Granularity.BLOCK
        else 256
    )
    return token_count * unit


def _state_registry_key(entry: CacheEntry) -> str:
    return json.dumps(to_primitive(entry.identity), sort_keys=True, separators=(",", ":"))


def _parse_listen(value: str) -> tuple[str, int]:
    host, separator, port_text = value.rpartition(":")
    if not separator or not host:
        raise argparse.ArgumentTypeError("listen must be host:port")
    try:
        port = int(port_text)
    except ValueError as error:
        raise argparse.ArgumentTypeError("listen port must be an integer") from error
    if port < 0 or port > 65535:
        raise argparse.ArgumentTypeError("listen port must be between 0 and 65535")
    return host, port


if __name__ == "__main__":
    main()
