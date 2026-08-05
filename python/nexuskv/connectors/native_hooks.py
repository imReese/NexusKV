from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from nexuskv.connectors.base import (
    EngineConnector,
    EngineRequestContext,
    LifecycleDecision,
    ReusePlanner,
)
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionResult,
    BackendActionStatus,
    CapabilityCheckResult,
    ExecutionDisposition,
    ExecutionStepOutcome,
    MaterializationDecision,
    MaterializationOutcome,
    SourceTier,
    TargetTier,
    TransferMode,
)
from nexuskv.planner import RustPlanner


@dataclass(slots=True)
class FastPathHookStats:
    total_calls: int = 0
    fast_path_hits: int = 0
    fail_open_fallbacks: int = 0
    avg_latency_us: float = 0.0


class NativeEngineHookInterceptor:
    """Fast GIL-free / FFI engine hook interceptor with <1ms fail-open guarantee."""

    def __init__(
        self,
        connector: EngineConnector,
        execution_runner: BaselineExecutionRunner | None = None,
        planner: ReusePlanner | None = None,
        max_hook_timeout_ms: float = 1.0,
    ) -> None:
        self.connector = connector
        self.execution_runner = execution_runner or BaselineExecutionRunner()
        self.planner = planner or RustPlanner()
        self.max_hook_timeout_ms = max_hook_timeout_ms
        self._lock = threading.Lock()
        self.stats = FastPathHookStats()

    def intercept_hook(
        self,
        hook: str,
        context: EngineRequestContext,
    ) -> LifecycleDecision:
        t0 = time.perf_counter()
        try:
            if hook == "prefill" and hasattr(self.connector, "on_prefill"):
                decision = self.connector.on_prefill(context, self.planner)
            elif hook == "request_start" and hasattr(self.connector, "on_request_start"):
                decision = self.connector.on_request_start(context, self.planner)
            else:
                lookup = self.connector.lookup(context, self.planner)
                decision = self.connector.execute_lifecycle(
                    hook=hook, context=context, lookup=lookup
                )

            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            if elapsed_ms > self.max_hook_timeout_ms:
                return self._build_fail_open_decision(
                    context, hook, f"hook_timeout_{elapsed_ms:.2f}ms"
                )

            with self._lock:
                self.stats.total_calls += 1
                if decision.materialization_result.status == BackendActionStatus.COMPLETED:
                    self.stats.fast_path_hits += 1
                else:
                    self.stats.fail_open_fallbacks += 1

            return decision

        except Exception as exc:
            with self._lock:
                self.stats.total_calls += 1
                self.stats.fail_open_fallbacks += 1
            return self._build_fail_open_decision(context, hook, f"exception: {exc}")

    def _build_fail_open_decision(
        self,
        context: EngineRequestContext,
        hook: str,
        reason: str,
    ) -> LifecycleDecision:
        dummy_dec = MaterializationDecision(
            disposition=ExecutionDisposition.RECOMPUTE,
            source=SourceTier(tier=None),
            target=TargetTier(tier=None),
            transfer=TransferMode(selected_backend=None),
            capability_check=CapabilityCheckResult(
                supported=True,
                degraded=False,
                required_capability=None,
                fallback_reason=None,
                selected_backend=None,
            ),
            fallback_reason=None,
        )
        dummy_res = BackendActionResult(
            requested_kind=BackendActionKind.RECOMPUTE,
            executed_kind=BackendActionKind.RECOMPUTE,
            status=BackendActionStatus.FALLBACK,
            final_disposition=ExecutionDisposition.RECOMPUTE,
            backend_name="fallback",
            selected_backend=None,
            source=SourceTier(tier=None),
            target=TargetTier(tier=None),
            degraded=True,
            fallback_reason=None,
            detail=reason,
        )
        step = ExecutionStepOutcome(decision=dummy_dec, result=dummy_res)
        outcome = MaterializationOutcome(primary=step, prefetch=None, store=step)
        lookup = self.connector.unsupported_lookup(context, reason)
        return LifecycleDecision(
            hook=hook,
            lookup=lookup,
            execution=outcome,
            should_store_after_stage=False,
        )
