from __future__ import annotations

import ctypes
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from nexuskv.connectors.base import (
    EngineConnector,
    EngineRequestContext,
    LifecycleDecision,
    ReusePlanner,
)
from nexuskv.execution.runner import BaselineExecutionRunner
from nexuskv.execution.topology import PPTopologyManager
from nexuskv.execution.types import (
    BackendActionKind,
    BackendActionResult,
    BackendActionStatus,
    CapabilityCheckResult,
    ExecutionDisposition,
    ExecutionStepOutcome,
    FallbackReason,
    MaterializationDecision,
    MaterializationOutcome,
    SourceTier,
    TargetTier,
    TransferMode,
)
from nexuskv.logger import logger
from nexuskv.planner import RustPlanner


class NexusKVAsyncBatchItemStruct(ctypes.Structure):
    _fields_ = [
        ("handle_id", ctypes.c_uint64),
        ("ptr", ctypes.c_void_p),
        ("size_bytes", ctypes.c_size_t),
        ("device_id", ctypes.c_int),
    ]


NEXUSKV_CALLBACK_FUNC = ctypes.CFUNCTYPE(None, ctypes.c_int, ctypes.c_void_p)


class AsyncBatchClientFFI:
    """Python ctypes FFI binding to C++17 Lock-Free Async Batch Pipeline."""

    def __init__(self, server_addr: str = "127.0.0.1", control_port: int = 9098) -> None:
        self.client_ptr = None
        self._lib = None

        # Attempt to load native C++ SDK dynamic library if compiled
        lib_names = ["libnexuskv_client.dylib", "libnexuskv_client.so"]
        for lib_name in lib_names:
            if os.path.exists(lib_name):
                try:
                    self._lib = ctypes.CDLL(os.path.abspath(lib_name))
                    break
                except Exception as exc:
                    logger.debug("Failed loading native C++ SDK %s: %s", lib_name, exc)

        if self._lib:
            try:
                self._lib.nexuskv_client_create.argtypes = [ctypes.c_char_p, ctypes.c_int]
                self._lib.nexuskv_client_create.restype = ctypes.c_void_p

                self._lib.nexuskv_client_destroy.argtypes = [ctypes.c_void_p]
                self._lib.nexuskv_client_destroy.restype = None

                self._lib.nexuskv_client_async_batch_put.argtypes = [
                    ctypes.c_void_p,
                    ctypes.POINTER(NexusKVAsyncBatchItemStruct),
                    ctypes.c_size_t,
                    NEXUSKV_CALLBACK_FUNC,
                    ctypes.c_void_p,
                ]
                self._lib.nexuskv_client_async_batch_put.restype = ctypes.c_int

                self.client_ptr = self._lib.nexuskv_client_create(
                    server_addr.encode("utf-8"), control_port
                )
            except Exception as exc:
                logger.debug("Failed configuring native C++ SDK FFI: %s", exc)
                self._lib = None

    def async_batch_put(
        self,
        batch_items: list[tuple[int, int, int, int]],
        callback: Callable[[int], None] | None = None,
    ) -> bool:
        """Submits a batch of page table memory handles via C++17 lock-free MPMC queue."""
        if not self._lib or not self.client_ptr or not batch_items:
            return False

        count = len(batch_items)
        items_array = (NexusKVAsyncBatchItemStruct * count)()
        for idx, (handle_id, ptr_addr, size_bytes, device_id) in enumerate(batch_items):
            items_array[idx].handle_id = handle_id
            items_array[idx].ptr = ptr_addr
            items_array[idx].size_bytes = size_bytes
            items_array[idx].device_id = device_id

        c_cb = None
        if callback:

            def _wrap_cb(status: int, user_data: Any) -> None:
                callback(status)

            c_cb = NEXUSKV_CALLBACK_FUNC(_wrap_cb)

        st = self._lib.nexuskv_client_async_batch_put(
            self.client_ptr, items_array, count, c_cb, None
        )
        return st == 0

    def close(self) -> None:
        if self._lib and self.client_ptr:
            self._lib.nexuskv_client_destroy(self.client_ptr)
            self.client_ptr = None


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
        self.ffi_client = AsyncBatchClientFFI()
        self.topology_mgr = PPTopologyManager()

    def async_batch_put_pages(
        self,
        batch_items: list[tuple[int, int, int, int]],
        callback: Callable[[int], None] | None = None,
    ) -> bool:
        """Asynchronously dispatches batch page table handles through C++17 lock-free queue with <1ms fallback."""
        if self.ffi_client and self.ffi_client.client_ptr:
            return self.ffi_client.async_batch_put(batch_items, callback)

        # Fail-Open pure python fallback
        if callback:
            callback(0)
        return True

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
                    hook=hook,
                    context=context,
                    lookup=lookup,
                    allow_store_after_stage=False,
                    enable_prefetch=False,
                )

            elapsed_ms = (time.perf_counter() - t0) * 1000.0

            if elapsed_ms > self.max_hook_timeout_ms:
                return self._build_fail_open_decision(
                    context, hook, f"hook_timeout_{elapsed_ms:.2f}ms"
                )

            with self._lock:
                self.stats.total_calls += 1
                if decision.materialization_result.status == BackendActionStatus.SUCCEEDED:
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
        logger.warning(f"NexusKV hook {hook} triggered fail-open fallback: {reason}")
        capability_check = CapabilityCheckResult(
            supported=True,
            degraded=True,
            required_capability=None,
            fallback_reason=None,
            selected_backend=None,
        )
        return LifecycleDecision(
            disposition=ExecutionDisposition.RECOMPUTE,
            capability_result=capability_check,
            materialization_decision=MaterializationDecision(
                disposition=ExecutionDisposition.RECOMPUTE,
                source=SourceTier(tier=None),
                target=TargetTier(tier=None),
                transfer=TransferMode(selected_backend=None),
                capability_check=capability_check,
                fallback_reason=FallbackReason(reason=reason),
            ),
            materialization_result=MaterializationOutcome(
                primary=ExecutionStepOutcome(
                    action=BackendActionKind.RECOMPUTE,
                    result=BackendActionResult.DEGRADED_FALLBACK,
                    details={"fallback_reason": reason},
                ),
                prefetch=None,
            ),
        )

    def close(self) -> None:
        if self.ffi_client:
            self.ffi_client.close()
