from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from nexuskv.contracts.generated import (
    AttentionStateDescriptor,
    CacheEntry,
    CompatibilityFlag,
    CompatibilitySignal,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    KeyIdentity,
    MatchClassification,
    MatchExtent,
    MatchResult,
    MaterializationCapability,
    PartialHitPlan,
    PlanDisposition,
    PolicyHint,
    QueryKey,
    RemainingWork,
    ReusableSlice,
    ReuseKey,
    TierKind,
    TransferBackend,
)


class LookupStatus(StrEnum):
    HIT = "hit"
    PARTIAL = "partial"
    MISS = "miss"
    UNSUPPORTED = "unsupported"


class MaterializationMode(StrEnum):
    FULL = "full"
    PARTIAL = "partial"


class PrefetchStatus(StrEnum):
    SCHEDULED = "scheduled"
    SKIPPED = "skipped"
    UNSUPPORTED = "unsupported"


class FallbackMode(StrEnum):
    RECOMPUTE = "recompute"
    SIMPLER_TRANSFER = "simpler_transfer"
    SKIP = "skip"


@dataclass(slots=True)
class ConnectorCapabilities:
    exact_reuse: bool
    prefix_reuse: bool
    partial_materialization: bool
    prefetch: bool
    decode_lookup: bool
    supported_hooks: tuple[str, ...]
    supported_transfer_backends: tuple[TransferBackend, ...]
    compatibility_flags: tuple[CompatibilityFlag, ...]


@dataclass(slots=True)
class EngineRequestContext:
    tenant: str
    namespace: str
    model: str
    tokens: list[int]
    descriptor: AttentionStateDescriptor
    preferred_backend: TransferBackend | None = None
    block_id: int | None = None
    page_id: int | None = None


@dataclass(slots=True)
class SGLangLifecycleContext(EngineRequestContext):
    pass


@dataclass(slots=True)
class VLLMLifecycleContext(EngineRequestContext):
    pass


@dataclass(slots=True)
class FallbackPlan:
    mode: FallbackMode
    reason: str
    selected_backend: TransferBackend | None


@dataclass(slots=True)
class LookupOutcome:
    query: QueryKey
    status: LookupStatus
    match: MatchResult | None
    partial_plan: PartialHitPlan | None
    fallback: FallbackPlan | None


@dataclass(slots=True)
class MaterializationDecision:
    mode: MaterializationMode | None
    backend: TransferBackend | None
    target_tier: TierKind | None
    degraded: bool
    fallback: FallbackPlan | None


@dataclass(slots=True)
class PrefetchDecision:
    status: PrefetchStatus
    backend: TransferBackend | None
    degraded: bool
    plan: PartialHitPlan | None
    fallback: FallbackPlan | None


@dataclass(slots=True)
class StoreIntent:
    reuse_key: ReuseKey
    entry: CacheEntry


@dataclass(slots=True)
class LifecycleDecision:
    hook: str
    lookup: LookupOutcome
    materialization: MaterializationDecision | None
    prefetch: PrefetchDecision | None
    should_store_after_stage: bool


class ReusePlanner(Protocol):
    def lookup(self, query: QueryKey) -> MatchResult | None:
        raise NotImplementedError

    def plan_partial_hit(self, query: QueryKey) -> PartialHitPlan | None:
        raise NotImplementedError


class EngineConnector(ABC):
    engine_name: str

    @abstractmethod
    def supported_hooks(self) -> tuple[str, ...]:
        raise NotImplementedError

    @abstractmethod
    def default_descriptor(self) -> AttentionStateDescriptor:
        raise NotImplementedError

    @abstractmethod
    def probe_capabilities(self, descriptor: AttentionStateDescriptor | None = None) -> ConnectorCapabilities:
        raise NotImplementedError

    def build_query_key(self, context: EngineRequestContext) -> QueryKey:
        return QueryKey(
            identity=KeyIdentity(
                tenant=context.tenant,
                namespace=context.namespace,
                model=context.model,
                engine_family=context.descriptor.engine_family,
                semantic_type=context.descriptor.semantic_type,
                tokens=list(context.tokens),
                block_id=context.block_id,
                page_id=context.page_id,
            )
        )

    def lookup(self, context: EngineRequestContext, planner: ReusePlanner) -> LookupOutcome:
        query = self.build_query_key(context)
        match = planner.lookup(query)
        if match is None:
            return LookupOutcome(
                query=query,
                status=LookupStatus.MISS,
                match=None,
                partial_plan=None,
                fallback=None,
            )

        if match.classification == MatchClassification.EXACT:
            return LookupOutcome(
                query=query,
                status=LookupStatus.HIT,
                match=match,
                partial_plan=None,
                fallback=None,
            )

        return LookupOutcome(
            query=query,
            status=LookupStatus.PARTIAL,
            match=match,
            partial_plan=planner.plan_partial_hit(query),
            fallback=None,
        )

    def unsupported_lookup(self, context: EngineRequestContext, reason: str) -> LookupOutcome:
        return LookupOutcome(
            query=self.build_query_key(context),
            status=LookupStatus.UNSUPPORTED,
            match=None,
            partial_plan=None,
            fallback=FallbackPlan(mode=FallbackMode.SKIP, reason=reason, selected_backend=None),
        )

    def materialize(
        self,
        descriptor: AttentionStateDescriptor,
        *,
        match: MatchResult | None = None,
        partial_plan: PartialHitPlan | None = None,
        preferred_backend: TransferBackend | None = None,
    ) -> MaterializationDecision:
        if partial_plan is not None:
            if MaterializationCapability.PARTIAL not in descriptor.materialization.capabilities:
                return MaterializationDecision(
                    mode=None,
                    backend=None,
                    target_tier=None,
                    degraded=False,
                    fallback=self.fallback(
                        descriptor,
                        "partial materialization is unsupported for this descriptor",
                        preferred_backend,
                    ),
                )
            backend, degraded, fallback = self.select_transfer_backend(descriptor, preferred_backend)
            return MaterializationDecision(
                mode=MaterializationMode.PARTIAL,
                backend=backend,
                target_tier=partial_plan.reusable.source_tier,
                degraded=degraded,
                fallback=fallback,
            )

        if match is not None:
            if MaterializationCapability.FULL not in descriptor.materialization.capabilities:
                return MaterializationDecision(
                    mode=None,
                    backend=None,
                    target_tier=None,
                    degraded=False,
                    fallback=self.fallback(
                        descriptor,
                        "full materialization is unsupported for this descriptor",
                        preferred_backend,
                    ),
                )
            backend, degraded, fallback = self.select_transfer_backend(descriptor, preferred_backend)
            return MaterializationDecision(
                mode=MaterializationMode.FULL,
                backend=backend,
                target_tier=match.entry.location.tier,
                degraded=degraded,
                fallback=fallback,
            )

        return MaterializationDecision(
            mode=None,
            backend=None,
            target_tier=None,
            degraded=False,
            fallback=None,
        )

    def prefetch(self, context: EngineRequestContext, planner: ReusePlanner) -> PrefetchDecision:
        descriptor = context.descriptor
        if MaterializationCapability.PREFETCH not in descriptor.materialization.capabilities:
            return PrefetchDecision(
                status=PrefetchStatus.UNSUPPORTED,
                backend=None,
                degraded=False,
                plan=None,
                fallback=FallbackPlan(
                    mode=FallbackMode.SKIP,
                    reason="prefetch is unsupported for this descriptor",
                    selected_backend=None,
                ),
            )

        plan = planner.plan_partial_hit(self.build_query_key(context))
        if plan is None:
            return PrefetchDecision(
                status=PrefetchStatus.SKIPPED,
                backend=None,
                degraded=False,
                plan=None,
                fallback=None,
            )

        backend, degraded, fallback = self.select_transfer_backend(descriptor, context.preferred_backend)
        if backend is None:
            return PrefetchDecision(
                status=PrefetchStatus.SKIPPED,
                backend=None,
                degraded=False,
                plan=plan,
                fallback=fallback,
            )

        return PrefetchDecision(
            status=PrefetchStatus.SCHEDULED,
            backend=backend,
            degraded=degraded,
            plan=plan,
            fallback=fallback,
        )

    def prepare_store(
        self,
        context: EngineRequestContext,
        *,
        entry_id: str,
        locator: str,
        tier: TierKind,
        generation: int = 1,
        lineage: str = "connector-generated",
    ) -> StoreIntent:
        query = self.build_query_key(context)
        reuse_key = ReuseKey(identity=query.identity)
        entry = CacheEntry(
            identity=EntryIdentity(
                key=query.identity,
                entry_id=entry_id,
                version=EntryVersion(generation=generation, lineage=lineage),
            ),
            descriptor=context.descriptor,
            location=EntryLocation(tier=tier, locator=locator),
            policy_hint=PolicyHint(
                reusable=True,
                admission_hint="connector_store",
                eviction_hint="default",
            ),
        )
        return StoreIntent(reuse_key=reuse_key, entry=entry)

    def fallback(
        self,
        descriptor: AttentionStateDescriptor,
        reason: str,
        preferred_backend: TransferBackend | None,
    ) -> FallbackPlan:
        available = tuple(path.backend for path in descriptor.transfer_paths)
        if preferred_backend is not None and preferred_backend not in available and available:
            return FallbackPlan(
                mode=FallbackMode.SIMPLER_TRANSFER,
                reason=reason,
                selected_backend=available[0],
            )
        return FallbackPlan(
            mode=FallbackMode.RECOMPUTE,
            reason=reason,
            selected_backend=None,
        )

    def select_transfer_backend(
        self,
        descriptor: AttentionStateDescriptor,
        preferred_backend: TransferBackend | None,
    ) -> tuple[TransferBackend | None, bool, FallbackPlan | None]:
        available = tuple(path.backend for path in descriptor.transfer_paths)
        if not available:
            return None, False, FallbackPlan(
                mode=FallbackMode.RECOMPUTE,
                reason="descriptor exposes no transfer paths",
                selected_backend=None,
            )

        if preferred_backend is None:
            return available[0], False, None

        if preferred_backend in available:
            return preferred_backend, False, None

        fallback = FallbackPlan(
            mode=FallbackMode.SIMPLER_TRANSFER,
            reason=f"preferred backend {preferred_backend} is unavailable",
            selected_backend=available[0],
        )
        return available[0], True, fallback

    def partial_plan_from_match(self, match: MatchResult) -> PartialHitPlan:
        reusable_tokens = match.requested_key.identity.tokens[: match.matched_extent.units]
        disposition = (
            PlanDisposition.FULL_REUSE if not match.remaining.tokens else PlanDisposition.PARTIAL_REUSE
        )
        return PartialHitPlan(
            disposition=disposition,
            reusable=ReusableSlice(tokens=list(reusable_tokens), source_tier=match.entry.location.tier),
            remaining=RemainingWork(
                tokens=list(match.remaining.tokens),
                fetch_required=match.remaining.fetch_required,
                recompute_required=match.remaining.recompute_required,
            ),
            entry=match.entry,
        )
