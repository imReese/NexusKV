from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Protocol

from nexuskv.contracts.generated import (
    AttentionStateDescriptor,
    CacheEntry,
    CompatibilityFlag,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    KeyIdentity,
    MatchClassification,
    MatchResult,
    PartialHitPlan,
    PolicyHint,
    QueryKey,
    ReuseKey,
    TierKind,
    TransferBackend,
)

if TYPE_CHECKING:
    from nexuskv.execution.runner import BaselineExecutionRunner
    from nexuskv.execution.types import MaterializationOutcome, MaterializationRequest


class LookupStatus(StrEnum):
    HIT = "hit"
    PARTIAL = "partial"
    MISS = "miss"
    UNSUPPORTED = "unsupported"


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
class LookupOutcome:
    query: QueryKey
    status: LookupStatus
    match: MatchResult | None
    partial_plan: PartialHitPlan | None
    reason: str | None = None


@dataclass(slots=True)
class StoreIntent:
    reuse_key: ReuseKey
    entry: CacheEntry


@dataclass(slots=True)
class LifecycleDecision:
    hook: str
    lookup: LookupOutcome
    execution: "MaterializationOutcome"
    should_store_after_stage: bool

    @property
    def materialization(self):
        return self.execution.primary

    @property
    def prefetch(self):
        return self.execution.prefetch

    @property
    def store(self):
        return self.execution.store


class ReusePlanner(Protocol):
    def lookup(self, query: QueryKey) -> MatchResult | None:
        raise NotImplementedError

    def plan_partial_hit(self, query: QueryKey) -> PartialHitPlan | None:
        raise NotImplementedError


class EngineConnector(ABC):
    engine_name: str

    def __init__(self, execution_runner: "BaselineExecutionRunner | None" = None) -> None:
        if execution_runner is None:
            from nexuskv.execution.runner import BaselineExecutionRunner

            execution_runner = BaselineExecutionRunner()
        self.execution_runner = execution_runner

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
            return LookupOutcome(query=query, status=LookupStatus.MISS, match=None, partial_plan=None)
        if match.classification == MatchClassification.EXACT:
            return LookupOutcome(query=query, status=LookupStatus.HIT, match=match, partial_plan=None)
        return LookupOutcome(
            query=query,
            status=LookupStatus.PARTIAL,
            match=match,
            partial_plan=planner.plan_partial_hit(query),
        )

    def unsupported_lookup(self, context: EngineRequestContext, reason: str) -> LookupOutcome:
        return LookupOutcome(
            query=self.build_query_key(context),
            status=LookupStatus.UNSUPPORTED,
            match=None,
            partial_plan=None,
            reason=reason,
        )

    def partial_plan_from_match(self, match: MatchResult) -> PartialHitPlan:
        from nexuskv.contracts.generated import PlanDisposition, RemainingWork, ReusableSlice

        return PartialHitPlan(
            disposition=PlanDisposition.FULL_REUSE if not match.remaining.tokens else PlanDisposition.PARTIAL_REUSE,
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

    def execute_lifecycle(
        self,
        *,
        hook: str,
        context: EngineRequestContext,
        lookup: LookupOutcome,
        allow_store_after_stage: bool,
        enable_prefetch: bool,
    ) -> LifecycleDecision:
        from nexuskv.execution.types import MaterializationRequest

        outcome = self.execution_runner.execute(
            MaterializationRequest(
                hook=hook,
                context=context,
                lookup=lookup,
                preferred_backend=context.preferred_backend,
                allow_store_after_stage=allow_store_after_stage,
                enable_prefetch=enable_prefetch,
            )
        )
        return LifecycleDecision(
            hook=hook,
            lookup=lookup,
            execution=outcome,
            should_store_after_stage=allow_store_after_stage,
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
