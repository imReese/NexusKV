from __future__ import annotations

from nexuskv.connectors.base import LookupOutcome, LookupStatus, SGLangLifecycleContext, VLLMLifecycleContext
from nexuskv.contracts.generated import (
    CacheEntry,
    CompatibilitySignal,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    KeyIdentity,
    MatchClassification,
    MatchExtent,
    MatchResult,
    RemainingWork,
    ReuseKey,
    TierKind,
)


def make_match(connector, tokens: list[int], remaining: list[int], *, page_id: int | None = None) -> MatchResult:
    descriptor = connector.default_descriptor()
    key = KeyIdentity(
        tenant="tenant-a",
        namespace="chat",
        model="llama-70b",
        engine_family=descriptor.engine_family,
        semantic_type=descriptor.semantic_type,
        tokens=tokens,
        block_id=None,
        page_id=page_id,
    )
    context = (
        SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=tokens,
            descriptor=descriptor,
        )
        if connector.engine_name == "sglang"
        else VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=tokens,
            descriptor=descriptor,
            page_id=page_id,
        )
    )
    policy_hint = connector.prepare_store(
        context,
        entry_id="entry-1",
        locator="remote://entry-1",
        tier=TierKind.REMOTE_SHARED,
    ).entry.policy_hint

    entry = CacheEntry(
        identity=EntryIdentity(
            key=key,
            entry_id="entry-1",
            version=EntryVersion(generation=1, lineage="lineage-a"),
        ),
        descriptor=descriptor,
        location=EntryLocation(tier=TierKind.REMOTE_SHARED, locator="remote://entry-1"),
        policy_hint=policy_hint,
    )
    classification = MatchClassification.EXACT if not remaining else MatchClassification.PARTIAL
    return MatchResult(
        classification=classification,
        matched_key=ReuseKey(identity=key),
        requested_key=connector.build_query_key(
            SGLangLifecycleContext(
                tenant="tenant-a",
                namespace="chat",
                model="llama-70b",
                tokens=tokens + remaining,
                descriptor=descriptor,
            )
            if connector.engine_name == "sglang"
            else VLLMLifecycleContext(
                tenant="tenant-a",
                namespace="chat",
                model="llama-70b",
                tokens=tokens + remaining,
                descriptor=descriptor,
                page_id=page_id,
            )
        ),
        matched_extent=MatchExtent(units=len(tokens), granularity=descriptor.granularity),
        entry=entry,
        remaining=RemainingWork(
            tokens=remaining,
            fetch_required=bool(remaining),
            recompute_required=bool(remaining),
        ),
        compatibility=CompatibilitySignal(
            reusable=True,
            fallback_to_recompute=False,
            reason="",
        ),
    )


def make_lookup_outcome(connector, tokens: list[int], remaining: list[int], *, page_id: int | None = None) -> LookupOutcome:
    match = make_match(connector, tokens, remaining, page_id=page_id)
    partial_plan = connector.partial_plan_from_match(match) if remaining else None
    requested_context = (
        SGLangLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=tokens + remaining,
            descriptor=connector.default_descriptor(),
        )
        if connector.engine_name == "sglang"
        else VLLMLifecycleContext(
            tenant="tenant-a",
            namespace="chat",
            model="llama-70b",
            tokens=tokens + remaining,
            descriptor=connector.default_descriptor(),
            page_id=page_id,
        )
    )
    return LookupOutcome(
        query=connector.build_query_key(requested_context),
        status=LookupStatus.HIT if not remaining else LookupStatus.PARTIAL,
        match=match,
        partial_plan=partial_plan,
    )
