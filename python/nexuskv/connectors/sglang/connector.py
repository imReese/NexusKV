from __future__ import annotations

from nexuskv.adapters.state import (
    AttentionStateDescriptor,
    CompatibilityFlag,
    EngineFamily,
    Granularity,
    LayoutMetadata,
    MaterializationCapability,
    MaterializationProfile,
    QuantizationMetadata,
    SCHEMA_VERSION,
    StateSemanticType,
    TensorRole,
    TensorSpec,
    TierKind,
    TransferBackend,
    TransferCapability,
    TransferPath,
)
from nexuskv.connectors.base import (
    ConnectorCapabilities,
    EngineConnector,
    LifecycleDecision,
    LookupStatus,
    MaterializationDecision,
    SGLangLifecycleContext,
    TransferBackend,
    ReusePlanner,
)


class SGLangConnector(EngineConnector):
    engine_name = "sglang"

    def supported_hooks(self) -> tuple[str, ...]:
        return ("prefill", "decode", "evict")

    def default_descriptor(self) -> AttentionStateDescriptor:
        return AttentionStateDescriptor(
            schema_version=SCHEMA_VERSION,
            descriptor_id="sglang-mha-token",
            semantic_type=StateSemanticType.MHA_KV,
            engine_family=EngineFamily.SGLANG,
            granularity=Granularity.TOKEN,
            tensor_specs=[
                TensorSpec(
                    name="k_cache",
                    role=TensorRole.KEY,
                    dtype="float16",
                    shape=["layers", "tokens", "heads", "head_dim"],
                ),
                TensorSpec(
                    name="v_cache",
                    role=TensorRole.VALUE,
                    dtype="float16",
                    shape=["layers", "tokens", "heads", "head_dim"],
                ),
            ],
            quantization=QuantizationMetadata(scheme="none", bits=16, group_size=0),
            layout=LayoutMetadata(layout="contiguous", page_tokens=1, block_tokens=1, packed=False),
            compatibility_flags=[CompatibilityFlag.EXACT_REUSE],
            transfer_paths=[
                TransferPath(
                    backend=TransferBackend.BASELINE_TRANSPORT,
                    capabilities=[
                        TransferCapability.HOST_TO_STORE,
                        TransferCapability.STORE_TO_HOST,
                    ],
                )
            ],
            materialization=MaterializationProfile(
                capabilities=[
                    MaterializationCapability.FULL,
                    MaterializationCapability.FALLBACK_RECOMPUTE,
                ],
                tier_kinds=[TierKind.HOST_DRAM, TierKind.REMOTE_SHARED],
                device_classes=[],
                buffer_kinds=[],
            ),
            layout_metadata={"engine_layout": "token_major"},
        )

    def probe_capabilities(self, descriptor: AttentionStateDescriptor | None = None) -> ConnectorCapabilities:
        descriptor = descriptor or self.default_descriptor()
        return ConnectorCapabilities(
            exact_reuse=True,
            prefix_reuse=True,
            partial_materialization=False,
            prefetch=False,
            decode_lookup=False,
            supported_hooks=self.supported_hooks(),
            supported_transfer_backends=tuple(path.backend for path in descriptor.transfer_paths),
            compatibility_flags=tuple(descriptor.compatibility_flags),
        )

    def on_prefill(self, context: SGLangLifecycleContext, planner: ReusePlanner) -> LifecycleDecision:
        lookup = self.lookup(context, planner)
        materialization = self._materialize_for_prefill_or_extend(context, lookup)
        return LifecycleDecision(
            hook="prefill",
            lookup=lookup,
            materialization=materialization,
            prefetch=None,
            should_store_after_stage=lookup.status != LookupStatus.HIT,
        )

    def on_extend(self, context: SGLangLifecycleContext, planner: ReusePlanner) -> LifecycleDecision:
        lookup = self.lookup(context, planner)
        materialization = self._materialize_for_prefill_or_extend(context, lookup)
        return LifecycleDecision(
            hook="extend",
            lookup=lookup,
            materialization=materialization,
            prefetch=None,
            should_store_after_stage=lookup.status != LookupStatus.HIT,
        )

    def on_decode(self, context: SGLangLifecycleContext, planner: ReusePlanner) -> LifecycleDecision:
        return LifecycleDecision(
            hook="decode",
            lookup=self.unsupported_lookup(
                context,
                "sglang decode stays on local state and does not issue planner lookups in v1",
            ),
            materialization=None,
            prefetch=None,
            should_store_after_stage=False,
        )

    def _materialize_for_prefill_or_extend(
        self,
        context: SGLangLifecycleContext,
        lookup,
    ) -> MaterializationDecision | None:
        if lookup.status == LookupStatus.HIT and lookup.match is not None:
            return self.materialize(
                context.descriptor,
                match=lookup.match,
                preferred_backend=context.preferred_backend or TransferBackend.BASELINE_TRANSPORT,
            )

        if lookup.status == LookupStatus.PARTIAL:
            return self.materialize(
                context.descriptor,
                partial_plan=lookup.partial_plan or (lookup.match and self.partial_plan_from_match(lookup.match)),
                preferred_backend=context.preferred_backend or TransferBackend.BASELINE_TRANSPORT,
            )

        return None
