from __future__ import annotations

from nexuskv.adapters.state import (
    AttentionStateDescriptor,
    BufferKind,
    CompatibilityFlag,
    DeviceClass,
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
    ReusePlanner,
    TransferBackend,
    VLLMLifecycleContext,
)


class VLLMConnector(EngineConnector):
    engine_name = "vllm"

    def supported_hooks(self) -> tuple[str, ...]:
        return ("request_start", "block_table_extend", "decode_step", "prefetch")

    def default_descriptor(self) -> AttentionStateDescriptor:
        return AttentionStateDescriptor(
            schema_version=SCHEMA_VERSION,
            descriptor_id="vllm-gqa-page",
            semantic_type=StateSemanticType.GQA_KV,
            engine_family=EngineFamily.VLLM,
            granularity=Granularity.PAGE,
            tensor_specs=[
                TensorSpec(
                    name="k_cache",
                    role=TensorRole.KEY,
                    dtype="float16",
                    shape=["layers", "pages", "kv_heads", "page_tokens", "head_dim"],
                ),
                TensorSpec(
                    name="v_cache",
                    role=TensorRole.VALUE,
                    dtype="float16",
                    shape=["layers", "pages", "kv_heads", "page_tokens", "head_dim"],
                ),
            ],
            quantization=QuantizationMetadata(scheme="none", bits=16, group_size=0),
            layout=LayoutMetadata(layout="paged", page_tokens=16, block_tokens=16, packed=True),
            compatibility_flags=[
                CompatibilityFlag.PREFIX_REUSE,
                CompatibilityFlag.PAGE_REUSE,
            ],
            transfer_paths=[
                TransferPath(
                    backend=TransferBackend.STAGED_COPY,
                    capabilities=[
                        TransferCapability.ASYNC,
                        TransferCapability.DEVICE_TO_HOST,
                        TransferCapability.HOST_TO_DEVICE,
                    ],
                )
            ],
            materialization=MaterializationProfile(
                capabilities=[
                    MaterializationCapability.FULL,
                    MaterializationCapability.PARTIAL,
                    MaterializationCapability.PREFETCH,
                    MaterializationCapability.FALLBACK_RECOMPUTE,
                ],
                tier_kinds=[TierKind.DEVICE, TierKind.HOST_DRAM, TierKind.REMOTE_SHARED],
                device_classes=[DeviceClass.CUDA, DeviceClass.CPU],
                buffer_kinds=[BufferKind.DEVICE, BufferKind.HOST_PINNED],
            ),
            layout_metadata={"engine_layout": "paged_kv"},
        )

    def probe_capabilities(self, descriptor: AttentionStateDescriptor | None = None) -> ConnectorCapabilities:
        descriptor = descriptor or self.default_descriptor()
        return ConnectorCapabilities(
            exact_reuse=True,
            prefix_reuse=True,
            partial_materialization=True,
            prefetch=True,
            decode_lookup=True,
            supported_hooks=self.supported_hooks(),
            supported_transfer_backends=tuple(path.backend for path in descriptor.transfer_paths),
            compatibility_flags=tuple(descriptor.compatibility_flags),
        )

    def on_request_start(self, context: VLLMLifecycleContext, planner: ReusePlanner) -> LifecycleDecision:
        lookup = self.lookup(context, planner)
        materialization = self._materialize_lookup(context, lookup)
        prefetch = self.prefetch(context, planner) if lookup.status == LookupStatus.PARTIAL else None
        return LifecycleDecision(
            hook="request_start",
            lookup=lookup,
            materialization=materialization,
            prefetch=prefetch,
            should_store_after_stage=lookup.status != LookupStatus.HIT,
        )

    def on_block_table_extend(self, context: VLLMLifecycleContext, planner: ReusePlanner) -> LifecycleDecision:
        lookup = self.lookup(context, planner)
        materialization = self._materialize_lookup(context, lookup)
        prefetch = self.prefetch(context, planner)
        return LifecycleDecision(
            hook="block_table_extend",
            lookup=lookup,
            materialization=materialization,
            prefetch=prefetch,
            should_store_after_stage=lookup.status != LookupStatus.HIT,
        )

    def on_decode_step(self, context: VLLMLifecycleContext, planner: ReusePlanner) -> LifecycleDecision:
        lookup = self.lookup(context, planner)
        materialization = self._materialize_lookup(context, lookup) if lookup.status != LookupStatus.MISS else None
        prefetch = self.prefetch(context, planner)
        return LifecycleDecision(
            hook="decode_step",
            lookup=lookup,
            materialization=materialization,
            prefetch=prefetch,
            should_store_after_stage=False,
        )

    def _materialize_lookup(self, context: VLLMLifecycleContext, lookup):
        if lookup.status == LookupStatus.HIT and lookup.match is not None:
            return self.materialize(
                context.descriptor,
                match=lookup.match,
                preferred_backend=context.preferred_backend or TransferBackend.STAGED_COPY,
            )
        if lookup.status == LookupStatus.PARTIAL:
            return self.materialize(
                context.descriptor,
                partial_plan=lookup.partial_plan or (lookup.match and self.partial_plan_from_match(lookup.match)),
                preferred_backend=context.preferred_backend or TransferBackend.STAGED_COPY,
            )
        return None
