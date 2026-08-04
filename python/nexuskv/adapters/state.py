from __future__ import annotations

from nexuskv.contracts.generated import (
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


class DescriptorValidationError(ValueError):
    pass


def validate_descriptor(descriptor: AttentionStateDescriptor) -> None:
    if descriptor.schema_version != SCHEMA_VERSION:
        raise DescriptorValidationError(
            f"unsupported schema version: {descriptor.schema_version}"
        )
    if not descriptor.tensor_specs:
        raise DescriptorValidationError("tensor_specs must not be empty")

    seen_roles: set[TensorRole] = set()
    for spec in descriptor.tensor_specs:
        if spec.role in seen_roles:
            raise DescriptorValidationError(f"duplicate tensor role: {spec.role}")
        seen_roles.add(spec.role)


def supports_partial_materialization(descriptor: AttentionStateDescriptor) -> bool:
    return (
        MaterializationCapability.PARTIAL in descriptor.materialization.capabilities
        or descriptor.granularity in {Granularity.BLOCK, Granularity.PAGE, Granularity.SEGMENT}
    )


def create_mla_descriptor(descriptor_id: str, engine_family: EngineFamily = EngineFamily.SGLANG) -> AttentionStateDescriptor:
    """DeepSeek Multi-head Latent Attention (MLA) state descriptor."""
    return AttentionStateDescriptor(
        schema_version=SCHEMA_VERSION,
        descriptor_id=descriptor_id,
        engine_family=engine_family,
        semantic_type=StateSemanticType.MLA_STATE,
        granularity=Granularity.PAGE,
        tensor_specs=[
            TensorSpec(name="c_kv", role=TensorRole.LATENT, dtype="bfloat16", shape=["num_pages", "page_size", "512"]),
            TensorSpec(name="k_pe", role=TensorRole.POSITION, dtype="bfloat16", shape=["num_pages", "page_size", "64"]),
        ],
        quantization=QuantizationMetadata(scheme="fp8", bits=8, group_size=128),
        layout=LayoutMetadata(layout="page_paged", page_tokens=16, block_tokens=16, packed=True),
        compatibility_flags=[CompatibilityFlag.EXACT_REUSE, CompatibilityFlag.PAGE_REUSE],
        transfer_paths=[TransferPath(backend=TransferBackend.STAGED_COPY, capabilities=[TransferCapability.HOST_TO_DEVICE])],
        materialization=MaterializationProfile(
            capabilities=[MaterializationCapability.FULL, MaterializationCapability.PARTIAL, MaterializationCapability.PREFETCH],
            tier_kinds=[TierKind.DEVICE, TierKind.HOST_DRAM],
            device_classes=[DeviceClass.CUDA],
            buffer_kinds=[BufferKind.DEVICE, BufferKind.HOST_PINNED],
        ),
        layout_metadata={"attention_family": "MLA", "latent_dim": "512", "pe_dim": "64"},
    )


def create_dsa_descriptor(descriptor_id: str, engine_family: EngineFamily = EngineFamily.SGLANG) -> AttentionStateDescriptor:
    """DeepSeek Sparse Attention (DSA) state descriptor."""
    return AttentionStateDescriptor(
        schema_version=SCHEMA_VERSION,
        descriptor_id=descriptor_id,
        engine_family=engine_family,
        semantic_type=StateSemanticType.DSA_STATE,
        granularity=Granularity.PAGE,
        tensor_specs=[
            TensorSpec(name="kv_sparse", role=TensorRole.KEY, dtype="bfloat16", shape=["num_pages", "page_size", "128"]),
            TensorSpec(name="selector_aux", role=TensorRole.AUXILIARY, dtype="int32", shape=["num_pages", "top_k"]),
        ],
        quantization=QuantizationMetadata(scheme="none", bits=16, group_size=0),
        layout=LayoutMetadata(layout="sparse_indexed", page_tokens=16, block_tokens=16, packed=False),
        compatibility_flags=[CompatibilityFlag.EXACT_REUSE, CompatibilityFlag.PAGE_REUSE],
        transfer_paths=[TransferPath(backend=TransferBackend.BASELINE_TRANSPORT, capabilities=[TransferCapability.HOST_TO_DEVICE])],
        materialization=MaterializationProfile(
            capabilities=[MaterializationCapability.FULL, MaterializationCapability.PARTIAL],
            tier_kinds=[TierKind.DEVICE, TierKind.HOST_DRAM],
            device_classes=[DeviceClass.CUDA],
            buffer_kinds=[BufferKind.DEVICE, BufferKind.HOST_PINNED],
        ),
        layout_metadata={"attention_family": "DSA", "selection_top_k": "64"},
    )


def create_kda_descriptor(descriptor_id: str, engine_family: EngineFamily = EngineFamily.VLLM) -> AttentionStateDescriptor:
    """Kimi Delta Attention (KDA) recurrent checkpoint descriptor."""
    return AttentionStateDescriptor(
        schema_version=SCHEMA_VERSION,
        descriptor_id=descriptor_id,
        engine_family=engine_family,
        semantic_type=StateSemanticType.KDA_CHECKPOINT,
        granularity=Granularity.SEGMENT,
        tensor_specs=[
            TensorSpec(name="h_recurrent", role=TensorRole.LATENT, dtype="float32", shape=["num_layers", "hidden_dim"]),
        ],
        quantization=QuantizationMetadata(scheme="none", bits=32, group_size=0),
        layout=LayoutMetadata(layout="recurrent_checkpoint", page_tokens=0, block_tokens=0, packed=True),
        compatibility_flags=[CompatibilityFlag.EXACT_REUSE, CompatibilityFlag.WARM_START],
        transfer_paths=[TransferPath(backend=TransferBackend.BASELINE_TRANSPORT, capabilities=[TransferCapability.HOST_TO_DEVICE])],
        materialization=MaterializationProfile(
            capabilities=[MaterializationCapability.FULL, MaterializationCapability.FALLBACK_RECOMPUTE],
            tier_kinds=[TierKind.DEVICE, TierKind.HOST_DRAM],
            device_classes=[DeviceClass.CUDA],
            buffer_kinds=[BufferKind.DEVICE, BufferKind.HOST_PINNED],
        ),
        layout_metadata={"attention_family": "KDA", "checkpoint_boundary": "terminal_recurrent_state"},
    )


def create_csa_descriptor(descriptor_id: str, engine_family: EngineFamily = EngineFamily.VLLM) -> AttentionStateDescriptor:
    """DeepSeek V4 Compressed Sparse Attention (CSA) descriptor (4-token group FP4 Top-K)."""
    return AttentionStateDescriptor(
        schema_version=SCHEMA_VERSION,
        descriptor_id=descriptor_id,
        engine_family=engine_family,
        semantic_type=StateSemanticType.CSA_STATE,
        granularity=Granularity.BLOCK,
        tensor_specs=[
            TensorSpec(name="csa_compressed_kv", role=TensorRole.LATENT, dtype="fp4", shape=["num_layers", "blocks", "dim"]),
            TensorSpec(name="csa_topk_indices", role=TensorRole.AUXILIARY, dtype="int32", shape=["num_layers", "topk"]),
        ],
        quantization=QuantizationMetadata(scheme="fp4", bits=4, group_size=4),
        layout=LayoutMetadata(layout="csa_4token_sparse", page_tokens=0, block_tokens=4, packed=True),
        compatibility_flags=[CompatibilityFlag.EXACT_REUSE, CompatibilityFlag.PREFIX_REUSE, CompatibilityFlag.BLOCK_REUSE],
        transfer_paths=[TransferPath(backend=TransferBackend.ZERO_COPY, capabilities=[TransferCapability.ASYNC, TransferCapability.ZERO_COPY_CANDIDATE])],
        materialization=MaterializationProfile(
            capabilities=[MaterializationCapability.FULL, MaterializationCapability.PARTIAL, MaterializationCapability.FALLBACK_RECOMPUTE],
            tier_kinds=[TierKind.DEVICE, TierKind.HOST_DRAM],
            device_classes=[DeviceClass.CUDA],
            buffer_kinds=[BufferKind.DEVICE, BufferKind.HOST_PINNED],
        ),
        layout_metadata={"attention_family": "CSA", "group_tokens": 4},
    )


def create_hca_descriptor(descriptor_id: str, engine_family: EngineFamily = EngineFamily.VLLM) -> AttentionStateDescriptor:
    """DeepSeek V4 Heavily Compressed Attention (HCA) 128-token global summary descriptor."""
    return AttentionStateDescriptor(
        schema_version=SCHEMA_VERSION,
        descriptor_id=descriptor_id,
        engine_family=engine_family,
        semantic_type=StateSemanticType.HCA_SUMMARY,
        granularity=Granularity.SEGMENT,
        tensor_specs=[
            TensorSpec(name="hca_summary_vec", role=TensorRole.LATENT, dtype="float16", shape=["num_layers", "summary_dim"]),
        ],
        quantization=QuantizationMetadata(scheme="none", bits=16, group_size=0),
        layout=LayoutMetadata(layout="hca_128token_summary", page_tokens=0, block_tokens=128, packed=True),
        compatibility_flags=[CompatibilityFlag.EXACT_REUSE, CompatibilityFlag.PREFIX_REUSE, CompatibilityFlag.WARM_START],
        transfer_paths=[TransferPath(backend=TransferBackend.ZERO_COPY, capabilities=[TransferCapability.ASYNC])],
        materialization=MaterializationProfile(
            capabilities=[MaterializationCapability.FULL, MaterializationCapability.FALLBACK_RECOMPUTE],
            tier_kinds=[TierKind.DEVICE, TierKind.HOST_DRAM],
            device_classes=[DeviceClass.CUDA],
            buffer_kinds=[BufferKind.DEVICE, BufferKind.HOST_PINNED],
        ),
        layout_metadata={"attention_family": "HCA", "summary_tokens": 128},
    )


def create_dspark_descriptor(descriptor_id: str, engine_family: EngineFamily = EngineFamily.SGLANG) -> AttentionStateDescriptor:
    """Distributed Spark Attention (DSpark) sparse sharded descriptor."""
    return AttentionStateDescriptor(
        schema_version=SCHEMA_VERSION,
        descriptor_id=descriptor_id,
        engine_family=engine_family,
        semantic_type=StateSemanticType.DSPARK_SPARSE,
        granularity=Granularity.PAGE,
        tensor_specs=[
            TensorSpec(name="spark_shards", role=TensorRole.KEY, dtype="float16", shape=["num_shards", "page_tokens", "dim"]),
        ],
        quantization=QuantizationMetadata(scheme="none", bits=16, group_size=0),
        layout=LayoutMetadata(layout="dspark_sharded_page", page_tokens=16, block_tokens=16, packed=False),
        compatibility_flags=[CompatibilityFlag.PAGE_REUSE, CompatibilityFlag.PREFIX_REUSE],
        transfer_paths=[TransferPath(backend=TransferBackend.RDMA, capabilities=[TransferCapability.ASYNC])],
        materialization=MaterializationProfile(
            capabilities=[MaterializationCapability.FULL, MaterializationCapability.PARTIAL, MaterializationCapability.FALLBACK_RECOMPUTE],
            tier_kinds=[TierKind.DEVICE, TierKind.HOST_DRAM, TierKind.REMOTE_SHARED],
            device_classes=[DeviceClass.CUDA],
            buffer_kinds=[BufferKind.DEVICE, BufferKind.HOST_PINNED, BufferKind.REMOTE],
        ),
        layout_metadata={"attention_family": "DSPARK", "sharded": True},
    )


__all__ = [
    "AttentionStateDescriptor",
    "BufferKind",
    "CompatibilityFlag",
    "DescriptorValidationError",
    "DeviceClass",
    "EngineFamily",
    "Granularity",
    "LayoutMetadata",
    "MaterializationCapability",
    "MaterializationProfile",
    "QuantizationMetadata",
    "SCHEMA_VERSION",
    "StateSemanticType",
    "TensorRole",
    "TensorSpec",
    "TierKind",
    "TransferBackend",
    "TransferCapability",
    "TransferPath",
    "create_csa_descriptor",
    "create_dsa_descriptor",
    "create_dspark_descriptor",
    "create_hca_descriptor",
    "create_kda_descriptor",
    "create_mla_descriptor",
    "supports_partial_materialization",
    "validate_descriptor",
]
