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
    "supports_partial_materialization",
    "validate_descriptor",
]
