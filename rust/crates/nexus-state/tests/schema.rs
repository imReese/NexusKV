use nexus_state::{
    AttentionStateDescriptor,
    BufferKind,
    DeviceClass,
    EngineFamily,
    Granularity,
    LayoutMetadata,
    MaterializationCapability,
    MaterializationProfile,
    QuantizationMetadata,
    SCHEMA_VERSION,
    StateSemanticType,
    CompatibilityFlag,
    TensorRole,
    TensorSpec,
    TransferBackend,
    TransferCapability,
    TransferPath,
    validate_descriptor,
};

#[test]
fn schema_version_constant_is_stable() {
    assert_eq!(SCHEMA_VERSION, "nexuskv.contract.v1");
}

#[test]
fn descriptor_supports_transfer_and_materialization_contracts() {
    let descriptor = AttentionStateDescriptor {
        schema_version: SCHEMA_VERSION.to_string(),
        descriptor_id: "sglang-mha-token".to_string(),
        engine_family: EngineFamily::Sglang,
        semantic_type: StateSemanticType::MhaKv,
        granularity: Granularity::Token,
        tensor_specs: vec![
            TensorSpec {
                name: "k_cache".to_string(),
                role: TensorRole::Key,
                dtype: "float16".to_string(),
                shape: vec!["layers".to_string(), "tokens".to_string()],
            },
            TensorSpec {
                name: "v_cache".to_string(),
                role: TensorRole::Value,
                dtype: "float16".to_string(),
                shape: vec!["layers".to_string(), "tokens".to_string()],
            },
        ],
        quantization: QuantizationMetadata {
            scheme: "none".to_string(),
            bits: 16,
            group_size: 0,
        },
        layout: LayoutMetadata {
            layout: "contiguous".to_string(),
            page_tokens: 1,
            block_tokens: 1,
            packed: false,
        },
        compatibility_flags: vec![CompatibilityFlag::ExactReuse],
        transfer_paths: vec![TransferPath {
            backend: TransferBackend::BaselineTransport,
            capabilities: vec![TransferCapability::HostToStore],
        }],
        materialization: MaterializationProfile {
            capabilities: vec![MaterializationCapability::Full],
            tier_kinds: vec![nexus_state::TierKind::HostDram],
            device_classes: vec![DeviceClass::Cpu],
            buffer_kinds: vec![BufferKind::HostPageable],
        },
        layout_metadata: Default::default(),
    };

    assert!(validate_descriptor(&descriptor).is_ok());
}
