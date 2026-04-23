use nexus_state::{
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
    supports_partial_materialization,
    validate_descriptor,
};

#[test]
fn descriptor_rejects_duplicate_roles() {
    let result = validate_descriptor(&AttentionStateDescriptor {
        schema_version: SCHEMA_VERSION.to_string(),
        descriptor_id: "duplicate-role".to_string(),
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
                name: "v_cache_duplicate".to_string(),
                role: TensorRole::Key,
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
            tier_kinds: vec![TierKind::HostDram],
            device_classes: vec![],
            buffer_kinds: vec![],
        },
        layout_metadata: Default::default(),
    });

    assert!(result.is_err());
}

#[test]
fn descriptor_reports_partial_materialization_for_page_granularity() {
    let descriptor = AttentionStateDescriptor {
        schema_version: SCHEMA_VERSION.to_string(),
        descriptor_id: "paged-gqa".to_string(),
        engine_family: EngineFamily::Vllm,
        semantic_type: StateSemanticType::GqaKv,
        granularity: Granularity::Page,
        tensor_specs: vec![
            TensorSpec {
                name: "k_cache".to_string(),
                role: TensorRole::Key,
                dtype: "float16".to_string(),
                shape: vec!["layers".to_string(), "pages".to_string()],
            },
            TensorSpec {
                name: "v_cache".to_string(),
                role: TensorRole::Value,
                dtype: "float16".to_string(),
                shape: vec!["layers".to_string(), "pages".to_string()],
            },
        ],
        quantization: QuantizationMetadata {
            scheme: "none".to_string(),
            bits: 16,
            group_size: 0,
        },
        layout: LayoutMetadata {
            layout: "paged".to_string(),
            page_tokens: 16,
            block_tokens: 16,
            packed: true,
        },
        compatibility_flags: vec![CompatibilityFlag::PageReuse],
        transfer_paths: vec![TransferPath {
            backend: TransferBackend::StagedCopy,
            capabilities: vec![TransferCapability::Async],
        }],
        materialization: MaterializationProfile {
            capabilities: vec![MaterializationCapability::Partial],
            tier_kinds: vec![TierKind::Device],
            device_classes: vec![DeviceClass::Cuda],
            buffer_kinds: vec![BufferKind::Device],
        },
        layout_metadata: Default::default(),
    };

    assert!(supports_partial_materialization(&descriptor));
}
