use nexus_state::{
    AttentionStateDescriptor, CompatibilityFlag, DeviceClass, EngineFamily, Granularity,
    LayoutMetadata, MaterializationCapability, MaterializationProfile, QuantizationMetadata,
    SCHEMA_VERSION, StateSemanticType, TensorRole, TensorSpec, TierKind, TransferBackend,
    TransferCapability, TransferPath,
};
use nxradixtree_core::{
    CacheEntry, EntryIdentity, EntryLocation, EntryVersion, KeyIdentity, PolicyHint, RadixTree,
    ReuseKey,
};
use std::sync::Arc;
use std::thread;

fn base_key(namespace: &str, model: &str, tokens: &[u32]) -> KeyIdentity {
    KeyIdentity {
        tenant: "tenant-a".to_string(),
        namespace: namespace.to_string(),
        model: model.to_string(),
        engine_family: EngineFamily::Vllm,
        semantic_type: StateSemanticType::GqaKv,
        tokens: tokens.to_vec(),
        block_id: None,
        page_id: None,
    }
}

fn base_entry(namespace: &str, model: &str, tokens: &[u32], entry_id: &str) -> CacheEntry {
    CacheEntry {
        identity: EntryIdentity {
            key: base_key(namespace, model, tokens),
            entry_id: entry_id.to_string(),
            version: EntryVersion {
                generation: 1,
                lineage: "lineage-a".to_string(),
            },
        },
        descriptor: default_descriptor(),
        location: EntryLocation {
            tier: TierKind::RemoteShared,
            locator: format!("remote://{entry_id}"),
        },
        policy_hint: PolicyHint {
            reusable: true,
            admission_hint: "default".to_string(),
            eviction_hint: "keep_warm".to_string(),
        },
    }
}

fn default_descriptor() -> AttentionStateDescriptor {
    AttentionStateDescriptor {
        schema_version: SCHEMA_VERSION.to_string(),
        descriptor_id: "vllm-gqa-page".to_string(),
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
            tier_kinds: vec![TierKind::Device, TierKind::RemoteShared],
            device_classes: vec![DeviceClass::Cuda],
            buffer_kinds: vec![],
        },
        layout_metadata: Default::default(),
    }
}

#[test]
fn test_concurrent_insert_and_lookup() {
    let tree = Arc::new(RadixTree::default());
    let mut handles = vec![];

    for t_id in 0..10 {
        let tree_clone = Arc::clone(&tree);
        handles.push(thread::spawn(move || {
            for i in 0..1000 {
                let tokens: Vec<u32> = (0..10).map(|_| (t_id * 1000 + i) as u32).collect();
                let key = ReuseKey {
                    identity: base_key("ns", "model", &tokens),
                };
                let entry = base_entry("ns", "model", &tokens, &format!("entry-{}-{}", t_id, i));

                tree_clone.insert(key, entry);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let stats = tree.stats();
    assert_eq!(stats.total_inserts, 10000);
}
