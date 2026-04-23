use nxradixtree_core::{
    CacheEntry,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    KeyIdentity,
    MatchClassification,
    PolicyHint,
    partial_hit_plan_from_match,
    query_key,
    reuse_key,
    RadixTree,
};
use nexus_state::{
    AttentionStateDescriptor,
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
};

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
fn exact_lookup_returns_rich_match_result() {
    let mut tree = RadixTree::default();
    let entry = base_entry("chat", "llama-70b", &[1, 2, 3], "entry-exact");
    tree.insert(reuse_key(entry.identity.key.clone()), entry.clone());

    let hit = tree
        .lookup(&query_key(base_key("chat", "llama-70b", &[1, 2, 3])))
        .expect("exact hit");

    assert_eq!(hit.classification, MatchClassification::Exact);
    assert_eq!(hit.matched_extent.units, 3);
    assert_eq!(hit.entry.identity.entry_id, "entry-exact");
    assert_eq!(hit.entry.location.tier, TierKind::RemoteShared);
}

#[test]
fn longest_prefix_returns_partial_plan_for_remaining_tokens() {
    let mut tree = RadixTree::default();
    tree.insert(
        reuse_key(base_key("chat", "llama-70b", &[10, 11, 12])),
        base_entry("chat", "llama-70b", &[10, 11, 12], "entry-prefix"),
    );

    let hit = tree
        .lookup(&query_key(base_key("chat", "llama-70b", &[10, 11, 12, 13, 14])))
        .expect("prefix hit");

    assert_eq!(hit.classification, MatchClassification::Partial);
    assert_eq!(hit.matched_extent.units, 3);
    assert_eq!(hit.remaining.tokens, vec![13, 14]);

    let plan = partial_hit_plan_from_match(&hit);
    assert_eq!(plan.reusable.tokens, vec![10, 11, 12]);
    assert_eq!(plan.remaining.tokens, vec![13, 14]);
}

#[test]
fn planner_boundary_returns_partial_reuse_disposition() {
    let mut tree = RadixTree::default();
    tree.insert(
        reuse_key(base_key("chat", "llama-70b", &[30, 31])),
        base_entry("chat", "llama-70b", &[30, 31], "entry-plan"),
    );

    let plan = tree
        .plan_partial_hit(&query_key(base_key("chat", "llama-70b", &[30, 31, 32])))
        .expect("plan");

    assert_eq!(plan.disposition, nxradixtree_core::PlanDisposition::PartialReuse);
    assert_eq!(plan.reusable.source_tier, TierKind::RemoteShared);
    assert_eq!(plan.remaining.tokens, vec![32]);
}

#[test]
fn identity_boundaries_isolate_namespace_and_model() {
    let mut tree = RadixTree::default();
    tree.insert(
        reuse_key(base_key("chat", "llama-70b", &[1, 2, 3])),
        base_entry("chat", "llama-70b", &[1, 2, 3], "entry-chat"),
    );

    let wrong_namespace = tree.lookup(&query_key(base_key("system", "llama-70b", &[1, 2, 3])));
    let wrong_model = tree.lookup(&query_key(base_key("chat", "mixtral-8x7b", &[1, 2, 3])));

    assert!(wrong_namespace.is_none());
    assert!(wrong_model.is_none());
}

#[test]
fn deterministic_tie_behavior_prefers_longest_prefix() {
    let mut tree = RadixTree::default();
    tree.insert(
        reuse_key(base_key("chat", "llama-70b", &[5, 6])),
        base_entry("chat", "llama-70b", &[5, 6], "entry-short"),
    );
    tree.insert(
        reuse_key(base_key("chat", "llama-70b", &[5, 6, 7])),
        base_entry("chat", "llama-70b", &[5, 6, 7], "entry-long"),
    );

    let hit = tree
        .lookup(&query_key(base_key("chat", "llama-70b", &[5, 6, 7, 8])))
        .expect("prefix hit");

    assert_eq!(hit.entry.identity.entry_id, "entry-long");
    assert_eq!(hit.classification, MatchClassification::Partial);
    assert_eq!(hit.remaining.tokens, vec![8]);
}

#[test]
fn page_identity_participates_in_key_stability() {
    let mut tree = RadixTree::default();
    let mut page_key = base_key("chat", "llama-70b", &[20, 21, 22]);
    page_key.page_id = Some(7);

    let mut other_page_key = base_key("chat", "llama-70b", &[20, 21, 22]);
    other_page_key.page_id = Some(8);

    tree.insert(
        reuse_key(page_key.clone()),
        CacheEntry {
            identity: EntryIdentity {
                key: page_key.clone(),
                entry_id: "entry-page-7".to_string(),
                version: EntryVersion {
                    generation: 2,
                    lineage: "lineage-b".to_string(),
                },
            },
            descriptor: default_descriptor(),
            location: EntryLocation {
                tier: TierKind::Device,
                locator: "device://page/7".to_string(),
            },
            policy_hint: PolicyHint {
                reusable: true,
                admission_hint: "page".to_string(),
                eviction_hint: "protect".to_string(),
            },
        },
    );

    assert!(tree.lookup(&query_key(other_page_key)).is_none());
}
