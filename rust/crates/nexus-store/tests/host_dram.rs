use nexus_state::{EngineFamily, EntryIdentity, EntryVersion, KeyIdentity, StateSemanticType};
use nexus_store::{HostDramKvStore, HostDramStoreError};

fn identity(tenant: &str, entry_id: &str, token: u32) -> EntryIdentity {
    EntryIdentity {
        key: KeyIdentity {
            tenant: tenant.to_string(),
            namespace: "chat".to_string(),
            model: "Qwen3-0.6B".to_string(),
            engine_family: EngineFamily::Sglang,
            semantic_type: StateSemanticType::GqaKv,
            tokens: vec![token],
            block_id: Some(token),
            page_id: None,
        },
        entry_id: entry_id.to_string(),
        version: EntryVersion {
            generation: 1,
            lineage: "main".to_string(),
        },
    }
}

#[test]
fn stores_and_returns_real_payload_bytes() {
    let mut store = HostDramKvStore::new(16).expect("valid capacity");
    let key = identity("tenant-a", "entry-a", 1);

    let outcome = store
        .put(key.clone(), vec![1, 2, 3, 4])
        .expect("payload fits");

    assert!(!outcome.replaced);
    assert!(outcome.evicted.is_empty());
    assert_eq!(store.get(&key), Some([1, 2, 3, 4].as_slice()));
    assert_eq!(store.stats().resident_bytes, 4);
    assert_eq!(store.stats().entry_count, 1);
}

#[test]
fn replacement_updates_resident_bytes_without_counting_an_eviction() {
    let mut store = HostDramKvStore::new(16).expect("valid capacity");
    let key = identity("tenant-a", "entry-a", 1);
    store.put(key.clone(), vec![1, 2]).expect("first payload");

    let outcome = store
        .put(key.clone(), vec![3, 4, 5, 6, 7])
        .expect("replacement payload");

    assert!(outcome.replaced);
    assert!(outcome.evicted.is_empty());
    assert_eq!(store.peek(&key), Some([3, 4, 5, 6, 7].as_slice()));
    assert_eq!(store.stats().resident_bytes, 5);
    assert_eq!(store.stats().eviction_count, 0);
}

#[test]
fn access_promotes_an_entry_before_capacity_eviction() {
    let mut store = HostDramKvStore::new(6).expect("valid capacity");
    let first = identity("tenant-a", "entry-a", 1);
    let second = identity("tenant-a", "entry-b", 2);
    let third = identity("tenant-a", "entry-c", 3);
    store.put(first.clone(), vec![1; 3]).expect("first payload");
    store
        .put(second.clone(), vec![2; 3])
        .expect("second payload");
    assert_eq!(store.get(&first), Some([1; 3].as_slice()));

    let outcome = store.put(third.clone(), vec![3; 3]).expect("third payload");

    assert_eq!(outcome.evicted, vec![second.clone()]);
    assert_eq!(store.peek(&second), None);
    assert_eq!(store.peek(&first), Some([1; 3].as_slice()));
    assert_eq!(store.peek(&third), Some([3; 3].as_slice()));
    assert_eq!(store.stats().resident_bytes, 6);
    assert_eq!(store.stats().eviction_count, 1);
}

#[test]
fn full_entry_identity_isolates_tenants_even_when_entry_ids_match() {
    let mut store = HostDramKvStore::new(8).expect("valid capacity");
    let tenant_a = identity("tenant-a", "shared-id", 1);
    let tenant_b = identity("tenant-b", "shared-id", 1);

    store
        .put(tenant_a.clone(), vec![1, 2])
        .expect("tenant A payload");
    store
        .put(tenant_b.clone(), vec![3, 4])
        .expect("tenant B payload");

    assert_eq!(store.peek(&tenant_a), Some([1, 2].as_slice()));
    assert_eq!(store.peek(&tenant_b), Some([3, 4].as_slice()));
}

#[test]
fn oversized_payload_fails_without_mutating_the_store() {
    let mut store = HostDramKvStore::new(4).expect("valid capacity");
    let existing = identity("tenant-a", "entry-a", 1);
    let oversized = identity("tenant-a", "entry-b", 2);
    store
        .put(existing.clone(), vec![1, 2, 3])
        .expect("existing payload");

    let error = store
        .put(oversized, vec![4; 5])
        .expect_err("oversized payload must fail");

    assert_eq!(
        error,
        HostDramStoreError::PayloadExceedsCapacity {
            payload_bytes: 5,
            capacity_bytes: 4,
        }
    );
    assert_eq!(store.peek(&existing), Some([1, 2, 3].as_slice()));
    assert_eq!(store.stats().resident_bytes, 3);
}
