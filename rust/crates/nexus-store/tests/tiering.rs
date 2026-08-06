use nexus_store::tiering::{MemoryTierKind, TieredStorageEngine};

#[test]
fn test_lru_k_tiered_migration_lifecycle() {
    let mut engine = TieredStorageEngine::new(2, 5000);

    // Initial access -> Cold (NvmeFlash)
    let tier1 = engine.record_access(101, 1000);
    assert_eq!(tier1, MemoryTierKind::NvmeFlash);

    // Second access within short window -> Warm (HostDramCxl)
    let tier2 = engine.record_access(101, 2000);
    assert_eq!(tier2, MemoryTierKind::Hbm3eGpu);

    assert_eq!(engine.stats(), 1);
    assert_eq!(engine.get_block_tier(101), MemoryTierKind::Hbm3eGpu);
}
