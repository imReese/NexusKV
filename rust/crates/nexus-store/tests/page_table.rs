use nexus_store::{NexusKVPageTable, NexusKVPagedGeometry};

#[test]
fn test_engine_agnostic_geometry_and_page_table() {
    // 1. Standard GQA Geometry (BlockSize=16, StrideBytes=4096)
    let gqa_geom = NexusKVPagedGeometry::new(16, 4096);
    assert_eq!(gqa_geom.page_block_bytes(), 65536);

    let gqa_page_table = NexusKVPageTable::new(
        gqa_geom,
        vec![0, 1, 2, 3],
        vec![0x1000, 0x2000, 0x3000, 0x4000],
    );
    assert_eq!(gqa_page_table.total_mapped_bytes(), 262144);
    assert_eq!(gqa_page_table.page_offset(2), 131072);

    // 2. Compressed MLA Geometry (BlockSize=32, StrideBytes=576)
    let mla_geom = NexusKVPagedGeometry::new(32, 576);
    assert_eq!(mla_geom.page_block_bytes(), 18432);

    let mla_page_table =
        NexusKVPageTable::new(mla_geom, vec![10, 11, 12], vec![0xA000, 0xB000, 0xC000]);
    assert_eq!(mla_page_table.total_mapped_bytes(), 55296);
    assert_eq!(mla_page_table.page_offset(1), 18432);
}
