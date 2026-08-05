use nexus_store::cxl::CxlStoreAllocator;

#[test]
fn test_cxl_store_allocator_lifecycle() {
    let mut allocator = CxlStoreAllocator::new(64 * 1024 * 1024, 16 * 1024 * 1024);

    let block1 = allocator.allocate_block().expect("Allocation 1 should succeed");
    assert_eq!(block1.block_id, 1);
    assert_eq!(allocator.allocated_bytes, 16 * 1024 * 1024);

    let block2 = allocator.allocate_block().expect("Allocation 2 should succeed");
    assert_eq!(block2.block_id, 2);

    let freed = allocator.free_block(block1.block_id);
    assert!(freed);
    assert_eq!(allocator.allocated_bytes, 16 * 1024 * 1024);
}
