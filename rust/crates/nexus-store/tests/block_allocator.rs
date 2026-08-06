use nexus_store::NexusKVPagedGeometry;
use nexus_store::block_allocator::PagedBlockAllocator;

#[test]
fn test_paged_block_allocator_lifecycle() {
    let geom = NexusKVPagedGeometry::new(16, 4096);
    let mut allocator = PagedBlockAllocator::new(geom, 10);

    let stats_initial = allocator.stats();
    assert_eq!(stats_initial.capacity_blocks, 10);
    assert_eq!(stats_initial.allocated_blocks, 0);
    assert_eq!(stats_initial.free_blocks, 10);

    // Allocate 3 blocks
    let _b1 = allocator.allocate_block().expect("should allocate b1");
    let b2 = allocator.allocate_block().expect("should allocate b2");
    let _b3 = allocator.allocate_block().expect("should allocate b3");

    let stats_after_alloc = allocator.stats();
    assert_eq!(stats_after_alloc.allocated_blocks, 3);
    assert_eq!(stats_after_alloc.free_blocks, 7);

    // Free b2
    assert!(allocator.free_block(b2));
    assert_eq!(allocator.stats().allocated_blocks, 2);
    assert_eq!(allocator.stats().free_blocks, 8);

    // Compact free blocks
    allocator.compact_free_blocks();
    assert_eq!(allocator.stats().free_blocks, 8);

    // Re-allocate
    let b2_re = allocator
        .allocate_block()
        .expect("should re-allocate b2 slot");
    assert_eq!(b2_re, b2);
}
