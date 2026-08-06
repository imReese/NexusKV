use nexus_store::cxl::CxlStoreAllocator;

#[test]
fn test_cxl_store_allocator_lifecycle() {
    let allocator = CxlStoreAllocator::new(64 * 1024 * 1024);

    let slice1 = allocator
        .allocate_slice("tenant-1", 16 * 1024 * 1024)
        .expect("Allocation 1 should succeed");
    assert_eq!(slice1.slice_id, 1);
    assert_eq!(allocator.allocated_bytes(), 16 * 1024 * 1024);

    let slice2 = allocator
        .allocate_slice("tenant-1", 16 * 1024 * 1024)
        .expect("Allocation 2 should succeed");
    assert_eq!(slice2.slice_id, 2);
    assert_eq!(allocator.allocated_bytes(), 32 * 1024 * 1024);

    let slices = allocator.get_tenant_slices("tenant-1");
    assert_eq!(slices.len(), 2);
}
