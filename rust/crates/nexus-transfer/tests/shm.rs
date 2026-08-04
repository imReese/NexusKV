use nexus_transfer::shm::{PosixShmAllocator, ShmError};

#[test]
fn test_posix_shm_allocator_lifecycle() {
    let mut allocator = PosixShmAllocator::new();

    let region = allocator
        .allocate_shm_region("/nexuskv_shm_test", 1024 * 1024)
        .expect("should allocate shm region");

    assert_eq!(region.shm_name, "/nexuskv_shm_test");
    assert_eq!(region.size_bytes, 1024 * 1024);
    assert_eq!(allocator.active_region_count(), 1);

    let err_empty = allocator.allocate_shm_region("", 1024);
    assert_eq!(err_empty, Err(ShmError::InvalidName));

    let err_zero = allocator.allocate_shm_region("/shm_zero", 0);
    assert_eq!(err_zero, Err(ShmError::ZeroCapacity));
}
