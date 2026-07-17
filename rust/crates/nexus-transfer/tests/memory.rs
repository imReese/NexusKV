use nexus_transfer::{
    KvCacheMemoryLocation, TransferableKvCacheMemory, TransferableKvCacheMemoryError,
    TransferableKvCacheRegion,
};

#[test]
fn validates_and_preserves_runtime_owned_regions() {
    let memory = TransferableKvCacheMemory::new(
        vec![
            TransferableKvCacheRegion {
                base_addr: 0x1000,
                byte_len: 0x800,
                page_size_bytes: 0x100,
            },
            TransferableKvCacheRegion {
                base_addr: 0x2000,
                byte_len: 0x800,
                page_size_bytes: 0x100,
            },
        ],
        0x200,
        KvCacheMemoryLocation::Cuda { device_id: 3 },
    )
    .expect("valid transferable memory");

    assert_eq!(memory.page_size_bytes(), 0x200);
    assert_eq!(memory.regions().len(), 2);
    assert_eq!(
        memory.location(),
        KvCacheMemoryLocation::Cuda { device_id: 3 }
    );
}

#[test]
fn rejects_region_geometry_that_cannot_describe_pages() {
    let error = TransferableKvCacheMemory::new(
        vec![TransferableKvCacheRegion {
            base_addr: 0x1000,
            byte_len: 0x180,
            page_size_bytes: 0x100,
        }],
        0x100,
        KvCacheMemoryLocation::Cpu { numa_node: 0 },
    )
    .expect_err("partial pages must fail");

    assert_eq!(
        error,
        TransferableKvCacheMemoryError::RegionLengthNotPageMultiple {
            region_index: 0,
            byte_len: 0x180,
            page_size_bytes: 0x100,
        }
    );
}

#[test]
fn rejects_overlapping_runtime_regions() {
    let error = TransferableKvCacheMemory::new(
        vec![
            TransferableKvCacheRegion {
                base_addr: 0x1000,
                byte_len: 0x400,
                page_size_bytes: 0x100,
            },
            TransferableKvCacheRegion {
                base_addr: 0x1200,
                byte_len: 0x400,
                page_size_bytes: 0x100,
            },
        ],
        0x200,
        KvCacheMemoryLocation::Rocm { device_id: 0 },
    )
    .expect_err("overlapping regions must fail");

    assert_eq!(
        error,
        TransferableKvCacheMemoryError::OverlappingRegions {
            first_region_index: 0,
            second_region_index: 1,
        }
    );
}
