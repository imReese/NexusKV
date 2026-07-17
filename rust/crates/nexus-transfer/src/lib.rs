//! Device-neutral contracts for memory that can participate in KV cache transfer.
//!
//! The types in this crate describe memory owned by an inference runtime. They do
//! not register, copy, or dereference the exposed addresses. A transfer backend
//! must keep the provider and its allocation alive for the whole registration and
//! transfer lifetime.

use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KvCacheMemoryLocation {
    Cpu { numa_node: usize },
    Cuda { device_id: usize },
    Rocm { device_id: usize },
    Metal { device_id: usize },
    Musa { device_id: usize },
    Xpu { device_id: usize },
    Npu { device_id: usize },
    Hpu { device_id: usize },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferableKvCacheRegion {
    pub base_addr: usize,
    pub byte_len: usize,
    pub page_size_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferableKvCacheMemory {
    regions: Vec<TransferableKvCacheRegion>,
    page_size_bytes: usize,
    location: KvCacheMemoryLocation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransferableKvCacheMemoryError {
    ZeroPageSize,
    EmptyRegions,
    UnevenPageLayout {
        page_size_bytes: usize,
        region_count: usize,
    },
    ZeroBaseAddress {
        region_index: usize,
    },
    ZeroRegionLength {
        region_index: usize,
    },
    RegionPageSizeMismatch {
        region_index: usize,
        expected: usize,
        actual: usize,
    },
    RegionLengthNotPageMultiple {
        region_index: usize,
        byte_len: usize,
        page_size_bytes: usize,
    },
    RegionAddressOverflow {
        region_index: usize,
    },
    OverlappingRegions {
        first_region_index: usize,
        second_region_index: usize,
    },
}

impl Display for TransferableKvCacheMemoryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroPageSize => {
                formatter.write_str("transferable KV memory page size must be non-zero")
            }
            Self::EmptyRegions => {
                formatter.write_str("transferable KV memory must expose at least one region")
            }
            Self::UnevenPageLayout {
                page_size_bytes,
                region_count,
            } => write!(
                formatter,
                "transferable KV memory page size {page_size_bytes} must be divisible by {region_count} regions"
            ),
            Self::ZeroBaseAddress { region_index } => write!(
                formatter,
                "transferable KV memory region {region_index} base address must be non-zero"
            ),
            Self::ZeroRegionLength { region_index } => write!(
                formatter,
                "transferable KV memory region {region_index} length must be non-zero"
            ),
            Self::RegionPageSizeMismatch {
                region_index,
                expected,
                actual,
            } => write!(
                formatter,
                "transferable KV memory region {region_index} page size {actual} does not match per-region page size {expected}"
            ),
            Self::RegionLengthNotPageMultiple {
                region_index,
                byte_len,
                page_size_bytes,
            } => write!(
                formatter,
                "transferable KV memory region {region_index} length {byte_len} must be a multiple of page size {page_size_bytes}"
            ),
            Self::RegionAddressOverflow { region_index } => write!(
                formatter,
                "transferable KV memory region {region_index} address range overflows usize"
            ),
            Self::OverlappingRegions {
                first_region_index,
                second_region_index,
            } => write!(
                formatter,
                "transferable KV memory regions {first_region_index} and {second_region_index} overlap"
            ),
        }
    }
}

impl Error for TransferableKvCacheMemoryError {}

impl TransferableKvCacheMemory {
    pub fn new(
        regions: Vec<TransferableKvCacheRegion>,
        page_size_bytes: usize,
        location: KvCacheMemoryLocation,
    ) -> Result<Self, TransferableKvCacheMemoryError> {
        if page_size_bytes == 0 {
            return Err(TransferableKvCacheMemoryError::ZeroPageSize);
        }
        if regions.is_empty() {
            return Err(TransferableKvCacheMemoryError::EmptyRegions);
        }
        if !page_size_bytes.is_multiple_of(regions.len()) {
            return Err(TransferableKvCacheMemoryError::UnevenPageLayout {
                page_size_bytes,
                region_count: regions.len(),
            });
        }

        let region_page_size_bytes = page_size_bytes / regions.len();
        let mut ranges = Vec::with_capacity(regions.len());
        for (region_index, region) in regions.iter().enumerate() {
            if region.base_addr == 0 {
                return Err(TransferableKvCacheMemoryError::ZeroBaseAddress { region_index });
            }
            if region.byte_len == 0 {
                return Err(TransferableKvCacheMemoryError::ZeroRegionLength { region_index });
            }
            if region.page_size_bytes != region_page_size_bytes {
                return Err(TransferableKvCacheMemoryError::RegionPageSizeMismatch {
                    region_index,
                    expected: region_page_size_bytes,
                    actual: region.page_size_bytes,
                });
            }
            if !region.byte_len.is_multiple_of(region.page_size_bytes) {
                return Err(
                    TransferableKvCacheMemoryError::RegionLengthNotPageMultiple {
                        region_index,
                        byte_len: region.byte_len,
                        page_size_bytes: region.page_size_bytes,
                    },
                );
            }
            let end_addr = region
                .base_addr
                .checked_add(region.byte_len)
                .ok_or(TransferableKvCacheMemoryError::RegionAddressOverflow { region_index })?;
            for (other_index, (other_start, other_end)) in ranges.iter().copied().enumerate() {
                if region.base_addr < other_end && other_start < end_addr {
                    return Err(TransferableKvCacheMemoryError::OverlappingRegions {
                        first_region_index: other_index,
                        second_region_index: region_index,
                    });
                }
            }
            ranges.push((region.base_addr, end_addr));
        }

        Ok(Self {
            regions,
            page_size_bytes,
            location,
        })
    }

    pub fn regions(&self) -> &[TransferableKvCacheRegion] {
        &self.regions
    }

    pub fn page_size_bytes(&self) -> usize {
        self.page_size_bytes
    }

    pub fn location(&self) -> KvCacheMemoryLocation {
        self.location
    }
}

pub trait KvCacheMemoryProvider {
    type Error;

    fn transferable_kv_cache_memory(&self) -> Result<TransferableKvCacheMemory, Self::Error>;
}
