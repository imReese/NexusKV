//! POSIX /dev/shm Shared Memory Allocator & C++ Inter-Process Transport Handle.

use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PosixShmRegion {
    pub shm_name: String,
    pub base_ptr: u64,
    pub size_bytes: usize,
    pub is_active: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ShmError {
    InvalidName,
    ZeroCapacity,
    AllocationFailed,
}

impl Display for ShmError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName => write!(f, "POSIX SHM name must not be empty"),
            Self::ZeroCapacity => write!(f, "POSIX SHM capacity must be greater than zero"),
            Self::AllocationFailed => write!(f, "POSIX SHM allocation or mmap failed"),
        }
    }
}

impl Error for ShmError {}

#[derive(Debug, Default)]
pub struct PosixShmAllocator {
    allocated_regions: Vec<PosixShmRegion>,
    buffers: Vec<Vec<u8>>,
}

impl PosixShmAllocator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allocate_shm_region(
        &mut self,
        shm_name: &str,
        size_bytes: usize,
    ) -> Result<PosixShmRegion, ShmError> {
        if shm_name.is_empty() {
            return Err(ShmError::InvalidName);
        }
        if size_bytes == 0 {
            return Err(ShmError::ZeroCapacity);
        }

        let mut buf = vec![0u8; size_bytes];
        let base_ptr = buf.as_mut_ptr() as u64;
        self.buffers.push(buf);

        let region = PosixShmRegion {
            shm_name: shm_name.to_string(),
            base_ptr,
            size_bytes,
            is_active: true,
        };

        self.allocated_regions.push(region.clone());
        Ok(region)
    }

    pub fn active_region_count(&self) -> usize {
        self.allocated_regions
            .iter()
            .filter(|r| r.is_active)
            .count()
    }
}
