//! CXL 3.1 & Shared Memory Multi-Tenant Partitioned Store Allocator.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CxlSliceDescriptor {
    pub tenant_id: String,
    pub slice_id: u64,
    pub capacity_bytes: usize,
    pub physical_base_addr: u64,
}

pub struct CxlStoreAllocator {
    capacity_bytes: usize,
    allocated_bytes: AtomicUsize,
    slices: Mutex<HashMap<String, Vec<CxlSliceDescriptor>>>,
}

impl CxlStoreAllocator {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            allocated_bytes: AtomicUsize::new(0),
            slices: Mutex::new(HashMap::new()),
        }
    }

    /// Allocates a multi-tenant isolated physical CXL 3.1 memory slice.
    pub fn allocate_slice(
        &self,
        tenant_id: &str,
        size_bytes: usize,
    ) -> Result<CxlSliceDescriptor, &'static str> {
        if size_bytes == 0 {
            return Err("Requested slice size must be non-zero");
        }

        let current = self.allocated_bytes.fetch_add(size_bytes, Ordering::SeqCst);
        if current + size_bytes > self.capacity_bytes {
            self.allocated_bytes.fetch_sub(size_bytes, Ordering::SeqCst);
            return Err("CXL memory capacity exceeded");
        }

        let slice_id = (current / size_bytes) as u64 + 1;
        let physical_base_addr = 0x8000_0000_0000_u64 + (current as u64);

        let slice = CxlSliceDescriptor {
            tenant_id: tenant_id.to_string(),
            slice_id,
            capacity_bytes: size_bytes,
            physical_base_addr,
        };

        let mut guard = self.slices.lock().unwrap();
        guard
            .entry(tenant_id.to_string())
            .or_default()
            .push(slice.clone());

        Ok(slice)
    }

    pub fn get_tenant_slices(&self, tenant_id: &str) -> Vec<CxlSliceDescriptor> {
        let guard = self.slices.lock().unwrap();
        guard.get(tenant_id).cloned().unwrap_or_default()
    }

    pub fn capacity_bytes(&self) -> usize {
        self.capacity_bytes
    }

    pub fn allocated_bytes(&self) -> usize {
        self.allocated_bytes.load(Ordering::Relaxed)
    }
}
