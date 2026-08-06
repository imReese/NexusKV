//! CXL 3.1 & UALink 2.0 (Compute Express Link & Ultra Accelerator Link) TraCT Shared Memory Pool Transfer Primitives.

use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CxlFabricLocation {
    pub fabric_id: usize,
    pub cxl_node_id: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CxlSharedMemoryRegion {
    pub base_phys_addr: u64,
    pub length_bytes: usize,
    pub page_size_bytes: usize,
    pub is_direct_load_store: bool,
}

impl CxlSharedMemoryRegion {
    pub fn new(
        base_phys_addr: u64,
        length_bytes: usize,
        page_size_bytes: usize,
    ) -> Result<Self, &'static str> {
        if base_phys_addr == 0 {
            return Err("CXL base physical address cannot be zero");
        }
        if length_bytes == 0 {
            return Err("CXL region length cannot be zero");
        }
        if page_size_bytes == 0 || !length_bytes.is_multiple_of(page_size_bytes) {
            return Err("CXL region length must be aligned to page_size_bytes");
        }

        Ok(Self {
            base_phys_addr,
            length_bytes,
            page_size_bytes,
            is_direct_load_store: true,
        })
    }

    pub fn page_count(&self) -> usize {
        self.length_bytes / self.page_size_bytes
    }
}

#[derive(Debug)]
pub struct CxlFabricMemoryPool {
    pub total_capacity_bytes: usize,
    pub allocated_bytes: usize,
    regions: HashMap<usize, CxlSharedMemoryRegion>,
}

impl CxlFabricMemoryPool {
    pub fn new(total_capacity_bytes: usize) -> Self {
        Self {
            total_capacity_bytes,
            allocated_bytes: 0,
            regions: HashMap::new(),
        }
    }

    pub fn register_region(
        &mut self,
        region_id: usize,
        region: CxlSharedMemoryRegion,
    ) -> Result<(), &'static str> {
        if self.allocated_bytes + region.length_bytes > self.total_capacity_bytes {
            return Err("CXL Fabric Pool Capacity Exceeded");
        }
        self.allocated_bytes += region.length_bytes;
        self.regions.insert(region_id, region);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UALink2FabricTransport {
    pub link_id: String,
    pub max_bandwidth_gbps: usize,
    pub is_active: bool,
}

impl UALink2FabricTransport {
    pub fn new(link_id: String) -> Self {
        Self {
            link_id,
            max_bandwidth_gbps: 3200,
            is_active: true,
        }
    }
}
