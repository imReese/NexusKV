//! CXL 3.0 (Compute Express Link) TraCT Shared Memory Pool Transfer Primitives.

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
        if page_size_bytes == 0 || length_bytes % page_size_bytes != 0 {
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
