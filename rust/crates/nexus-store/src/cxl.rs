//! CXL 3.0 Paged Block Store Allocator.

use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CxlStoreBlock {
    pub block_id: usize,
    pub phys_ptr: u64,
    pub size_bytes: usize,
    pub is_pinned: bool,
}

#[derive(Debug)]
pub struct CxlStoreAllocator {
    pub total_capacity_bytes: usize,
    pub block_size_bytes: usize,
    pub allocated_bytes: usize,
    next_block_id: usize,
    blocks: HashMap<usize, CxlStoreBlock>,
}

impl CxlStoreAllocator {
    pub fn new(total_capacity_bytes: usize, block_size_bytes: usize) -> Self {
        Self {
            total_capacity_bytes,
            block_size_bytes,
            allocated_bytes: 0,
            next_block_id: 1,
            blocks: HashMap::new(),
        }
    }

    pub fn allocate_block(&mut self) -> Result<CxlStoreBlock, &'static str> {
        if self.allocated_bytes + self.block_size_bytes > self.total_capacity_bytes {
            return Err("CXL Memory Out of Space");
        }

        let block_id = self.next_block_id;
        self.next_block_id += 1;
        let phys_ptr = 0xC000_0000_0000 + (block_id * self.block_size_bytes) as u64;

        let block = CxlStoreBlock {
            block_id,
            phys_ptr,
            size_bytes: self.block_size_bytes,
            is_pinned: true,
        };

        self.allocated_bytes += self.block_size_bytes;
        self.blocks.insert(block_id, block.clone());
        Ok(block)
    }

    pub fn free_block(&mut self, block_id: usize) -> bool {
        if let Some(block) = self.blocks.remove(&block_id) {
            self.allocated_bytes = self.allocated_bytes.saturating_sub(block.size_bytes);
            true
        } else {
            false
        }
    }
}
