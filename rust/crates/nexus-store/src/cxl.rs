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
    raw_buffers: HashMap<usize, Vec<u8>>,
}

impl CxlStoreAllocator {
    pub fn new(total_capacity_bytes: usize, block_size_bytes: usize) -> Self {
        Self {
            total_capacity_bytes,
            block_size_bytes,
            allocated_bytes: 0,
            next_block_id: 1,
            blocks: HashMap::new(),
            raw_buffers: HashMap::new(),
        }
    }

    pub fn allocate_block(&mut self) -> Result<CxlStoreBlock, &'static str> {
        if self.allocated_bytes + self.block_size_bytes > self.total_capacity_bytes {
            return Err("CXL Memory Out of Space");
        }

        let block_id = self.next_block_id;
        self.next_block_id += 1;

        let mut raw_buf = vec![0u8; self.block_size_bytes];
        let phys_ptr = raw_buf.as_mut_ptr() as u64;

        let block = CxlStoreBlock {
            block_id,
            phys_ptr,
            size_bytes: self.block_size_bytes,
            is_pinned: true,
        };

        self.allocated_bytes += self.block_size_bytes;
        self.raw_buffers.insert(block_id, raw_buf);
        self.blocks.insert(block_id, block.clone());
        Ok(block)
    }

    pub fn free_block(&mut self, block_id: usize) -> bool {
        self.raw_buffers.remove(&block_id);
        if let Some(block) = self.blocks.remove(&block_id) {
            self.allocated_bytes = self.allocated_bytes.saturating_sub(block.size_bytes);
            true
        } else {
            false
        }
    }
}
