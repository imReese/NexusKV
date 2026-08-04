//! HBM Paged Block Allocator & Fragment Manager for GPU Memory.

use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HbmBlock {
    pub block_id: usize,
    pub ptr: u64,
    pub size_bytes: usize,
    pub is_pinned: bool,
    pub is_active: bool,
}

#[derive(Debug)]
pub struct HbmBlockAllocator {
    pub total_capacity_bytes: usize,
    pub block_size_bytes: usize,
    pub allocated_bytes: usize,
    pub pinned_bytes: usize,
    next_block_id: usize,
    blocks: HashMap<usize, HbmBlock>,
}

impl HbmBlockAllocator {
    pub fn new(total_capacity_bytes: usize, block_size_bytes: usize) -> Self {
        Self {
            total_capacity_bytes,
            block_size_bytes,
            allocated_bytes: 0,
            pinned_bytes: 0,
            next_block_id: 1,
            blocks: HashMap::new(),
        }
    }

    pub fn allocate_block(&mut self) -> Result<HbmBlock, &'static str> {
        if self.allocated_bytes + self.block_size_bytes > self.total_capacity_bytes {
            return Err("HBM Out of Memory");
        }

        let block_id = self.next_block_id;
        self.next_block_id += 1;
        // Simulated CUDA device pointer
        let ptr = 0x7FFF_0000_0000 + (block_id * self.block_size_bytes) as u64;

        let block = HbmBlock {
            block_id,
            ptr,
            size_bytes: self.block_size_bytes,
            is_pinned: true,
            is_active: true,
        };

        self.allocated_bytes += self.block_size_bytes;
        self.pinned_bytes += self.block_size_bytes;
        self.blocks.insert(block_id, block.clone());

        Ok(block)
    }

    pub fn unpin_block(&mut self, block_id: usize) -> bool {
        if let Some(block) = self.blocks.get_mut(&block_id) {
            if block.is_pinned {
                block.is_pinned = false;
                self.pinned_bytes -= block.size_bytes;
                return true;
            }
        }
        false
    }

    pub fn pin_block(&mut self, block_id: usize) -> bool {
        if let Some(block) = self.blocks.get_mut(&block_id) {
            if !block.is_pinned {
                block.is_pinned = true;
                self.pinned_bytes += block.size_bytes;
                return true;
            }
        }
        false
    }

    pub fn free_block(&mut self, block_id: usize) -> bool {
        if let Some(block) = self.blocks.remove(&block_id) {
            self.allocated_bytes -= block.size_bytes;
            if block.is_pinned {
                self.pinned_bytes -= block.size_bytes;
            }
            return true;
        }
        false
    }

    pub fn active_block_count(&self) -> usize {
        self.blocks.len()
    }
}
