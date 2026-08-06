//! Engine-Agnostic Paged Block Free-List Allocator & Memory Defragmenter.

use crate::NexusKVPagedGeometry;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AllocatorStats {
    pub capacity_blocks: usize,
    pub allocated_blocks: usize,
    pub free_blocks: usize,
    pub fragmentation_ratio: f64,
}

#[derive(Debug)]
pub struct PagedBlockAllocator {
    geometry: NexusKVPagedGeometry,
    capacity_blocks: usize,
    free_list: Vec<u64>,
    allocated_set: HashSet<u64>,
}

impl PagedBlockAllocator {
    pub fn new(geometry: NexusKVPagedGeometry, capacity_blocks: usize) -> Self {
        let mut free_list = Vec::with_capacity(capacity_blocks);
        for id in 0..capacity_blocks {
            free_list.push(id as u64);
        }

        Self {
            geometry,
            capacity_blocks,
            free_list,
            allocated_set: HashSet::new(),
        }
    }

    /// Allocate a single physical block slot. O(1) complexity.
    pub fn allocate_block(&mut self) -> Option<u64> {
        if let Some(block_id) = self.free_list.pop() {
            self.allocated_set.insert(block_id);
            Some(block_id)
        } else {
            None
        }
    }

    /// Return a physical block slot back to the free list.
    pub fn free_block(&mut self, block_id: u64) -> bool {
        if self.allocated_set.remove(&block_id) {
            self.free_list.push(block_id);
            true
        } else {
            false
        }
    }

    /// Compact and sort free blocks to reduce memory fragmentation.
    pub fn compact_free_blocks(&mut self) {
        self.free_list.sort_unstable();
        self.free_list.dedup();
    }

    /// Calculate allocator statistics and fragmentation ratio.
    pub fn stats(&self) -> AllocatorStats {
        let allocated_blocks = self.allocated_set.len();
        let free_blocks = self.free_list.len();

        let fragmentation_ratio = if self.capacity_blocks == 0 {
            0.0
        } else {
            (self.capacity_blocks - allocated_blocks - free_blocks) as f64
                / self.capacity_blocks as f64
        };

        AllocatorStats {
            capacity_blocks: self.capacity_blocks,
            allocated_blocks,
            free_blocks,
            fragmentation_ratio: fragmentation_ratio.max(0.0),
        }
    }

    pub fn geometry(&self) -> &NexusKVPagedGeometry {
        &self.geometry
    }
}
