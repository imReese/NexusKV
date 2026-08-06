//! MoE Expert Partitioning & LRU-K Hotness-Aware Tiered Memory Migration Engine.

use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryTierKind {
    Hbm3eGpu,
    HostDramCxl,
    NvmeFlash,
}

#[derive(Debug, Clone)]
pub struct BlockHotnessScore {
    pub block_id: u64,
    pub access_count: u64,
    pub last_access_timestamp_ns: u64,
    pub lru_k_distance_ns: u64,
    pub current_tier: MemoryTierKind,
}

pub struct TieredStorageEngine {
    k_value: usize,
    hot_threshold_ns: u64,
    block_scores: HashMap<u64, BlockHotnessScore>,
    access_histories: HashMap<u64, Vec<u64>>,
}

impl TieredStorageEngine {
    pub fn new(k_value: usize, hot_threshold_ns: u64) -> Self {
        Self {
            k_value,
            hot_threshold_ns,
            block_scores: HashMap::new(),
            access_histories: HashMap::new(),
        }
    }

    /// Record a block access event and dynamically compute LRU-K hotness score.
    pub fn record_access(&mut self, block_id: u64, timestamp_ns: u64) -> MemoryTierKind {
        let history = self.access_histories.entry(block_id).or_default();
        history.push(timestamp_ns);
        if history.len() > self.k_value {
            history.remove(0);
        }

        let access_count = history.len() as u64;
        let last_access = *history.last().unwrap_or(&timestamp_ns);

        let k_distance = if history.len() == self.k_value {
            timestamp_ns.saturating_sub(history[0])
        } else {
            u64::MAX
        };

        let target_tier = if k_distance <= self.hot_threshold_ns {
            MemoryTierKind::Hbm3eGpu
        } else if access_count > 1 {
            MemoryTierKind::HostDramCxl
        } else {
            MemoryTierKind::NvmeFlash
        };

        self.block_scores.insert(
            block_id,
            BlockHotnessScore {
                block_id,
                access_count,
                last_access_timestamp_ns: last_access,
                lru_k_distance_ns: k_distance,
                current_tier: target_tier,
            },
        );

        target_tier
    }

    pub fn get_block_tier(&self, block_id: u64) -> MemoryTierKind {
        self.block_scores
            .get(&block_id)
            .map(|score| score.current_tier)
            .unwrap_or(MemoryTierKind::NvmeFlash)
    }

    pub fn stats(&self) -> usize {
        self.block_scores.len()
    }
}
