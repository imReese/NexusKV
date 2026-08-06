//! Concrete NexusKV payload stores.

use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

pub mod block_allocator;
pub mod cxl;
pub mod hbm;
pub mod lockfree_queue;
pub mod tiering;

use nexus_state::EntryIdentity;

/// Engine-agnostic physical token page geometry description.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NexusKVPagedGeometry {
    /// Number of tokens grouped per physical block (e.g., 1, 16, 32, 64).
    pub block_size: usize,
    /// Physical byte stride per token slot (e.g., 4096 bytes for GQA, 576 bytes for MLA).
    pub stride_bytes: usize,
}

impl NexusKVPagedGeometry {
    pub fn new(block_size: usize, stride_bytes: usize) -> Self {
        Self {
            block_size,
            stride_bytes,
        }
    }

    /// Calculate total byte capacity of a single physical page block.
    pub fn page_block_bytes(&self) -> usize {
        self.block_size * self.stride_bytes
    }
}

/// Engine-agnostic physical page table indirection descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NexusKVPageTable {
    pub geometry: NexusKVPagedGeometry,
    pub page_indices: Vec<u64>,
    pub physical_handles: Vec<u64>,
}

impl NexusKVPageTable {
    pub fn new(
        geometry: NexusKVPagedGeometry,
        page_indices: Vec<u64>,
        physical_handles: Vec<u64>,
    ) -> Self {
        Self {
            geometry,
            page_indices,
            physical_handles,
        }
    }

    /// Calculate total mapped byte capacity across all physical pages.
    pub fn total_mapped_bytes(&self) -> usize {
        self.page_indices.len() * self.geometry.page_block_bytes()
    }

    /// Calculate byte offset for a target page slot index.
    pub fn page_offset(&self, page_slot_idx: usize) -> usize {
        page_slot_idx * self.geometry.page_block_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KvPayloadKey(EntryIdentity);

impl From<EntryIdentity> for KvPayloadKey {
    fn from(identity: EntryIdentity) -> Self {
        Self(identity)
    }
}

impl Hash for KvPayloadKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let identity = &self.0;
        let key = &identity.key;
        key.tenant.hash(state);
        key.namespace.hash(state);
        key.model.hash(state);
        key.engine_family.hash(state);
        key.semantic_type.hash(state);
        key.tokens.hash(state);
        key.block_id.hash(state);
        key.page_id.hash(state);
        identity.entry_id.hash(state);
        identity.version.generation.hash(state);
        identity.version.lineage.hash(state);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostDramStoreError {
    ZeroCapacity,
    EmptyEntryId,
    EmptyPayload,
    PayloadExceedsCapacity {
        payload_bytes: usize,
        capacity_bytes: usize,
    },
}

impl Display for HostDramStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroCapacity => formatter.write_str("Host DRAM store capacity must be non-zero"),
            Self::EmptyEntryId => formatter.write_str("Host DRAM store entry_id must be non-empty"),
            Self::EmptyPayload => formatter.write_str("Host DRAM store payload must be non-empty"),
            Self::PayloadExceedsCapacity {
                payload_bytes,
                capacity_bytes,
            } => write!(
                formatter,
                "Host DRAM payload is {payload_bytes} bytes but store capacity is {capacity_bytes} bytes"
            ),
        }
    }
}

impl Error for HostDramStoreError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostDramStoreStats {
    pub capacity_bytes: usize,
    pub resident_bytes: usize,
    pub entry_count: usize,
    pub eviction_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostDramPutOutcome {
    pub replaced: bool,
    pub evicted: Vec<EntryIdentity>,
}

#[derive(Debug)]
struct StoredPayload {
    bytes: Vec<u8>,
    older: Option<KvPayloadKey>,
    newer: Option<KvPayloadKey>,
}

#[derive(Debug)]
pub struct HostDramKvStore {
    capacity_bytes: usize,
    resident_bytes: usize,
    eviction_count: u64,
    entries: HashMap<KvPayloadKey, StoredPayload>,
    least_recent: Option<KvPayloadKey>,
    most_recent: Option<KvPayloadKey>,
}

impl HostDramKvStore {
    pub fn new(capacity_bytes: usize) -> Result<Self, HostDramStoreError> {
        if capacity_bytes == 0 {
            return Err(HostDramStoreError::ZeroCapacity);
        }
        Ok(Self {
            capacity_bytes,
            resident_bytes: 0,
            eviction_count: 0,
            entries: HashMap::new(),
            least_recent: None,
            most_recent: None,
        })
    }

    pub fn put(
        &mut self,
        identity: EntryIdentity,
        payload: Vec<u8>,
    ) -> Result<HostDramPutOutcome, HostDramStoreError> {
        if identity.entry_id.is_empty() {
            return Err(HostDramStoreError::EmptyEntryId);
        }
        if payload.is_empty() {
            return Err(HostDramStoreError::EmptyPayload);
        }
        if payload.len() > self.capacity_bytes {
            return Err(HostDramStoreError::PayloadExceedsCapacity {
                payload_bytes: payload.len(),
                capacity_bytes: self.capacity_bytes,
            });
        }

        let key = KvPayloadKey::from(identity);
        let replaced = self.remove_key(&key).is_some();
        let mut evicted = Vec::new();
        while payload.len() > self.capacity_bytes - self.resident_bytes {
            let evicted_key = self
                .least_recent
                .clone()
                .expect("resident byte accounting requires an LRU entry");
            let removed = self
                .remove_key(&evicted_key)
                .expect("LRU entry must exist in the payload map");
            self.eviction_count += 1;
            evicted.push(evicted_key.0);
            drop(removed);
        }

        self.resident_bytes += payload.len();
        self.entries.insert(
            key.clone(),
            StoredPayload {
                bytes: payload,
                older: None,
                newer: None,
            },
        );
        self.attach_most_recent(&key);

        Ok(HostDramPutOutcome { replaced, evicted })
    }

    pub fn get(&mut self, identity: &EntryIdentity) -> Option<&[u8]> {
        let key = KvPayloadKey::from(identity.clone());
        if !self.entries.contains_key(&key) {
            return None;
        }
        self.touch(&key);
        self.entries.get(&key).map(|entry| entry.bytes.as_slice())
    }

    pub fn peek(&self, identity: &EntryIdentity) -> Option<&[u8]> {
        let key = KvPayloadKey::from(identity.clone());
        self.entries.get(&key).map(|entry| entry.bytes.as_slice())
    }

    pub fn remove(&mut self, identity: &EntryIdentity) -> Option<Vec<u8>> {
        let key = KvPayloadKey::from(identity.clone());
        self.remove_key(&key).map(|entry| entry.bytes)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.resident_bytes = 0;
        self.least_recent = None;
        self.most_recent = None;
    }

    pub fn stats(&self) -> HostDramStoreStats {
        HostDramStoreStats {
            capacity_bytes: self.capacity_bytes,
            resident_bytes: self.resident_bytes,
            entry_count: self.entries.len(),
            eviction_count: self.eviction_count,
        }
    }

    fn touch(&mut self, key: &KvPayloadKey) {
        if self.most_recent.as_ref() == Some(key) {
            return;
        }
        self.detach(key);
        self.attach_most_recent(key);
    }

    fn attach_most_recent(&mut self, key: &KvPayloadKey) {
        let previous_most_recent = self.most_recent.clone();
        let entry = self
            .entries
            .get_mut(key)
            .expect("attached payload must exist in the payload map");
        entry.older = previous_most_recent.clone();
        entry.newer = None;

        if let Some(previous_key) = previous_most_recent {
            self.entries
                .get_mut(&previous_key)
                .expect("previous MRU payload must exist in the payload map")
                .newer = Some(key.clone());
        } else {
            self.least_recent = Some(key.clone());
        }
        self.most_recent = Some(key.clone());
    }

    fn detach(&mut self, key: &KvPayloadKey) {
        let (older, newer) = {
            let entry = self
                .entries
                .get(key)
                .expect("detached payload must exist in the payload map");
            (entry.older.clone(), entry.newer.clone())
        };

        if let Some(older_key) = &older {
            self.entries
                .get_mut(older_key)
                .expect("older payload must exist in the payload map")
                .newer = newer.clone();
        } else {
            self.least_recent = newer.clone();
        }
        if let Some(newer_key) = &newer {
            self.entries
                .get_mut(newer_key)
                .expect("newer payload must exist in the payload map")
                .older = older.clone();
        } else {
            self.most_recent = older.clone();
        }

        let entry = self
            .entries
            .get_mut(key)
            .expect("detached payload must exist in the payload map");
        entry.older = None;
        entry.newer = None;
    }

    fn remove_key(&mut self, key: &KvPayloadKey) -> Option<StoredPayload> {
        if !self.entries.contains_key(key) {
            return None;
        }
        self.detach(key);
        let entry = self
            .entries
            .remove(key)
            .expect("detached payload must exist in the payload map");
        self.resident_bytes -= entry.bytes.len();
        Some(entry)
    }
}
