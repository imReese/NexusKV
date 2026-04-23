use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SCHEMA_VERSION: &str = "nexuskv.contract.v1";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EngineFamily {
    Unknown,
    Sglang,
    Vllm,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateSemanticType {
    MhaKv,
    GqaKv,
    MqaKv,
    MlaState,
    GenericContainer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TensorRole {
    Key,
    Value,
    Latent,
    Position,
    Auxiliary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Granularity {
    Token,
    Block,
    Page,
    Segment,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompatibilityFlag {
    ExactReuse,
    PrefixReuse,
    PageReuse,
    BlockReuse,
    WarmStart,
    PostEvictionReload,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeviceClass {
    Cpu,
    Cuda,
    Rocm,
    Tpu,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferKind {
    Device,
    HostPinned,
    HostPageable,
    Remote,
    FileBacked,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferBackend {
    BaselineTransport,
    StagedCopy,
    Rdma,
    ZeroCopy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransferCapability {
    HostToDevice,
    DeviceToHost,
    HostToStore,
    StoreToHost,
    Async,
    ZeroCopyCandidate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierKind {
    Device,
    HostDram,
    LocalSsd,
    RemoteShared,
    ObjectStore,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaterializationCapability {
    Full,
    Partial,
    Prefetch,
    AsyncFetch,
    FallbackRecompute,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchClassification {
    Exact,
    Prefix,
    Partial,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanDisposition {
    FullReuse,
    PartialReuse,
    Recompute,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TensorSpec {
    pub name: String,
    pub role: TensorRole,
    pub dtype: String,
    pub shape: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuantizationMetadata {
    pub scheme: String,
    pub bits: u32,
    pub group_size: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayoutMetadata {
    pub layout: String,
    pub page_tokens: u32,
    pub block_tokens: u32,
    pub packed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferPath {
    pub backend: TransferBackend,
    pub capabilities: Vec<TransferCapability>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationProfile {
    pub capabilities: Vec<MaterializationCapability>,
    pub tier_kinds: Vec<TierKind>,
    pub device_classes: Vec<DeviceClass>,
    pub buffer_kinds: Vec<BufferKind>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttentionStateDescriptor {
    pub schema_version: String,
    pub descriptor_id: String,
    pub engine_family: EngineFamily,
    pub semantic_type: StateSemanticType,
    pub granularity: Granularity,
    pub tensor_specs: Vec<TensorSpec>,
    pub quantization: QuantizationMetadata,
    pub layout: LayoutMetadata,
    pub compatibility_flags: Vec<CompatibilityFlag>,
    pub transfer_paths: Vec<TransferPath>,
    pub materialization: MaterializationProfile,
    pub layout_metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyIdentity {
    pub tenant: String,
    pub namespace: String,
    pub model: String,
    pub engine_family: EngineFamily,
    pub semantic_type: StateSemanticType,
    pub tokens: Vec<u32>,
    pub block_id: Option<u32>,
    pub page_id: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReuseKey {
    pub identity: KeyIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryKey {
    pub identity: KeyIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryVersion {
    pub generation: u32,
    pub lineage: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryLocation {
    pub tier: TierKind,
    pub locator: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyHint {
    pub reusable: bool,
    pub admission_hint: String,
    pub eviction_hint: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryIdentity {
    pub key: KeyIdentity,
    pub entry_id: String,
    pub version: EntryVersion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntry {
    pub identity: EntryIdentity,
    pub descriptor: AttentionStateDescriptor,
    pub location: EntryLocation,
    pub policy_hint: PolicyHint,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchExtent {
    pub units: u32,
    pub granularity: Granularity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemainingWork {
    pub tokens: Vec<u32>,
    pub fetch_required: bool,
    pub recompute_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompatibilitySignal {
    pub reusable: bool,
    pub fallback_to_recompute: bool,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchResult {
    pub classification: MatchClassification,
    pub matched_key: ReuseKey,
    pub requested_key: QueryKey,
    pub matched_extent: MatchExtent,
    pub entry: CacheEntry,
    pub remaining: RemainingWork,
    pub compatibility: CompatibilitySignal,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReusableSlice {
    pub tokens: Vec<u32>,
    pub source_tier: TierKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartialHitPlan {
    pub disposition: PlanDisposition,
    pub reusable: ReusableSlice,
    pub remaining: RemainingWork,
    pub entry: CacheEntry,
}
