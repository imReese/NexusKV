//! nxradixtree core primitives.

use std::collections::HashMap;

pub use nexus_state::{
    CacheEntry,
    CompatibilitySignal,
    EntryIdentity,
    EntryLocation,
    EntryVersion,
    KeyIdentity,
    MatchClassification,
    MatchExtent,
    PartialHitPlan,
    PlanDisposition,
    PolicyHint,
    QueryKey,
    RemainingWork,
    ReusableSlice,
    ReuseKey,
};
use nexus_state::{Granularity, TierKind};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct KeyScope {
    tenant: String,
    namespace: String,
    model: String,
    engine_family: nexus_state::EngineFamily,
    semantic_type: nexus_state::StateSemanticType,
    block_id: Option<u32>,
    page_id: Option<u32>,
}

impl From<&KeyIdentity> for KeyScope {
    fn from(value: &KeyIdentity) -> Self {
        Self {
            tenant: value.tenant.clone(),
            namespace: value.namespace.clone(),
            model: value.model.clone(),
            engine_family: value.engine_family,
            semantic_type: value.semantic_type,
            block_id: value.block_id,
            page_id: value.page_id,
        }
    }
}

pub fn reuse_key(identity: KeyIdentity) -> ReuseKey {
    ReuseKey { identity }
}

pub fn query_key(identity: KeyIdentity) -> QueryKey {
    QueryKey { identity }
}

pub fn partial_hit_plan_from_match(hit: &nexus_state::MatchResult) -> PartialHitPlan {
    let reusable_tokens = hit.requested_key.identity.tokens[..hit.matched_extent.units as usize].to_vec();
    let disposition = if hit.remaining.tokens.is_empty() {
        PlanDisposition::FullReuse
    } else {
        PlanDisposition::PartialReuse
    };
    PartialHitPlan {
        disposition,
        reusable: ReusableSlice {
            tokens: reusable_tokens,
            source_tier: hit.entry.location.tier,
        },
        remaining: hit.remaining.clone(),
        entry: hit.entry.clone(),
    }
}

#[derive(Debug)]
struct Node {
    children: HashMap<u32, Node>,
    terminal_entry: Option<CacheEntry>,
}

impl Default for Node {
    fn default() -> Self {
        Self {
            children: HashMap::new(),
            terminal_entry: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct RadixTree {
    scopes: HashMap<KeyScope, Node>,
}

impl RadixTree {
    pub fn insert(&mut self, key: ReuseKey, entry: CacheEntry) {
        let scope = KeyScope::from(&key.identity);
        let mut node = self.scopes.entry(scope).or_default();
        for token in &key.identity.tokens {
            node = node.children.entry(*token).or_default();
        }
        node.terminal_entry = Some(entry);
    }

    pub fn lookup(&self, query: &QueryKey) -> Option<nexus_state::MatchResult> {
        let scope = KeyScope::from(&query.identity);
        let mut node = self.scopes.get(&scope)?;
        let mut best_entry: Option<(usize, CacheEntry)> = None;

        for (index, token) in query.identity.tokens.iter().enumerate() {
            let Some(child) = node.children.get(token) else {
                break;
            };
            node = child;
            if let Some(entry) = node.terminal_entry.as_ref() {
                best_entry = Some((index + 1, entry.clone()));
            }
        }

        let (matched_units, entry) = best_entry?;
        let classification = classify_match(query.identity.tokens.len(), matched_units);

        Some(nexus_state::MatchResult {
            classification,
            matched_key: reuse_key(entry.identity.key.clone()),
            requested_key: query.clone(),
            matched_extent: MatchExtent {
                units: matched_units as u32,
                granularity: granularity_for(&entry.identity.key),
            },
            entry,
            remaining: RemainingWork {
                tokens: query.identity.tokens[matched_units..].to_vec(),
                fetch_required: classification != MatchClassification::Exact,
                recompute_required: classification != MatchClassification::Exact,
            },
            compatibility: CompatibilitySignal {
                reusable: true,
                fallback_to_recompute: false,
                reason: String::new(),
            },
        })
    }

    pub fn plan_partial_hit(&self, query: &QueryKey) -> Option<PartialHitPlan> {
        self.lookup(query).map(|hit| partial_hit_plan_from_match(&hit))
    }
}

fn classify_match(requested_len: usize, matched_len: usize) -> MatchClassification {
    if matched_len == requested_len {
        MatchClassification::Exact
    } else if matched_len > 0 {
        MatchClassification::Partial
    } else {
        MatchClassification::Prefix
    }
}

fn granularity_for(identity: &KeyIdentity) -> Granularity {
    if identity.page_id.is_some() {
        Granularity::Page
    } else if identity.block_id.is_some() {
        Granularity::Block
    } else {
        Granularity::Token
    }
}

pub fn source_tier(plan: &PartialHitPlan) -> TierKind {
    plan.reusable.source_tier
}
