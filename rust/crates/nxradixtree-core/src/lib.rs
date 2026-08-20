//! nxradixtree core primitives for high-performance concurrent prefix matching.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use dashmap::DashMap;

pub use nexus_state::{
    CacheEntry, CompatibilitySignal, EntryIdentity, EntryLocation, EntryVersion, KeyIdentity,
    MatchClassification, MatchExtent, PartialHitPlan, PlanDisposition, PolicyHint, QueryKey,
    RemainingWork, ReusableSlice, ReuseKey,
};
use nexus_state::{Granularity, TierKind};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyScope {
    pub tenant: String,
    pub namespace: String,
    pub model: String,
    pub engine_family: nexus_state::EngineFamily,
    pub semantic_type: nexus_state::StateSemanticType,
    pub block_id: Option<u32>,
    pub page_id: Option<u32>,
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
    let reusable_tokens =
        hit.requested_key.identity.tokens[..hit.matched_extent.units as usize].to_vec();
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

#[derive(Clone, Debug, Default)]
pub struct RadixNode {
    pub children: im::HashMap<u32, Arc<RadixNode>>,
    pub terminal_entry: Option<CacheEntry>,
}

#[derive(Debug, Default)]
pub struct RadixTreeStats {
    pub total_lookups: u64,
    pub total_inserts: u64,
    pub exact_hits: u64,
    pub active_cow_branches: u64,
}

#[derive(Debug)]
pub struct RadixTree {
    scopes: DashMap<KeyScope, Arc<ArcSwap<RadixNode>>>,
    total_lookups: AtomicU64,
    total_inserts: AtomicU64,
    exact_hits: AtomicU64,
    active_cow_branches: AtomicU64,
}

impl Default for RadixTree {
    fn default() -> Self {
        Self {
            scopes: DashMap::new(),
            total_lookups: AtomicU64::new(0),
            total_inserts: AtomicU64::new(0),
            exact_hits: AtomicU64::new(0),
            active_cow_branches: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RadixTreeBranch {
    branch_id: String,
    root_scope: KeyScope,
    node: Arc<RadixNode>,
}

impl RadixTree {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, key: ReuseKey, entry: CacheEntry) {
        self.total_inserts.fetch_add(1, Ordering::Relaxed);
        let scope = KeyScope::from(&key.identity);

        let root_swap = self
            .scopes
            .entry(scope)
            .or_insert_with(|| Arc::new(ArcSwap::from_pointee(RadixNode::default())))
            .clone();

        let tokens = &key.identity.tokens;

        loop {
            let current_root = root_swap.load();

            // 1. Traverse and collect path to avoid recursion
            let mut path: Vec<Arc<RadixNode>> = Vec::with_capacity(tokens.len() + 1);
            path.push(current_root.clone());
            let mut curr = current_root.clone();

            for token in tokens {
                let child = curr
                    .children
                    .get(token)
                    .cloned()
                    .unwrap_or_else(|| Arc::new(RadixNode::default()));
                path.push(child.clone());
                curr = child;
            }

            // 2. Rebuild path bottom-up
            let mut new_node = RadixNode {
                children: path.last().unwrap().children.clone(),
                terminal_entry: Some(entry.clone()),
            };

            for i in (0..tokens.len()).rev() {
                let token = tokens[i];
                let parent = &path[i];
                let mut new_children = parent.children.clone();
                new_children.insert(token, Arc::new(new_node));
                new_node = RadixNode {
                    children: new_children,
                    terminal_entry: parent.terminal_entry.clone(),
                };
            }

            // 3. CAS root
            let new_root = Arc::new(new_node);
            let prev = root_swap.compare_and_swap(&current_root, new_root);
            if Arc::ptr_eq(&prev, &current_root) {
                break;
            }
        }
    }

    pub fn fork_branch(&self, key: &ReuseKey, branch_id: &str) -> Option<RadixTreeBranch> {
        let scope = KeyScope::from(&key.identity);
        let root_swap = self.scopes.get(&scope)?;
        let root = root_swap.load().clone();

        self.active_cow_branches.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(branch_id = %branch_id, "Forked CoW RadixTree branch");

        Some(RadixTreeBranch {
            branch_id: branch_id.to_string(),
            root_scope: scope,
            node: root,
        })
    }

    pub fn lookup(&self, query: &QueryKey) -> Option<nexus_state::MatchResult> {
        self.total_lookups.fetch_add(1, Ordering::Relaxed);
        let scope = KeyScope::from(&query.identity);
        let root_swap = self.scopes.get(&scope)?;
        let mut curr = root_swap.load().clone();

        let mut best_entry: Option<(usize, CacheEntry)> = None;

        for (index, token) in query.identity.tokens.iter().enumerate() {
            let child = match curr.children.get(token) {
                Some(c) => c.clone(),
                None => break,
            };
            curr = child;
            if let Some(entry) = curr.terminal_entry.as_ref() {
                best_entry = Some((index + 1, entry.clone()));
            }
        }

        let (matched_units, entry) = best_entry?;
        let classification = classify_match(query.identity.tokens.len(), matched_units);

        if classification == MatchClassification::Exact {
            self.exact_hits.fetch_add(1, Ordering::Relaxed);
        }

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
        self.lookup(query)
            .map(|hit| partial_hit_plan_from_match(&hit))
    }

    pub fn stats(&self) -> RadixTreeStats {
        RadixTreeStats {
            total_lookups: self.total_lookups.load(Ordering::Relaxed),
            total_inserts: self.total_inserts.load(Ordering::Relaxed),
            exact_hits: self.exact_hits.load(Ordering::Relaxed),
            active_cow_branches: self.active_cow_branches.load(Ordering::Relaxed),
        }
    }
}

impl RadixTreeBranch {
    pub fn branch_id(&self) -> &str {
        &self.branch_id
    }

    pub fn root_scope(&self) -> &KeyScope {
        &self.root_scope
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
