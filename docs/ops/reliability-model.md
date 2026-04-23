# Reliability Model

## Consistency direction

NexusKV should treat reuse metadata as soft state and cached attention state as opportunistically reusable data. A miss must always degrade safely to recompute.

## Safety rules

- cache miss is never fatal if recompute is available
- retries must be idempotent for metadata fetch paths
- stale metadata must fail closed to miss, not return incompatible state
- partial materialization must validate descriptor compatibility before exposure

## Durability direction

- device-local and host-local tiers are performance tiers, not durable system-of-record tiers
- local SSD and remote shared tiers can provide restart value, but durability guarantees must be explicit per deployment mode
- long-term object storage is deferred

## Failure modes to design for

- remote tier timeout
- partial fetch timeout
- stale metadata during rolling upgrade
- descriptor mismatch between engine adapter and stored state
- corrupted segment or page payload

## Upgrade boundary

Versioned descriptors and protocols are required before cross-version reuse is enabled by default.
