# Control-Plane Execution Policy

## Goal

Define a small versioned policy contract that lets the Go control plane steer Python execution behavior without leaking policy logic into connector lifecycle code.

## Why this is a separate schema

NexusKV now has two distinct contract families:

- `nexuskv.contract.v1` for shared runtime data such as descriptors, planner identities, and planner results
- `nexuskv.execution_policy.v1` for operator-authored control-plane configuration

This PR uses a separate versioned JSON schema at [schema/nexuskv_execution_policy_v1.json](../../schema/nexuskv_execution_policy_v1.json) instead of extending the hot-path IDL.

Why:

- execution policy is control-plane configuration, not a high-frequency planner/runtime payload
- Go and Python both need to load the policy directly as config, not as generated transport objects
- JSON schema is easier for operators and tooling to validate, diff, and render
- the boundary still stays explicit and versioned, which prevents control-plane and Python execution drift

## Schema summary

`nexuskv.execution_policy.v1` currently defines:

- enabled transfer backends
- backend priority order
- allowed source tiers
- allowed target tiers
- allowed device classes
- allowed buffer kinds
- default fallback behavior
- recompute fallback policy
- tenant and namespace placeholder policy
- quota and admission placeholder policy

Current fallback behavior values:

- `degrade`
- `recompute`
- `skip`

Current placeholder modes:

- `disabled`
- `advisory`

## Ownership boundary

Go control plane owns:

- config types
- config loading
- config rendering
- config validation
- future policy distribution surfaces

Python execution owns:

- policy consumption
- backend catalog filtering
- backend priority application
- execution fallback interpretation
- target/source tier and device/buffer enforcement

Connectors do not own policy evaluation.

## Current behavior

The policy currently affects:

- which execution backends are registered
- how transfer backends are prioritized
- whether degraded backend selection is allowed
- whether source tiers are permitted
- whether target tiers are permitted
- whether target device classes are permitted
- whether target buffer kinds are permitted
- whether fallback becomes recompute or skip

Current placeholders are explicit but non-operative:

- tenant and namespace policy
- quota and admission policy

Those fields are validated and carried in config, but they do not yet drive admission, quota, or fairness behavior.

## Deferred work

Deferred to later PRs:

- distribution of policy from a Go service into Python processes
- live reload or rollout semantics
- tenant-specific overrides beyond placeholders
- quota enforcement
- staged-buffer reuse and admission policy
- richer backend capability predicates

This PR is only about establishing the cross-layer contract so future policy work does not drift away from the control plane.
