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
- backend capability overlays

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
- file export for Python process handoff
- future API-server and operator distribution surfaces

Python execution owns:

- policy consumption
- file-based initial load and reload
- last-known-good policy retention on invalid reload
- backend catalog filtering
- backend priority application
- backend overlay application
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

Backend overlays can narrow or override behavior for a single transfer backend:

- enable or disable that backend
- override backend priority
- restrict source tiers
- restrict target tiers
- restrict device classes
- restrict buffer kinds
- restrict materialization capabilities
- disable degraded selection for that backend

Overlay restrictions are additive. A backend must satisfy both the global policy and its backend-specific overlay.

## Distribution and reload

The current distribution model is file-based:

- Go renders a validated policy file through `ExportExecutionPolicy`
- Python receives an explicit path or reads `NEXUSKV_EXECUTION_POLICY_PATH`
- Python validates before activation
- invalid initial load fails clearly
- invalid reload preserves the last-known-good policy and records the reload error
- successful reload updates future execution decisions without connector changes

This is intentionally smaller than an API server. It gives us a stable contract for process startup, tests, and local deployment while leaving room for later Kubernetes `ConfigMap`, operator, or control-plane API distribution.

Current placeholders are explicit but non-operative:

- tenant and namespace policy
- quota and admission policy

Those fields are validated and carried in config, but they do not yet drive admission, quota, or fairness behavior.

## Deferred work

Deferred to later PRs:

- RPC or watch-based policy distribution from a Go service into Python processes
- Kubernetes `ConfigMap` and operator rollout semantics
- tenant-specific overrides beyond placeholders
- quota enforcement
- staged-buffer reuse and admission policy

This policy layer is the control point future admission, quota, and staged-buffer behavior should plug into.
