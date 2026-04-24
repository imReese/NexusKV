package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultExecutionPolicyIsValid(t *testing.T) {
	policy := DefaultExecutionPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatalf("expected default execution policy to validate: %v", err)
	}
}

func TestLoadExecutionPolicyFromJSON(t *testing.T) {
	path := filepath.Join("testdata", "execution_policy_valid.json")
	policy, err := LoadExecutionPolicy(path)
	if err != nil {
		t.Fatalf("expected policy to load: %v", err)
	}

	if policy.DefaultFallbackBehavior != FallbackBehaviorDegrade {
		t.Fatalf("expected degrade fallback behavior, got %q", policy.DefaultFallbackBehavior)
	}
	if len(policy.EnabledTransferBackends) != 2 {
		t.Fatalf("expected 2 enabled backends, got %d", len(policy.EnabledTransferBackends))
	}
	if policy.BackendOverlays[TransferBackendStagedCopy].PriorityOverride == nil {
		t.Fatalf("expected staged-copy overlay priority override")
	}
}

func TestParseExecutionPolicyRejectsInvalidBackendPriority(t *testing.T) {
	data := []byte(`{
	  "schema_version": "nexuskv.execution_policy.v1",
	  "enabled_transfer_backends": ["baseline_transport"],
	  "backend_priority_order": ["staged_copy"],
	  "allowed_source_tiers": ["host_dram"],
	  "allowed_target_tiers": ["host_dram"],
	  "allowed_device_classes": ["cpu"],
	  "allowed_buffer_kinds": ["host_pageable"],
	  "default_fallback_behavior": "degrade",
	  "recompute_fallback_policy": {
	    "on_cache_miss": true,
	    "on_backend_rejection": true,
	    "on_capability_miss": true,
	    "on_policy_denial": false
	  },
	  "tenant_namespace_policy": {
	    "mode": "advisory",
	    "default_tenant": "default",
	    "default_namespace": "default"
	  },
	  "quota_admission_policy": {
	    "mode": "advisory",
	    "max_payload_bytes": 0,
	    "max_entries": 0
	  }
	}`)

	_, err := ParseExecutionPolicy(data)
	if err == nil {
		t.Fatalf("expected invalid backend priority to be rejected")
	}
}

func TestParseExecutionPolicyRejectsInvalidSchemaVersion(t *testing.T) {
	data := []byte(`{
	  "schema_version": "nexuskv.execution_policy.v0",
	  "enabled_transfer_backends": ["baseline_transport"],
	  "backend_priority_order": ["baseline_transport"],
	  "allowed_source_tiers": ["host_dram"],
	  "allowed_target_tiers": ["host_dram"],
	  "allowed_device_classes": ["cpu"],
	  "allowed_buffer_kinds": ["host_pageable"],
	  "default_fallback_behavior": "skip",
	  "recompute_fallback_policy": {
	    "on_cache_miss": true,
	    "on_backend_rejection": true,
	    "on_capability_miss": true,
	    "on_policy_denial": false
	  },
	  "tenant_namespace_policy": {
	    "mode": "disabled",
	    "default_tenant": "",
	    "default_namespace": ""
	  },
	  "quota_admission_policy": {
	    "mode": "disabled",
	    "max_payload_bytes": 0,
	    "max_entries": 0
	  }
	}`)

	_, err := ParseExecutionPolicy(data)
	if err == nil {
		t.Fatalf("expected invalid schema version to be rejected")
	}
}

func TestRenderExecutionPolicyRoundTrips(t *testing.T) {
	rendered, err := RenderExecutionPolicy(DefaultExecutionPolicy())
	if err != nil {
		t.Fatalf("expected render to succeed: %v", err)
	}

	parsed, err := ParseExecutionPolicy(rendered)
	if err != nil {
		t.Fatalf("expected rendered policy to parse: %v", err)
	}

	if parsed.SchemaVersion != ExecutionPolicySchemaVersion {
		t.Fatalf("expected schema version %q, got %q", ExecutionPolicySchemaVersion, parsed.SchemaVersion)
	}
}

func TestExportExecutionPolicyWritesStableJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "execution-policy.json")
	policy := DefaultExecutionPolicy()
	priority := 3
	policy.BackendOverlays[TransferBackendStagedCopy] = BackendCapabilityOverlay{
		PriorityOverride: &priority,
		AllowedTargetTiers: []TierKind{
			TierKindDevice,
			TierKindHostDRAM,
		},
		AllowedMaterializationCapabilities: []string{
			"full",
			"partial",
		},
	}

	if err := ExportExecutionPolicy(path, policy); err != nil {
		t.Fatalf("expected export to succeed: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected exported policy to be readable: %v", err)
	}
	parsed, err := ParseExecutionPolicy(raw)
	if err != nil {
		t.Fatalf("expected exported policy to parse: %v", err)
	}
	if parsed.BackendOverlays[TransferBackendStagedCopy].PriorityOverride == nil {
		t.Fatalf("expected exported overlay priority")
	}
}

func TestExecutionPolicyRejectsInvalidOverlay(t *testing.T) {
	priority := -1
	policy := DefaultExecutionPolicy()
	policy.BackendOverlays[TransferBackendStagedCopy] = BackendCapabilityOverlay{
		PriorityOverride: &priority,
	}

	if err := policy.Validate(); err == nil {
		t.Fatalf("expected negative overlay priority to be rejected")
	}
}
