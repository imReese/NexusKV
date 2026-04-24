package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
)

const ExecutionPolicySchemaVersion = "nexuskv.execution_policy.v1"

type TransferBackend string
type TierKind string
type DeviceClass string
type BufferKind string
type FallbackBehavior string
type PlaceholderMode string

const (
	TransferBackendBaselineTransport TransferBackend = "baseline_transport"
	TransferBackendStagedCopy        TransferBackend = "staged_copy"
	TransferBackendRDMA              TransferBackend = "rdma"
	TransferBackendZeroCopy          TransferBackend = "zero_copy"
)

const (
	TierKindDevice       TierKind = "device"
	TierKindHostDRAM     TierKind = "host_dram"
	TierKindLocalSSD     TierKind = "local_ssd"
	TierKindRemoteShared TierKind = "remote_shared"
	TierKindObjectStore  TierKind = "object_store"
)

const (
	DeviceClassCPU   DeviceClass = "cpu"
	DeviceClassCUDA  DeviceClass = "cuda"
	DeviceClassROCm  DeviceClass = "rocm"
	DeviceClassTPU   DeviceClass = "tpu"
	DeviceClassOther DeviceClass = "other"
)

const (
	BufferKindDevice       BufferKind = "device"
	BufferKindHostPinned   BufferKind = "host_pinned"
	BufferKindHostPageable BufferKind = "host_pageable"
	BufferKindRemote       BufferKind = "remote"
	BufferKindFileBacked   BufferKind = "file_backed"
)

const (
	FallbackBehaviorDegrade   FallbackBehavior = "degrade"
	FallbackBehaviorRecompute FallbackBehavior = "recompute"
	FallbackBehaviorSkip      FallbackBehavior = "skip"
)

const (
	PlaceholderModeDisabled PlaceholderMode = "disabled"
	PlaceholderModeAdvisory PlaceholderMode = "advisory"
)

type RecomputeFallbackPolicy struct {
	OnCacheMiss        bool `json:"on_cache_miss"`
	OnBackendRejection bool `json:"on_backend_rejection"`
	OnCapabilityMiss   bool `json:"on_capability_miss"`
	OnPolicyDenial     bool `json:"on_policy_denial"`
}

type TenantNamespacePolicy struct {
	Mode             PlaceholderMode `json:"mode"`
	DefaultTenant    string          `json:"default_tenant"`
	DefaultNamespace string          `json:"default_namespace"`
}

type QuotaAdmissionPolicy struct {
	Mode            PlaceholderMode `json:"mode"`
	MaxPayloadBytes int64           `json:"max_payload_bytes"`
	MaxEntries      int64           `json:"max_entries"`
}

type ExecutionPolicy struct {
	SchemaVersion           string                  `json:"schema_version"`
	EnabledTransferBackends []TransferBackend       `json:"enabled_transfer_backends"`
	BackendPriorityOrder    []TransferBackend       `json:"backend_priority_order"`
	AllowedSourceTiers      []TierKind              `json:"allowed_source_tiers"`
	AllowedTargetTiers      []TierKind              `json:"allowed_target_tiers"`
	AllowedDeviceClasses    []DeviceClass           `json:"allowed_device_classes"`
	AllowedBufferKinds      []BufferKind            `json:"allowed_buffer_kinds"`
	DefaultFallbackBehavior FallbackBehavior        `json:"default_fallback_behavior"`
	RecomputeFallback       RecomputeFallbackPolicy `json:"recompute_fallback_policy"`
	TenantNamespacePolicy   TenantNamespacePolicy   `json:"tenant_namespace_policy"`
	QuotaAdmissionPolicy    QuotaAdmissionPolicy    `json:"quota_admission_policy"`
}

func DefaultExecutionPolicy() ExecutionPolicy {
	return ExecutionPolicy{
		SchemaVersion: ExecutionPolicySchemaVersion,
		EnabledTransferBackends: []TransferBackend{
			TransferBackendBaselineTransport,
			TransferBackendStagedCopy,
			TransferBackendRDMA,
			TransferBackendZeroCopy,
		},
		BackendPriorityOrder: []TransferBackend{
			TransferBackendStagedCopy,
			TransferBackendBaselineTransport,
			TransferBackendRDMA,
			TransferBackendZeroCopy,
		},
		AllowedSourceTiers: []TierKind{
			TierKindDevice,
			TierKindHostDRAM,
			TierKindLocalSSD,
			TierKindRemoteShared,
			TierKindObjectStore,
		},
		AllowedTargetTiers: []TierKind{
			TierKindDevice,
			TierKindHostDRAM,
			TierKindLocalSSD,
			TierKindRemoteShared,
		},
		AllowedDeviceClasses: []DeviceClass{
			DeviceClassCPU,
			DeviceClassCUDA,
			DeviceClassROCm,
			DeviceClassTPU,
			DeviceClassOther,
		},
		AllowedBufferKinds: []BufferKind{
			BufferKindDevice,
			BufferKindHostPinned,
			BufferKindHostPageable,
			BufferKindRemote,
			BufferKindFileBacked,
		},
		DefaultFallbackBehavior: FallbackBehaviorDegrade,
		RecomputeFallback: RecomputeFallbackPolicy{
			OnCacheMiss:        true,
			OnBackendRejection: true,
			OnCapabilityMiss:   true,
			OnPolicyDenial:     false,
		},
		TenantNamespacePolicy: TenantNamespacePolicy{
			Mode:             PlaceholderModeAdvisory,
			DefaultTenant:    "default",
			DefaultNamespace: "default",
		},
		QuotaAdmissionPolicy: QuotaAdmissionPolicy{
			Mode:            PlaceholderModeAdvisory,
			MaxPayloadBytes: 0,
			MaxEntries:      0,
		},
	}
}

func LoadExecutionPolicy(path string) (ExecutionPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ExecutionPolicy{}, err
	}
	return ParseExecutionPolicy(data)
}

func ParseExecutionPolicy(data []byte) (ExecutionPolicy, error) {
	var policy ExecutionPolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return ExecutionPolicy{}, err
	}
	if err := policy.Validate(); err != nil {
		return ExecutionPolicy{}, err
	}
	return policy, nil
}

func RenderExecutionPolicy(policy ExecutionPolicy) ([]byte, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return json.MarshalIndent(policy, "", "  ")
}

func (p ExecutionPolicy) Validate() error {
	if p.SchemaVersion != ExecutionPolicySchemaVersion {
		return fmt.Errorf("execution policy schema_version must be %q", ExecutionPolicySchemaVersion)
	}
	if err := validateUniqueTransferBackends("enabled_transfer_backends", p.EnabledTransferBackends); err != nil {
		return err
	}
	if len(p.EnabledTransferBackends) == 0 {
		return errors.New("enabled_transfer_backends must contain at least one backend")
	}
	if err := validateUniqueTransferBackends("backend_priority_order", p.BackendPriorityOrder); err != nil {
		return err
	}
	if len(p.BackendPriorityOrder) == 0 {
		return errors.New("backend_priority_order must contain at least one backend")
	}
	if err := validateBackendPriorityCoverage(p.EnabledTransferBackends, p.BackendPriorityOrder); err != nil {
		return err
	}
	if err := validateUniqueTiers("allowed_source_tiers", p.AllowedSourceTiers); err != nil {
		return err
	}
	if err := validateUniqueTiers("allowed_target_tiers", p.AllowedTargetTiers); err != nil {
		return err
	}
	if err := validateUniqueDeviceClasses("allowed_device_classes", p.AllowedDeviceClasses); err != nil {
		return err
	}
	if err := validateUniqueBufferKinds("allowed_buffer_kinds", p.AllowedBufferKinds); err != nil {
		return err
	}
	if !isValidFallbackBehavior(p.DefaultFallbackBehavior) {
		return fmt.Errorf("unsupported default_fallback_behavior %q", p.DefaultFallbackBehavior)
	}
	if err := validatePlaceholderMode("tenant_namespace_policy.mode", p.TenantNamespacePolicy.Mode); err != nil {
		return err
	}
	if err := validatePlaceholderMode("quota_admission_policy.mode", p.QuotaAdmissionPolicy.Mode); err != nil {
		return err
	}
	if p.QuotaAdmissionPolicy.MaxPayloadBytes < 0 {
		return errors.New("quota_admission_policy.max_payload_bytes must be >= 0")
	}
	if p.QuotaAdmissionPolicy.MaxEntries < 0 {
		return errors.New("quota_admission_policy.max_entries must be >= 0")
	}
	return nil
}

func validateUniqueTransferBackends(name string, values []TransferBackend) error {
	seen := map[TransferBackend]struct{}{}
	for _, value := range values {
		if !isValidTransferBackend(value) {
			return fmt.Errorf("%s contains unsupported backend %q", name, value)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s contains duplicate backend %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateUniqueTiers(name string, values []TierKind) error {
	seen := map[TierKind]struct{}{}
	for _, value := range values {
		if !isValidTier(value) {
			return fmt.Errorf("%s contains unsupported tier %q", name, value)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s contains duplicate tier %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateUniqueDeviceClasses(name string, values []DeviceClass) error {
	seen := map[DeviceClass]struct{}{}
	for _, value := range values {
		if !isValidDeviceClass(value) {
			return fmt.Errorf("%s contains unsupported device class %q", name, value)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s contains duplicate device class %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateUniqueBufferKinds(name string, values []BufferKind) error {
	seen := map[BufferKind]struct{}{}
	for _, value := range values {
		if !isValidBufferKind(value) {
			return fmt.Errorf("%s contains unsupported buffer kind %q", name, value)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s contains duplicate buffer kind %q", name, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateBackendPriorityCoverage(enabled []TransferBackend, priority []TransferBackend) error {
	for _, backend := range enabled {
		if !slices.Contains(priority, backend) {
			return fmt.Errorf("backend_priority_order must include enabled backend %q", backend)
		}
	}
	for _, backend := range priority {
		if !slices.Contains(enabled, backend) {
			return fmt.Errorf("backend_priority_order contains backend %q that is not enabled", backend)
		}
	}
	return nil
}

func validatePlaceholderMode(name string, value PlaceholderMode) error {
	if value == PlaceholderModeDisabled || value == PlaceholderModeAdvisory {
		return nil
	}
	return fmt.Errorf("%s must be one of %q or %q", name, PlaceholderModeDisabled, PlaceholderModeAdvisory)
}

func isValidFallbackBehavior(value FallbackBehavior) bool {
	switch value {
	case FallbackBehaviorDegrade, FallbackBehaviorRecompute, FallbackBehaviorSkip:
		return true
	default:
		return false
	}
}

func isValidTransferBackend(value TransferBackend) bool {
	switch value {
	case TransferBackendBaselineTransport, TransferBackendStagedCopy, TransferBackendRDMA, TransferBackendZeroCopy:
		return true
	default:
		return false
	}
}

func isValidTier(value TierKind) bool {
	switch value {
	case TierKindDevice, TierKindHostDRAM, TierKindLocalSSD, TierKindRemoteShared, TierKindObjectStore:
		return true
	default:
		return false
	}
}

func isValidDeviceClass(value DeviceClass) bool {
	switch value {
	case DeviceClassCPU, DeviceClassCUDA, DeviceClassROCm, DeviceClassTPU, DeviceClassOther:
		return true
	default:
		return false
	}
}

func isValidBufferKind(value BufferKind) bool {
	switch value {
	case BufferKindDevice, BufferKindHostPinned, BufferKindHostPageable, BufferKindRemote, BufferKindFileBacked:
		return true
	default:
		return false
	}
}
