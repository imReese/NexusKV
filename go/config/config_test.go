package config

import "testing"

func TestDefaultConfigEnablesAdminAPI(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.Admin.Enabled {
		t.Fatalf("expected admin API enabled by default")
	}

	if cfg.Admin.ListenAddress == "" {
		t.Fatalf("expected listen address to be populated")
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected default config to validate: %v", err)
	}
}
