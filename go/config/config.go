package config

type AdminConfig struct {
	Enabled       bool
	ListenAddress string
}

type ControlPlaneConfig struct {
	ClusterName string
	Admin       AdminConfig
}

func DefaultConfig() ControlPlaneConfig {
	return ControlPlaneConfig{
		ClusterName: "nexuskv-dev",
		Admin: AdminConfig{
			Enabled:       true,
			ListenAddress: "127.0.0.1:8081",
		},
	}
}
