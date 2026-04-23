package app

import "testing"

func TestNewServerExposesDefaultAdminRoutes(t *testing.T) {
	server := NewServer(ServerConfig{ListenAddress: "127.0.0.1:0"})

	routes := server.Routes()
	if len(routes) == 0 {
		t.Fatalf("expected default routes")
	}

	if routes[0] != "/livez" {
		t.Fatalf("unexpected first route: %s", routes[0])
	}
}
